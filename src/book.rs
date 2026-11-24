//! Order book management for Polymarket client

use crate::errors::{PolyfillError, Result};
use crate::types::*;
use crate::utils::math;
use chrono::Utc;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc; // For shared access across multiple tasks
use tracing::{debug, trace, warn}; // Logging for debugging and monitoring

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StoredLevel {
    qty: Qty,
    generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ParsedBookLevel {
    pub side: Side,
    pub price_ticks: Price,
    pub size_units: Qty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BookSideKind {
    Bid,
    Ask,
}

#[derive(Debug, Clone)]
struct BookSide {
    levels: Vec<(Price, StoredLevel)>,
    kind: BookSideKind,
}

impl BookSide {
    fn new(kind: BookSideKind, max_depth: usize) -> Self {
        Self {
            levels: Vec::with_capacity(max_depth.saturating_add(8)),
            kind,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.levels.len()
    }

    #[inline]
    fn search(&self, price: Price) -> std::result::Result<usize, usize> {
        match self.kind {
            // Bids are stored highest price first.
            BookSideKind::Bid => self
                .levels
                .binary_search_by(|(level_price, _)| price.cmp(level_price)),
            // Asks are stored lowest price first.
            BookSideKind::Ask => self
                .levels
                .binary_search_by(|(level_price, _)| level_price.cmp(&price)),
        }
    }

    #[inline]
    fn best(&self) -> Option<(Price, &StoredLevel)> {
        self.levels.first().map(|(price, level)| (*price, level))
    }

    #[inline]
    fn get(&self, price: Price) -> Option<&StoredLevel> {
        self.search(price)
            .ok()
            .and_then(|idx| self.levels.get(idx).map(|(_, level)| level))
    }

    #[inline]
    fn upsert(&mut self, price: Price, level: StoredLevel) {
        match self.search(price) {
            Ok(idx) if level.qty == 0 => {
                self.levels.remove(idx);
            },
            Ok(idx) => {
                self.levels[idx].1 = level;
            },
            Err(idx) if level.qty != 0 => {
                self.levels.insert(idx, (price, level));
            },
            Err(_) => {},
        }
    }

    #[inline]
    fn retain_generation(&mut self, generation: u64) {
        self.levels
            .retain(|(_, level)| level.generation == generation);
    }

    #[inline]
    fn iter_top(&self, depth: usize) -> impl Iterator<Item = (Price, &StoredLevel)> {
        self.levels
            .iter()
            .take(depth)
            .map(|(price, level)| (*price, level))
    }

    #[inline]
    fn iter_all(&self) -> impl Iterator<Item = (Price, &StoredLevel)> {
        self.levels.iter().map(|(price, level)| (*price, level))
    }

    fn sum_range(&self, min_price: Price, max_price: Price) -> Qty {
        let mut total = 0;
        for (price, level) in &self.levels {
            match self.kind {
                BookSideKind::Bid => {
                    if *price > max_price {
                        continue;
                    }
                    if *price < min_price {
                        break;
                    }
                },
                BookSideKind::Ask => {
                    if *price < min_price {
                        continue;
                    }
                    if *price > max_price {
                        break;
                    }
                },
            }

            total += level.qty;
        }
        total
    }
}

const DEFAULT_BOOK_SHARDS: usize = 64;

/// High-performance order book implementation
///
/// This is the core data structure that holds all the live buy/sell orders for a token.
/// The efficiency of this code is critical as the order book is constantly being updated as orders are added and removed.
///
/// PERFORMANCE OPTIMIZATION: This struct now uses fixed-point integers internally
/// instead of Decimal for maximum speed. The performance difference is dramatic:
///
/// Before (Decimal):  ~100ns per operation + memory allocation
/// After (fixed-point): ~5ns per operation, zero allocations

#[derive(Debug, Clone)]
pub struct OrderBook {
    /// Token ID this book represents (like "123456" for a specific prediction market outcome)
    pub token_id: String,

    /// Hash of token_id for fast lookups (avoids string comparisons in hot path)
    pub token_id_hash: u64,

    /// Legacy delta sequence number.
    ///
    /// Kept as a compatibility alias for [`Self::last_delta_sequence`]. WebSocket
    /// snapshot timestamps are tracked separately in [`Self::last_snapshot_timestamp_ms`].
    pub sequence: u64,

    /// Last accepted legacy/incremental delta sequence number.
    ///
    /// Delta sequences are a different clock from WebSocket snapshot timestamps and
    /// must not be compared against millisecond timestamps.
    pub last_delta_sequence: u64,

    /// Last accepted full-book snapshot timestamp in milliseconds.
    ///
    /// Used for WebSocket `book` snapshots and serde `BookUpdate` snapshots.
    pub last_snapshot_timestamp_ms: u64,

    /// Last update timestamp - when we last got new data for this book
    pub timestamp: chrono::DateTime<Utc>,

    /// Bid side (price -> size, sorted descending) - NOW USING FIXED-POINT!
    /// Stored as a sorted vector with highest bids first for cache-local top-of-book iteration.
    /// Key = price in ticks (like 6500 for $0.65), Value = size in fixed-point units
    ///
    /// BEFORE (slow): bids: BTreeMap<Decimal, Decimal>,
    /// AFTER (fast):  bids: sorted Vec<(Price, StoredLevel)>,
    ///
    /// Why this is faster:
    /// - Integer comparisons are ~10x faster than Decimal comparisons
    /// - No memory allocation for each price level
    /// - Better CPU cache utilization (smaller data structures)
    bids: BookSide,

    /// Ask side (price -> size, sorted ascending) - NOW USING FIXED-POINT!
    /// Stored as a sorted vector with lowest asks first - people selling at cheapest prices.
    ///
    /// BEFORE (slow): asks: BTreeMap<Decimal, Decimal>,
    /// AFTER (fast):  asks: sorted Vec<(Price, StoredLevel)>,
    asks: BookSide,

    /// Snapshot generation used to retain book levels without rescanning input payloads.
    snapshot_generation: u64,

    /// Last accepted full-book snapshot hash fingerprint, when the feed provides one.
    ///
    /// This is used as a tie-breaker for same-millisecond snapshots. If the timestamp is equal
    /// but the hash fingerprint differs, the snapshot can still represent a newer book state.
    last_snapshot_hash_fingerprint: Option<u64>,

    /// Minimum tick size for this market in ticks (like 10 for $0.001 increments)
    /// Some markets only allow certain price increments
    /// We store this in ticks for fast validation without conversion
    tick_size_ticks: Option<Price>,

    /// Maximum number of levels returned per side in public views.
    /// All received levels remain stored so deltas can reveal deeper prices.
    max_depth: usize,
}

impl OrderBook {
    /// Create a book that retains all supplied levels for incremental updates.
    /// `max_depth` caps returned views. Storage grows with the number of live levels.
    pub fn new(token_id: String, max_depth: usize) -> Self {
        // Hash the token_id once for fast lookups later
        let token_id_hash = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            token_id.hash(&mut hasher);
            hasher.finish()
        };

        Self {
            token_id,
            token_id_hash,
            sequence: 0, // Compatibility alias for last_delta_sequence
            last_delta_sequence: 0,
            last_snapshot_timestamp_ms: 0,
            timestamp: Utc::now(),
            bids: BookSide::new(BookSideKind::Bid, max_depth), // Empty to start
            asks: BookSide::new(BookSideKind::Ask, max_depth), // Empty to start
            snapshot_generation: 0,
            last_snapshot_hash_fingerprint: None,
            tick_size_ticks: None, // We'll set this later when we learn about the market
            max_depth,
        }
    }

    /// Discard data and ordering clocks after the owning feed loses continuity.
    pub(crate) fn invalidate(&mut self) {
        self.bids.levels.clear();
        self.asks.levels.clear();
        self.sequence = 0;
        self.last_delta_sequence = 0;
        self.last_snapshot_timestamp_ms = 0;
        self.last_snapshot_hash_fingerprint = None;
        self.snapshot_generation = 0;
        self.timestamp = chrono::DateTime::UNIX_EPOCH;
    }

    /// Set the tick size for this book
    /// This tells us the minimum price increment allowed
    /// We store it in ticks for fast validation without conversion overhead
    pub fn set_tick_size(&mut self, tick_size: Decimal) -> Result<()> {
        let tick_size_ticks = decimal_to_price_exact(tick_size)
            .map_err(|_| PolyfillError::validation("Invalid tick size"))?;
        self.tick_size_ticks = Some(tick_size_ticks);
        Ok(())
    }

    /// Set the tick size directly in ticks (even faster)
    /// Use this when you already have the tick size in our internal format
    pub fn set_tick_size_ticks(&mut self, tick_size_ticks: Price) {
        self.tick_size_ticks = Some(tick_size_ticks);
    }

    /// Get the current best bid (highest price someone is willing to pay)
    /// Bids are stored highest price first.
    ///
    /// PERFORMANCE: Now returns data in external format but internally uses fast lookups
    pub fn best_bid(&self) -> Option<BookLevel> {
        // BEFORE (slow, ~50ns + allocation):
        // self.bids.iter().next_back().map(|(&price, &size)| BookLevel { price, size })

        // AFTER (fast, ~5ns, no allocation for the lookup):
        self.bids.best().map(|(price_ticks, level)| {
            // Convert from internal fixed-point to external Decimal format
            // This conversion only happens at the API boundary
            BookLevel {
                price: price_to_decimal(price_ticks),
                size: qty_to_decimal(level.qty),
            }
        })
    }

    /// Get the current best ask (lowest price someone is willing to sell at)
    /// Asks are stored lowest price first.
    ///
    /// PERFORMANCE: Now returns data in external format but internally uses fast lookups
    pub fn best_ask(&self) -> Option<BookLevel> {
        // BEFORE (slow, ~50ns + allocation):
        // self.asks.iter().next().map(|(&price, &size)| BookLevel { price, size })

        // AFTER (fast, ~5ns, no allocation for the lookup):
        self.asks.best().map(|(price_ticks, level)| {
            // Convert from internal fixed-point to external Decimal format
            // This conversion only happens at the API boundary
            BookLevel {
                price: price_to_decimal(price_ticks),
                size: qty_to_decimal(level.qty),
            }
        })
    }

    /// Get the current best bid in fast internal format
    /// Use this for internal calculations to avoid conversion overhead
    pub fn best_bid_fast(&self) -> Option<FastBookLevel> {
        self.bids
            .best()
            .map(|(price, level)| FastBookLevel::new(price, level.qty))
    }

    /// Get the current best ask in fast internal format
    /// Use this for internal calculations to avoid conversion overhead
    pub fn best_ask_fast(&self) -> Option<FastBookLevel> {
        self.asks
            .best()
            .map(|(price, level)| FastBookLevel::new(price, level.qty))
    }

    /// Get the current spread (difference between best ask and best bid)
    /// This tells us how "tight" the market is - smaller spread = more liquid market
    ///
    /// PERFORMANCE: Now uses fast internal calculations, only converts to Decimal at the end
    pub fn spread(&self) -> Option<Decimal> {
        // BEFORE (slow, ~100ns + multiple allocations):
        // match (self.best_bid(), self.best_ask()) {
        //     (Some(bid), Some(ask)) => Some(ask.price - bid.price),
        //     _ => None,
        // }

        // AFTER (fast, ~5ns, no allocations):
        let (best_bid_ticks, best_ask_ticks) = self.best_prices_fast()?;
        let spread_ticks = math::spread_fast(best_bid_ticks, best_ask_ticks)?;
        Some(price_to_decimal(spread_ticks))
    }

    /// Get the current mid price (halfway between best bid and ask)
    /// This is often used as the "fair value" of the market
    ///
    /// PERFORMANCE: Now uses fast internal calculations, only converts to Decimal at the end
    pub fn mid_price(&self) -> Option<Decimal> {
        // BEFORE (slow, ~80ns + allocations):
        // math::mid_price(
        //     self.best_bid()?.price,
        //     self.best_ask()?.price,
        // )

        // AFTER (fast, ~3ns, no allocations):
        let (best_bid_ticks, best_ask_ticks) = self.best_prices_fast()?;
        let mid_ticks = math::mid_price_fast(best_bid_ticks, best_ask_ticks)?;
        Some(price_to_decimal(mid_ticks))
    }

    /// Get the spread as a percentage (relative to the bid price)
    /// Useful for comparing spreads across different price levels
    ///
    /// PERFORMANCE: Now uses fast internal calculations and returns basis points
    pub fn spread_pct(&self) -> Option<Decimal> {
        let (best_bid_ticks, best_ask_ticks) = self.best_prices_fast()?;
        let spread_bps = math::spread_pct_fast(best_bid_ticks, best_ask_ticks)?;
        // Convert basis points back to percentage decimal
        Some(Decimal::from(spread_bps) / Decimal::from(100))
    }

    /// Get best bid and ask prices in fast internal format
    /// Helper method to avoid code duplication and minimize conversions
    fn best_prices_fast(&self) -> Option<(Price, Price)> {
        let best_bid_ticks = self.bids.best()?.0;
        let best_ask_ticks = self.asks.best()?.0;
        Some((best_bid_ticks, best_ask_ticks))
    }

    /// Get the current spread in fast internal format (PERFORMANCE OPTIMIZED)
    /// Returns spread in ticks - use this for internal calculations
    pub fn spread_fast(&self) -> Option<Price> {
        let (best_bid_ticks, best_ask_ticks) = self.best_prices_fast()?;
        math::spread_fast(best_bid_ticks, best_ask_ticks)
    }

    /// Get the current mid price in fast internal format (PERFORMANCE OPTIMIZED)
    /// Returns mid price in ticks - use this for internal calculations
    pub fn mid_price_fast(&self) -> Option<Price> {
        let (best_bid_ticks, best_ask_ticks) = self.best_prices_fast()?;
        math::mid_price_fast(best_bid_ticks, best_ask_ticks)
    }

    /// Get bids up to the requested depth, capped at `max_depth`.
    /// Returns them in descending price order (best bids first)
    ///
    /// PERFORMANCE: Converts from internal fixed-point to external Decimal format
    /// Only call this when you need to return data to external APIs
    pub fn bids(&self, depth: Option<usize>) -> Vec<BookLevel> {
        let depth = depth.unwrap_or(self.max_depth).min(self.max_depth);
        self.bids
            .iter_top(depth)
            .map(|(price_ticks, level)| BookLevel {
                price: price_to_decimal(price_ticks),
                size: qty_to_decimal(level.qty),
            })
            .collect()
    }

    /// Get asks up to the requested depth, capped at `max_depth`.
    /// Returns them in ascending price order (best asks first)
    ///
    /// PERFORMANCE: Converts from internal fixed-point to external Decimal format
    /// Only call this when you need to return data to external APIs
    pub fn asks(&self, depth: Option<usize>) -> Vec<BookLevel> {
        let depth = depth.unwrap_or(self.max_depth).min(self.max_depth);
        self.asks
            .iter_top(depth)
            .map(|(price_ticks, level)| BookLevel {
                price: price_to_decimal(price_ticks),
                size: qty_to_decimal(level.qty),
            })
            .collect()
    }

    /// Get bids in fast internal format, capped at `max_depth`.
    /// Use this for internal calculations to avoid conversion overhead
    pub fn bids_fast(&self, depth: Option<usize>) -> Vec<FastBookLevel> {
        let depth = depth.unwrap_or(self.max_depth).min(self.max_depth);
        self.bids
            .iter_top(depth)
            .map(|(price, level)| FastBookLevel::new(price, level.qty))
            .collect()
    }

    /// Get asks in fast internal format, capped at `max_depth`.
    /// Use this for internal calculations to avoid conversion overhead
    pub fn asks_fast(&self, depth: Option<usize>) -> Vec<FastBookLevel> {
        let depth = depth.unwrap_or(self.max_depth).min(self.max_depth);
        self.asks
            .iter_top(depth)
            .map(|(price, level)| FastBookLevel::new(price, level.qty))
            .collect()
    }

    /// Copy a view of the book capped at `max_depth` per side.
    /// This view can omit levels required to seed another incremental book.
    pub fn snapshot(&self) -> crate::types::OrderBook {
        crate::types::OrderBook {
            token_id: self.token_id.clone(),
            timestamp: self.timestamp,
            bids: self.bids(None), // Get all bids (up to max_depth)
            asks: self.asks(None), // Get all asks (up to max_depth)
            sequence: self.last_delta_sequence,
            last_delta_sequence: self.last_delta_sequence,
            last_snapshot_timestamp_ms: self.last_snapshot_timestamp_ms,
        }
    }

    /// Apply a delta update to the book (LEGACY VERSION - for external API compatibility)
    /// A "delta" is an incremental change - like "add 100 tokens at $0.65" or "remove all at $0.70"
    ///
    /// This method converts the external Decimal delta to our internal fixed-point format
    /// and then calls the fast version. Use apply_delta_fast() directly when possible.
    pub fn apply_delta(&mut self, delta: OrderDelta) -> Result<()> {
        // Convert to fast internal format with tick alignment validation
        let tick_size_decimal = self.tick_size_ticks.map(price_to_decimal);
        let fast_delta = FastOrderDelta::from_order_delta(&delta, tick_size_decimal)
            .map_err(|e| PolyfillError::validation(format!("Invalid delta: {}", e)))?;

        // Use the fast internal version
        self.apply_delta_fast(fast_delta)
    }

    /// Apply a delta update to the book
    ///
    /// This is the high-performance version that works directly with fixed-point data.
    /// It includes tick alignment validation and is much faster than the Decimal version.
    ///
    /// Performance improvement: ~50x faster than the old Decimal version!
    /// - No Decimal conversions in the hot path
    /// - Integer comparisons instead of Decimal comparisons
    /// - Existing-level updates reuse storage, while new levels may grow it
    pub fn apply_delta_fast(&mut self, delta: FastOrderDelta) -> Result<()> {
        // Validate sequence ordering - ignore old updates that arrive late
        // This is crucial for maintaining data integrity in real-time systems
        if delta.sequence <= self.last_delta_sequence {
            trace!(
                "Ignoring stale delta: {} <= {}",
                delta.sequence,
                self.last_delta_sequence
            );
            return Ok(());
        }

        // Validate token ID hash matches (fast string comparison avoidance)
        if delta.token_id_hash != self.token_id_hash {
            return Err(PolyfillError::validation("Token ID mismatch"));
        }

        // TICK ALIGNMENT VALIDATION - this is where we enforce price rules
        // If we have a tick size, make sure the price aligns properly
        if let Some(tick_size_ticks) = self.tick_size_ticks {
            // BEFORE (slow, ~200ns + multiple conversions):
            // let tick_size_decimal = price_to_decimal(tick_size_ticks);
            // if !is_price_tick_aligned(price_to_decimal(delta.price), tick_size_decimal) {
            //     return Err(...);
            // }

            // AFTER (fast, ~2ns, pure integer):
            if tick_size_ticks > 0 && !delta.price.is_multiple_of(tick_size_ticks) {
                // Price is not aligned to tick size - reject the update
                warn!(
                    "Rejecting misaligned price: {} not divisible by tick size {}",
                    delta.price, tick_size_ticks
                );
                return Err(PolyfillError::validation("Price not aligned to tick size"));
            }
        }

        // Update our tracking info
        self.last_delta_sequence = delta.sequence;
        self.sequence = delta.sequence;
        self.timestamp = delta.timestamp;

        // Apply the actual change to the appropriate side (FAST VERSION)
        match delta.side {
            Side::BUY => self.apply_bid_delta_fast(delta.price, delta.size),
            Side::SELL => self.apply_ask_delta_fast(delta.price, delta.size),
        }

        debug!(
            "Applied fast delta: {} {} @ {} ticks (seq: {})",
            delta.side.as_str(),
            delta.size,
            delta.price,
            delta.sequence
        );

        Ok(())
    }

    /// Return whether a WebSocket `book` snapshot should be applied.
    pub(crate) fn should_apply_ws_book_update(
        &self,
        asset_id: &str,
        timestamp: u64,
        hash: Option<&str>,
    ) -> Result<bool> {
        if asset_id != self.token_id {
            return Err(PolyfillError::validation("Token ID mismatch"));
        }

        Ok(self.should_apply_snapshot(timestamp, hash))
    }

    /// Atomically apply a WebSocket `book` snapshot.
    ///
    /// The caller should parse levels into `ParsedBookLevel`s before calling this method. This
    /// method validates tick alignment before mutating sequence/generation or book levels, so a
    /// malformed snapshot leaves the existing book unchanged.
    pub(crate) fn apply_ws_book_snapshot_fast(
        &mut self,
        asset_id: &str,
        timestamp: u64,
        hash: Option<&str>,
        levels: &[ParsedBookLevel],
    ) -> Result<bool> {
        if !self.should_apply_ws_book_update(asset_id, timestamp, hash)? {
            return Ok(false);
        }

        self.apply_validated_snapshot(timestamp, hash, levels)?;

        Ok(true)
    }

    /// Apply a WebSocket `book` update for this token.
    ///
    /// The official Polymarket CLOB WebSocket `book` event is a full snapshot.
    /// Unlike `apply_delta_fast`, this method can apply many levels that share
    /// the same message timestamp.
    ///
    /// Notes:
    /// - This replaces the snapshot for both sides.
    /// - Levels omitted from the message are removed.
    /// - Insertions of *new* price levels may allocate or shift vector entries.
    pub fn apply_book_update(&mut self, update: &BookUpdate) -> Result<()> {
        if update.asset_id != self.token_id {
            return Err(PolyfillError::validation("Token ID mismatch"));
        }

        // Use the exchange-provided timestamp as the primary snapshot marker. The WS `book`
        // message does not expose a monotonic server sequence/version, so same-millisecond
        // snapshots with different hashes are ordered by websocket arrival order. The hash
        // distinguishes duplicate vs distinct state; it is not a logical ordering key.
        // Snapshot timestamps are intentionally separate from legacy delta sequence numbers.
        if !self.should_apply_snapshot(update.timestamp, update.hash.as_deref()) {
            return Ok(());
        }

        // Validate the whole snapshot before mutating this book. A malformed level must not leave
        // behind a partial generation or an advanced sequence number.
        for level in &update.bids {
            let parsed = self.parse_snapshot_summary(Side::BUY, level)?;
            self.validate_snapshot_level(parsed)?;
        }

        for level in &update.asks {
            let parsed = self.parse_snapshot_summary(Side::SELL, level)?;
            self.validate_snapshot_level(parsed)?;
        }

        self.last_snapshot_timestamp_ms = update.timestamp;
        self.last_snapshot_hash_fingerprint = update.hash.as_deref().map(snapshot_hash_fingerprint);
        self.timestamp = chrono::DateTime::<Utc>::from_timestamp_millis(update.timestamp as i64)
            .unwrap_or_else(Utc::now);
        self.begin_snapshot();

        // Re-parse after validation to preserve the existing no-allocation behavior for
        // `BookUpdate` snapshots. Decimal conversion is deterministic, so these conversions cannot
        // fail after the validation pass above.
        for level in &update.bids {
            let parsed = self
                .parse_snapshot_summary(Side::BUY, level)
                .expect("book update bid level was validated before mutation");
            self.apply_snapshot_level(parsed.side, parsed.price_ticks, parsed.size_units);
        }

        for level in &update.asks {
            let parsed = self
                .parse_snapshot_summary(Side::SELL, level)
                .expect("book update ask level was validated before mutation");
            self.apply_snapshot_level(parsed.side, parsed.price_ticks, parsed.size_units);
        }

        self.finish_snapshot();
        Ok(())
    }

    /// Apply a bid-side delta (someone wants to buy) - LEGACY VERSION
    /// If size is 0, it means "remove this price level entirely"
    /// Otherwise, set the total size at this price level
    ///
    /// This converts to fixed-point and calls the fast version
    #[allow(dead_code)]
    fn apply_bid_delta(&mut self, price: Decimal, size: Decimal) {
        // Convert to fixed-point (this should be rare since we use fast path)
        let price_ticks = decimal_to_price_lossy(price).unwrap_or(0);
        let size_units = decimal_to_qty(size).unwrap_or(0);
        self.apply_bid_delta_fast(price_ticks, size_units);
    }

    /// Apply an ask-side delta (someone wants to sell) - LEGACY VERSION
    /// Same logic as bids - size of 0 means remove the price level
    ///
    /// This converts to fixed-point and calls the fast version
    #[allow(dead_code)]
    fn apply_ask_delta(&mut self, price: Decimal, size: Decimal) {
        // Convert to fixed-point (this should be rare since we use fast path)
        let price_ticks = decimal_to_price_lossy(price).unwrap_or(0);
        let size_units = decimal_to_qty(size).unwrap_or(0);
        self.apply_ask_delta_fast(price_ticks, size_units);
    }

    /// Apply a bid-side delta (someone wants to buy) - FAST VERSION
    ///
    /// This is the high-performance version that works directly with fixed-point.
    /// Much faster than the Decimal version - pure integer operations.
    fn apply_bid_delta_fast(&mut self, price_ticks: Price, size_units: Qty) {
        // BEFORE (slow, ~100ns + allocation):
        // if size.is_zero() {
        //     self.bids.remove(&price);
        // } else {
        //     self.bids.insert(price, size);
        // }

        // AFTER (fast, ~5ns, no allocation):
        self.bids.upsert(
            price_ticks,
            StoredLevel {
                qty: size_units,
                generation: self.snapshot_generation,
            },
        );
    }

    /// Apply an ask-side delta (someone wants to sell) - FAST VERSION
    ///
    /// This is the high-performance version that works directly with fixed-point.
    /// Much faster than the Decimal version - pure integer operations.
    fn apply_ask_delta_fast(&mut self, price_ticks: Price, size_units: Qty) {
        // BEFORE (slow, ~100ns + allocation):
        // if size.is_zero() {
        //     self.asks.remove(&price);
        // } else {
        //     self.asks.insert(price, size);
        // }

        // AFTER (fast, ~5ns, no allocation):
        self.asks.upsert(
            price_ticks,
            StoredLevel {
                qty: size_units,
                generation: self.snapshot_generation,
            },
        );
    }

    #[inline]
    fn begin_snapshot(&mut self) {
        self.snapshot_generation = self.snapshot_generation.wrapping_add(1);
    }

    #[inline]
    fn should_apply_snapshot(&self, timestamp: u64, hash: Option<&str>) -> bool {
        // Without a server sequence/version, equal-timestamp distinct hashes can only be ordered
        // by arrival. Older timestamps are always stale; exact same timestamp/hash is duplicate.
        if timestamp > self.last_snapshot_timestamp_ms {
            return true;
        }
        if timestamp < self.last_snapshot_timestamp_ms {
            return false;
        }

        hash.is_some_and(|hash| {
            self.last_snapshot_hash_fingerprint != Some(snapshot_hash_fingerprint(hash))
        })
    }

    #[inline]
    fn parse_snapshot_summary(&self, side: Side, level: &OrderSummary) -> Result<ParsedBookLevel> {
        let price_ticks = decimal_to_price_exact(level.price)
            .map_err(|_| PolyfillError::validation("Invalid price"))?;
        let size_units =
            decimal_to_qty(level.size).map_err(|_| PolyfillError::validation("Invalid size"))?;

        Ok(ParsedBookLevel {
            side,
            price_ticks,
            size_units,
        })
    }

    #[inline]
    fn validate_snapshot_level(&self, level: ParsedBookLevel) -> Result<()> {
        if let Some(tick_size_ticks) = self.tick_size_ticks {
            if tick_size_ticks > 0 && !level.price_ticks.is_multiple_of(tick_size_ticks) {
                return Err(PolyfillError::validation("Price not aligned to tick size"));
            }
        }

        Ok(())
    }

    fn apply_validated_snapshot(
        &mut self,
        timestamp: u64,
        hash: Option<&str>,
        levels: &[ParsedBookLevel],
    ) -> Result<()> {
        for &level in levels {
            self.validate_snapshot_level(level)?;
        }

        self.last_snapshot_timestamp_ms = timestamp;
        self.last_snapshot_hash_fingerprint = hash.map(snapshot_hash_fingerprint);
        self.timestamp = chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
            .unwrap_or_else(Utc::now);
        self.begin_snapshot();

        for &level in levels {
            self.apply_snapshot_level(level.side, level.price_ticks, level.size_units);
        }

        self.finish_snapshot();

        Ok(())
    }

    #[inline]
    fn apply_snapshot_level(&mut self, side: Side, price_ticks: Price, size_units: Qty) {
        let generation = self.snapshot_generation;
        let book_side = match side {
            Side::BUY => &mut self.bids,
            Side::SELL => &mut self.asks,
        };

        book_side.upsert(
            price_ticks,
            StoredLevel {
                qty: size_units,
                generation,
            },
        );
    }

    #[inline]
    fn finish_snapshot(&mut self) {
        let generation = self.snapshot_generation;
        self.bids.retain_generation(generation);
        self.asks.retain_generation(generation);
    }

    /// Calculate market impact using all retained levels.
    pub fn calculate_market_impact(&self, side: Side, size: Decimal) -> Option<MarketImpact> {
        let size_units = decimal_to_qty(size).ok()?;
        let (filled_units, total_notional, best_price_ticks) = match side {
            Side::BUY => fill_market_impact(self.asks.iter_all(), size_units)?,
            Side::SELL => fill_market_impact(self.bids.iter_all(), size_units)?,
        };

        let total_cost = Decimal::from_i128_with_scale(total_notional, 8);
        let filled_size = qty_to_decimal(filled_units);
        let avg_price = total_cost / filled_size;
        let best_price = price_to_decimal(best_price_ticks);
        let impact = if side == Side::BUY {
            (avg_price - best_price) / best_price
        } else {
            (best_price - avg_price) / best_price
        };

        Some(MarketImpact {
            average_price: avg_price,
            impact_pct: impact,
            total_cost,
            size_filled: filled_size,
        })
    }

    /// Check if the book is stale (no recent updates)
    /// Useful for detecting when we've lost connection to live data
    pub fn is_stale(&self, max_age: std::time::Duration) -> bool {
        let age = Utc::now() - self.timestamp;
        age > chrono::Duration::from_std(max_age).unwrap_or_default()
    }

    /// Get the total liquidity at a given price level
    /// Tells you how much you can buy/sell at exactly this price
    pub fn liquidity_at_price(&self, price: Decimal, side: Side) -> Decimal {
        // Convert decimal price to our internal fixed-point representation
        let price_ticks = match decimal_to_price_exact(price) {
            Ok(ticks) => ticks,
            Err(_) => return Decimal::ZERO, // Invalid price
        };

        match side {
            Side::BUY => {
                // How much we can buy at this price (look at asks)
                let size_units = self
                    .asks
                    .get(price_ticks)
                    .map(|level| level.qty)
                    .unwrap_or_default();
                qty_to_decimal(size_units)
            },
            Side::SELL => {
                // How much we can sell at this price (look at bids)
                let size_units = self
                    .bids
                    .get(price_ticks)
                    .map(|level| level.qty)
                    .unwrap_or_default();
                qty_to_decimal(size_units)
            },
        }
    }

    /// Get the total liquidity within a price range
    /// Useful for understanding how much depth exists in a certain price band
    pub fn liquidity_in_range(
        &self,
        min_price: Decimal,
        max_price: Decimal,
        side: Side,
    ) -> Decimal {
        // Convert decimal prices to our internal fixed-point representation
        let min_price_ticks = match decimal_to_price_exact(min_price) {
            Ok(ticks) => ticks,
            Err(_) => return Decimal::ZERO, // Invalid price
        };
        let max_price_ticks = match decimal_to_price_exact(max_price) {
            Ok(ticks) => ticks,
            Err(_) => return Decimal::ZERO, // Invalid price
        };

        let total_size_units: Qty = match side {
            Side::BUY => self.asks.sum_range(min_price_ticks, max_price_ticks),
            Side::SELL => self.bids.sum_range(min_price_ticks, max_price_ticks),
        };

        qty_to_decimal(total_size_units)
    }

    /// Validate that prices are properly ordered
    /// A healthy book should have best bid < best ask (otherwise there's an arbitrage opportunity)
    pub fn is_valid(&self) -> bool {
        self.best_prices_fast()
            .map(|(best_bid_ticks, best_ask_ticks)| best_bid_ticks < best_ask_ticks)
            .unwrap_or(true)
    }
}

fn fill_market_impact<'a>(
    levels: impl Iterator<Item = (Price, &'a StoredLevel)>,
    size_units: Qty,
) -> Option<(Qty, i128, Price)> {
    if size_units <= 0 {
        return None;
    }

    let mut remaining_units = size_units;
    let mut filled_units = 0;
    let mut total_notional = 0i128;
    let mut best_price_ticks = None;

    for (price_ticks, level) in levels {
        if best_price_ticks.is_none() {
            best_price_ticks = Some(price_ticks);
        }

        if level.qty <= 0 {
            continue;
        }

        let fill_units = remaining_units.min(level.qty);
        let fill_notional = (price_ticks as i128).checked_mul(fill_units as i128)?;
        total_notional = total_notional.checked_add(fill_notional)?;
        filled_units += fill_units;
        remaining_units -= fill_units;

        if remaining_units == 0 {
            break;
        }
    }

    if remaining_units > 0 {
        return None;
    }

    Some((filled_units, total_notional, best_price_ticks?))
}

/// Market impact calculation result
/// This tells you what would happen if you executed a large order
#[derive(Debug, Clone)]
pub struct MarketImpact {
    pub average_price: Decimal, // The average price you'd get across all fills
    pub impact_pct: Decimal,    // How much worse than the best price (as percentage)
    pub total_cost: Decimal,    // Total amount you'd pay/receive
    pub size_filled: Decimal,   // How much of your order got filled
}

/// Thread-safe order book manager
/// This manages multiple order books (one per token) and handles concurrent access
/// Multiple threads can read/write different books simultaneously
///
/// Each book retains all received levels. `max_depth` bounds returned views,
/// while storage scales with the total number of live levels across tokens.
#[derive(Debug)]
pub struct OrderBookManager {
    shards: Arc<[BookShard]>, // Token ID -> shard-local OrderBook
    max_depth: usize,
}

#[derive(Debug, Default)]
struct BookShard {
    books: RwLock<HashMap<String, OrderBook>>,
}

#[inline]
fn snapshot_hash_fingerprint(hash: &str) -> u64 {
    let mut fingerprint = 0xcbf2_9ce4_8422_2325u64;
    for &byte in hash.as_bytes() {
        fingerprint ^= byte as u64;
        fingerprint = fingerprint.wrapping_mul(0x0000_0100_0000_01b3);
    }
    fingerprint
}

#[inline]
fn shard_index(token_id: &str, shard_count: usize) -> usize {
    debug_assert!(shard_count > 0);
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for &byte in token_id.as_bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash as usize) % shard_count
}

impl OrderBookManager {
    /// Create a new order book manager
    /// Starts with an empty collection of books
    pub fn new(max_depth: usize) -> Self {
        Self::with_shard_count(max_depth, DEFAULT_BOOK_SHARDS)
    }

    /// Create a new order book manager with an explicit shard count.
    ///
    /// This is primarily useful for tests and tuning. A shard count of zero is
    /// treated as one shard.
    pub fn with_shard_count(max_depth: usize, shard_count: usize) -> Self {
        let shard_count = shard_count.max(1);
        let shards = (0..shard_count)
            .map(|_| BookShard::default())
            .collect::<Vec<_>>()
            .into();

        Self { shards, max_depth }
    }

    #[inline]
    fn shard_for(&self, token_id: &str) -> &BookShard {
        &self.shards[shard_index(token_id, self.shards.len())]
    }

    /// Get or create an order book for a token
    /// If we don't have a book for this token yet, create a new empty one
    pub fn get_or_create_book(&self, token_id: &str) -> Result<OrderBook> {
        let shard = self.shard_for(token_id);
        let mut books = shard.books.write();

        if let Some(book) = books.get(token_id) {
            Ok(book.clone()) // Return a copy of the existing book
        } else {
            // Create a new book for this token
            let book = OrderBook::new(token_id.to_string(), self.max_depth);
            books.insert(token_id.to_string(), book.clone());
            Ok(book)
        }
    }

    /// Execute a closure with mutable access to a managed book.
    ///
    /// This is useful for hot-path update ingestion where you want to avoid allocating
    /// intermediate update structs (e.g., applying WS updates directly).
    pub fn with_book_mut<R>(
        &self,
        token_id: &str,
        f: impl FnOnce(&mut OrderBook) -> Result<R>,
    ) -> Result<R> {
        let shard = self.shard_for(token_id);
        let mut books = shard.books.write();

        let book = books.get_mut(token_id).ok_or_else(|| {
            PolyfillError::market_data(
                format!("No book found for token: {}", token_id),
                crate::errors::MarketDataErrorKind::TokenNotFound,
            )
        })?;

        f(book)
    }

    /// Update a book with a delta
    /// This is called when we receive real-time updates from the exchange
    pub fn apply_delta(&self, delta: OrderDelta) -> Result<()> {
        let shard = self.shard_for(delta.token_id.as_str());
        let mut books = shard.books.write();

        // Find the book for this token (must already exist)
        let book = books.get_mut(&delta.token_id).ok_or_else(|| {
            PolyfillError::market_data(
                format!("No book found for token: {}", delta.token_id),
                crate::errors::MarketDataErrorKind::TokenNotFound,
            )
        })?;

        // Apply the update to the specific book
        book.apply_delta(delta)
    }

    /// Apply a WebSocket `book` update to a managed book.
    ///
    /// This is the preferred way to ingest `StreamMessage::Book` updates into
    /// the in-memory order books (avoids rebuilding snapshots via per-level deltas).
    pub fn apply_book_update(&self, update: &BookUpdate) -> Result<()> {
        let shard = self.shard_for(update.asset_id.as_str());
        let mut books = shard.books.write();

        if !books.contains_key(update.asset_id.as_str()) {
            let token_id = update.asset_id.clone();
            books.insert(token_id.clone(), OrderBook::new(token_id, self.max_depth));
        }

        books
            .get_mut(update.asset_id.as_str())
            .ok_or_else(|| PolyfillError::internal_simple("Failed to insert order book"))?
            .apply_book_update(update)
    }

    /// Get a book snapshot
    /// Returns a copy of the current book state that won't change
    pub fn get_book(&self, token_id: &str) -> Result<crate::types::OrderBook> {
        let shard = self.shard_for(token_id);
        let books = shard.books.read();

        books
            .get(token_id)
            .map(|book| book.snapshot()) // Create a snapshot copy
            .ok_or_else(|| {
                PolyfillError::market_data(
                    format!("No book found for token: {}", token_id),
                    crate::errors::MarketDataErrorKind::TokenNotFound,
                )
            })
    }

    /// Get all available books
    /// Returns snapshots of every book we're currently tracking
    pub fn get_all_books(&self) -> Result<Vec<crate::types::OrderBook>> {
        let mut snapshots = Vec::new();
        for shard in self.shards.iter() {
            let books = shard.books.read();
            snapshots.extend(books.values().map(|book| book.snapshot()));
        }

        Ok(snapshots)
    }

    /// Remove stale books
    /// Cleans up books that haven't been updated recently (probably disconnected)
    /// This prevents memory leaks from accumulating dead books
    pub fn cleanup_stale_books(&self, max_age: std::time::Duration) -> Result<usize> {
        let mut removed = 0usize;

        for shard in self.shards.iter() {
            let mut books = shard.books.write();
            let initial_count = books.len();
            books.retain(|_, book| !book.is_stale(max_age)); // Keep only non-stale books
            removed += initial_count - books.len();
        }

        if removed > 0 {
            debug!("Removed {} stale order books", removed);
        }

        Ok(removed)
    }
}

/// Order book analytics and statistics
/// Provides a summary view of the book's health and characteristics
#[derive(Debug, Clone)]
pub struct BookAnalytics {
    pub token_id: String,
    pub timestamp: chrono::DateTime<Utc>,
    pub bid_count: usize,            // How many different bid price levels
    pub ask_count: usize,            // How many different ask price levels
    pub total_bid_size: Decimal,     // Total size of all bids combined
    pub total_ask_size: Decimal,     // Total size of all asks combined
    pub spread: Option<Decimal>,     // Current spread (ask - bid)
    pub spread_pct: Option<Decimal>, // Spread as percentage
    pub mid_price: Option<Decimal>,  // Current mid price
    pub volatility: Option<Decimal>, // Price volatility (if calculated)
}

impl OrderBook {
    /// Calculate analytics over all retained levels in this book.
    pub fn analytics(&self) -> BookAnalytics {
        let bid_count = self.bids.len();
        let ask_count = self.asks.len();
        // Sum up all bid/ask sizes, converting from fixed-point back to Decimal
        let total_bid_size_units: i64 = self.bids.iter_all().map(|(_, level)| level.qty).sum();
        let total_ask_size_units: i64 = self.asks.iter_all().map(|(_, level)| level.qty).sum();
        let total_bid_size = qty_to_decimal(total_bid_size_units);
        let total_ask_size = qty_to_decimal(total_ask_size_units);

        BookAnalytics {
            token_id: self.token_id.clone(),
            timestamp: self.timestamp,
            bid_count,
            ask_count,
            total_bid_size,
            total_ask_size,
            spread: self.spread(),
            spread_pct: self.spread_pct(),
            mid_price: self.mid_price(),
            volatility: self.calculate_volatility(),
        }
    }

    /// Calculate price volatility (simplified)
    /// This is a placeholder - real volatility needs historical price data
    fn calculate_volatility(&self) -> Option<Decimal> {
        // This is a simplified volatility calculation
        // In a real implementation, you'd want to track price history over time
        // and calculate standard deviation of price changes
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::str::FromStr;
    use std::time::Duration; // Convenient macro for creating Decimal literals

    #[test]
    fn test_order_book_creation() {
        // Test that we can create a new empty order book
        let book = OrderBook::new("test_token".to_string(), 10);
        assert_eq!(book.token_id, "test_token");
        assert_eq!(book.bids.len(), 0); // Should start empty
        assert_eq!(book.asks.len(), 0); // Should start empty
    }

    #[test]
    fn test_set_tick_size_requires_exact_fixed_point_value() {
        let mut book = OrderBook::new("test_token".to_string(), 10);

        book.set_tick_size(dec!(0.0001)).unwrap();

        let err = book.set_tick_size(dec!(0.00005)).unwrap_err();
        assert!(err.to_string().contains("Invalid tick size"));

        let err = book.set_tick_size(Decimal::ZERO).unwrap_err();
        assert!(err.to_string().contains("Invalid tick size"));
    }

    #[test]
    fn test_order_book_manager_routes_tokens_to_shards() {
        let shard_count = 4;
        let first_token = "test_token_0";
        let first_shard = shard_index(first_token, shard_count);
        let second_token = (1..100)
            .map(|idx| format!("test_token_{idx}"))
            .find(|token| shard_index(token, shard_count) != first_shard)
            .expect("test tokens should cover multiple shards");

        let manager = OrderBookManager::with_shard_count(10, shard_count);
        manager.get_or_create_book(first_token).unwrap();
        manager.get_or_create_book(&second_token).unwrap();

        manager
            .apply_delta(OrderDelta {
                token_id: first_token.to_string(),
                timestamp: Utc::now(),
                side: Side::BUY,
                price: dec!(0.50),
                size: dec!(100),
                sequence: 1,
            })
            .unwrap();
        manager
            .apply_delta(OrderDelta {
                token_id: second_token.clone(),
                timestamp: Utc::now(),
                side: Side::SELL,
                price: dec!(0.60),
                size: dec!(200),
                sequence: 1,
            })
            .unwrap();

        let first_book = manager.get_book(first_token).unwrap();
        let second_book = manager.get_book(&second_token).unwrap();

        assert_eq!(first_book.bids.len(), 1);
        assert_eq!(first_book.asks.len(), 0);
        assert_eq!(second_book.bids.len(), 0);
        assert_eq!(second_book.asks.len(), 1);
        assert_eq!(manager.get_all_books().unwrap().len(), 2);
    }

    #[test]
    fn test_apply_delta() {
        // Test that we can apply order book updates
        let mut book = OrderBook::new("test_token".to_string(), 10);

        // Create a buy order at $0.50 for 100 tokens
        let delta = OrderDelta {
            token_id: "test_token".to_string(),
            timestamp: Utc::now(),
            side: Side::BUY,
            price: dec!(0.5),
            size: dec!(100),
            sequence: 1,
        };

        book.apply_delta(delta).unwrap();
        assert_eq!(book.sequence, 1); // Sequence should update
        assert_eq!(book.last_delta_sequence, 1);
        assert_eq!(book.last_snapshot_timestamp_ms, 0);
        assert_eq!(book.best_bid().unwrap().price, dec!(0.5)); // Should be our bid
        assert_eq!(book.best_bid().unwrap().size, dec!(100)); // Should be our size
    }

    #[test]
    fn test_sorted_side_ordering_and_removal() {
        let mut book = OrderBook::new("test_token".to_string(), 10);

        for (sequence, price) in [(1, dec!(0.73)), (2, dec!(0.75)), (3, dec!(0.74))] {
            book.apply_delta(OrderDelta {
                token_id: "test_token".to_string(),
                timestamp: Utc::now(),
                side: Side::BUY,
                price,
                size: dec!(100),
                sequence,
            })
            .unwrap();
        }

        for (sequence, price) in [(4, dec!(0.78)), (5, dec!(0.76)), (6, dec!(0.77))] {
            book.apply_delta(OrderDelta {
                token_id: "test_token".to_string(),
                timestamp: Utc::now(),
                side: Side::SELL,
                price,
                size: dec!(100),
                sequence,
            })
            .unwrap();
        }

        let bids: Vec<_> = book
            .bids(Some(3))
            .into_iter()
            .map(|level| level.price)
            .collect();
        let asks: Vec<_> = book
            .asks(Some(3))
            .into_iter()
            .map(|level| level.price)
            .collect();
        assert_eq!(bids, vec![dec!(0.75), dec!(0.74), dec!(0.73)]);
        assert_eq!(asks, vec![dec!(0.76), dec!(0.77), dec!(0.78)]);

        book.apply_delta(OrderDelta {
            token_id: "test_token".to_string(),
            timestamp: Utc::now(),
            side: Side::BUY,
            price: dec!(0.75),
            size: Decimal::ZERO,
            sequence: 7,
        })
        .unwrap();
        book.apply_delta(OrderDelta {
            token_id: "test_token".to_string(),
            timestamp: Utc::now(),
            side: Side::SELL,
            price: dec!(0.76),
            size: Decimal::ZERO,
            sequence: 8,
        })
        .unwrap();

        assert_eq!(book.best_bid().unwrap().price, dec!(0.74));
        assert_eq!(book.best_ask().unwrap().price, dec!(0.77));
    }

    #[test]
    fn test_book_update_replaces_snapshot_and_uses_millis_timestamp() {
        let mut book = OrderBook::new("test_token".to_string(), 10);
        let timestamp = 1_757_908_892_351;

        book.apply_book_update(&BookUpdate {
            asset_id: "test_token".to_string(),
            market: "0xabc".to_string(),
            timestamp,
            bids: vec![
                OrderSummary {
                    price: dec!(0.48),
                    size: dec!(10),
                },
                OrderSummary {
                    price: dec!(0.49),
                    size: dec!(20),
                },
            ],
            asks: vec![
                OrderSummary {
                    price: dec!(0.52),
                    size: dec!(30),
                },
                OrderSummary {
                    price: dec!(0.53),
                    size: dec!(40),
                },
            ],
            hash: None,
        })
        .unwrap();

        book.apply_book_update(&BookUpdate {
            asset_id: "test_token".to_string(),
            market: "0xabc".to_string(),
            timestamp: timestamp + 1,
            bids: vec![OrderSummary {
                price: dec!(0.49),
                size: dec!(25),
            }],
            asks: vec![OrderSummary {
                price: dec!(0.53),
                size: dec!(45),
            }],
            hash: None,
        })
        .unwrap();

        assert_eq!(book.timestamp.timestamp_millis(), timestamp as i64 + 1);
        assert_eq!(book.bids(None).len(), 1);
        assert_eq!(book.asks(None).len(), 1);
        assert_eq!(book.best_bid().unwrap().price, dec!(0.49));
        assert_eq!(book.best_bid().unwrap().size, dec!(25));
        assert_eq!(book.best_ask().unwrap().price, dec!(0.53));
        assert_eq!(book.best_ask().unwrap().size, dec!(45));
    }

    #[test]
    fn test_ws_snapshot_timestamp_does_not_block_delta_sequence() {
        let mut book = OrderBook::new("test_token".to_string(), 10);
        let snapshot_timestamp_ms = 1_757_908_892_351;
        let levels = [
            ParsedBookLevel {
                side: Side::BUY,
                price_ticks: 5_000,
                size_units: 100_000,
            },
            ParsedBookLevel {
                side: Side::SELL,
                price_ticks: 6_000,
                size_units: 100_000,
            },
        ];

        assert!(book
            .apply_ws_book_snapshot_fast("test_token", snapshot_timestamp_ms, None, &levels)
            .unwrap());

        assert_eq!(book.sequence, 0);
        assert_eq!(book.last_delta_sequence, 0);
        assert_eq!(book.last_snapshot_timestamp_ms, snapshot_timestamp_ms);

        book.apply_delta(OrderDelta {
            token_id: "test_token".to_string(),
            timestamp: Utc::now(),
            side: Side::BUY,
            price: dec!(0.51),
            size: dec!(11),
            sequence: 1,
        })
        .unwrap();

        assert_eq!(book.sequence, 1);
        assert_eq!(book.last_delta_sequence, 1);
        assert_eq!(book.last_snapshot_timestamp_ms, snapshot_timestamp_ms);
        assert_eq!(book.best_bid().unwrap().price, dec!(0.51));
    }

    #[test]
    fn test_delta_sequence_does_not_block_snapshot_timestamp() {
        let mut book = OrderBook::new("test_token".to_string(), 10);

        book.apply_delta(OrderDelta {
            token_id: "test_token".to_string(),
            timestamp: Utc::now(),
            side: Side::BUY,
            price: dec!(0.40),
            size: dec!(10),
            sequence: 10_000,
        })
        .unwrap();

        book.apply_book_update(&BookUpdate {
            asset_id: "test_token".to_string(),
            market: "0xabc".to_string(),
            timestamp: 1_000,
            bids: vec![OrderSummary {
                price: dec!(0.50),
                size: dec!(20),
            }],
            asks: vec![OrderSummary {
                price: dec!(0.60),
                size: dec!(30),
            }],
            hash: None,
        })
        .unwrap();

        assert_eq!(book.sequence, 10_000);
        assert_eq!(book.last_delta_sequence, 10_000);
        assert_eq!(book.last_snapshot_timestamp_ms, 1_000);
        assert_eq!(book.best_bid().unwrap().price, dec!(0.50));
        assert_eq!(book.best_ask().unwrap().price, dec!(0.60));

        let snapshot = book.snapshot();
        assert_eq!(snapshot.sequence, 10_000);
        assert_eq!(snapshot.last_delta_sequence, 10_000);
        assert_eq!(snapshot.last_snapshot_timestamp_ms, 1_000);
    }

    #[test]
    fn test_book_update_allows_same_timestamp_with_different_hash() {
        let mut book = OrderBook::new("test_token".to_string(), 10);
        let timestamp = 1_757_908_892_351;

        book.apply_book_update(&BookUpdate {
            asset_id: "test_token".to_string(),
            market: "0xabc".to_string(),
            timestamp,
            bids: vec![OrderSummary {
                price: dec!(0.48),
                size: dec!(10),
            }],
            asks: vec![OrderSummary {
                price: dec!(0.52),
                size: dec!(20),
            }],
            hash: Some("hash_a".to_string()),
        })
        .unwrap();

        book.apply_book_update(&BookUpdate {
            asset_id: "test_token".to_string(),
            market: "0xabc".to_string(),
            timestamp,
            bids: vec![OrderSummary {
                price: dec!(0.49),
                size: dec!(30),
            }],
            asks: vec![OrderSummary {
                price: dec!(0.53),
                size: dec!(40),
            }],
            hash: Some("hash_b".to_string()),
        })
        .unwrap();

        assert_eq!(book.best_bid().unwrap().price, dec!(0.49));
        assert_eq!(book.best_bid().unwrap().size, dec!(30));
        assert_eq!(book.best_ask().unwrap().price, dec!(0.53));
        assert_eq!(book.best_ask().unwrap().size, dec!(40));

        book.apply_book_update(&BookUpdate {
            asset_id: "test_token".to_string(),
            market: "0xabc".to_string(),
            timestamp,
            bids: vec![OrderSummary {
                price: dec!(0.50),
                size: dec!(50),
            }],
            asks: vec![OrderSummary {
                price: dec!(0.54),
                size: dec!(60),
            }],
            hash: Some("hash_b".to_string()),
        })
        .unwrap();

        assert_eq!(book.best_bid().unwrap().price, dec!(0.49));
        assert_eq!(book.best_ask().unwrap().price, dec!(0.53));
    }

    #[test]
    fn test_book_update_rejects_same_timestamp_without_hash_tie_breaker() {
        let mut book = OrderBook::new("test_token".to_string(), 10);
        let timestamp = 1_757_908_892_351;

        book.apply_book_update(&BookUpdate {
            asset_id: "test_token".to_string(),
            market: "0xabc".to_string(),
            timestamp,
            bids: vec![OrderSummary {
                price: dec!(0.48),
                size: dec!(10),
            }],
            asks: vec![OrderSummary {
                price: dec!(0.52),
                size: dec!(20),
            }],
            hash: None,
        })
        .unwrap();

        book.apply_book_update(&BookUpdate {
            asset_id: "test_token".to_string(),
            market: "0xabc".to_string(),
            timestamp,
            bids: vec![OrderSummary {
                price: dec!(0.49),
                size: dec!(30),
            }],
            asks: vec![OrderSummary {
                price: dec!(0.53),
                size: dec!(40),
            }],
            hash: None,
        })
        .unwrap();

        assert_eq!(book.best_bid().unwrap().price, dec!(0.48));
        assert_eq!(book.best_ask().unwrap().price, dec!(0.52));
    }

    #[test]
    fn test_book_update_error_keeps_existing_snapshot() {
        let mut book = OrderBook::new("test_token".to_string(), 10);
        book.set_tick_size_ticks(100);

        book.apply_book_update(&BookUpdate {
            asset_id: "test_token".to_string(),
            market: "0xabc".to_string(),
            timestamp: 100,
            bids: vec![OrderSummary {
                price: dec!(0.50),
                size: dec!(10),
            }],
            asks: vec![OrderSummary {
                price: dec!(0.60),
                size: dec!(20),
            }],
            hash: None,
        })
        .unwrap();

        let err = book
            .apply_book_update(&BookUpdate {
                asset_id: "test_token".to_string(),
                market: "0xabc".to_string(),
                timestamp: 101,
                bids: vec![
                    OrderSummary {
                        price: dec!(0.51),
                        size: dec!(11),
                    },
                    OrderSummary {
                        price: dec!(0.515),
                        size: dec!(12),
                    },
                ],
                asks: vec![OrderSummary {
                    price: dec!(0.61),
                    size: dec!(21),
                }],
                hash: None,
            })
            .unwrap_err();

        assert!(err.to_string().contains("Price not aligned"));
        assert_eq!(book.sequence, 0);
        assert_eq!(book.last_delta_sequence, 0);
        assert_eq!(book.last_snapshot_timestamp_ms, 100);
        assert_eq!(book.bids(None).len(), 1);
        assert_eq!(book.asks(None).len(), 1);
        assert_eq!(book.best_bid().unwrap().price, dec!(0.50));
        assert_eq!(book.best_bid().unwrap().size, dec!(10));
        assert_eq!(book.best_ask().unwrap().price, dec!(0.60));
        assert_eq!(book.best_ask().unwrap().size, dec!(20));
    }

    #[test]
    fn test_book_update_depth_keeps_best_prices_independent_of_payload_order() {
        let mut book = OrderBook::new("test_token".to_string(), 2);

        book.apply_book_update(&BookUpdate {
            asset_id: "test_token".to_string(),
            market: "0xabc".to_string(),
            timestamp: 1_757_908_892_351,
            bids: vec![
                OrderSummary {
                    price: dec!(0.49),
                    size: dec!(10),
                },
                OrderSummary {
                    price: dec!(0.50),
                    size: dec!(20),
                },
                OrderSummary {
                    price: dec!(0.48),
                    size: dec!(30),
                },
            ],
            asks: vec![
                OrderSummary {
                    price: dec!(0.52),
                    size: dec!(40),
                },
                OrderSummary {
                    price: dec!(0.54),
                    size: dec!(50),
                },
                OrderSummary {
                    price: dec!(0.53),
                    size: dec!(60),
                },
            ],
            hash: None,
        })
        .unwrap();

        let bids = book.bids(None);
        assert_eq!(bids.len(), 2);
        assert_eq!(bids[0].price, dec!(0.50));
        assert_eq!(bids[1].price, dec!(0.49));

        let asks = book.asks(None);
        assert_eq!(asks.len(), 2);
        assert_eq!(asks[0].price, dec!(0.52));
        assert_eq!(asks[1].price, dec!(0.53));
    }

    #[test]
    fn test_spread_calculation() {
        // Test that we can calculate the spread between bid and ask
        let mut book = OrderBook::new("test_token".to_string(), 10);

        // Add a bid at $0.50
        book.apply_delta(OrderDelta {
            token_id: "test_token".to_string(),
            timestamp: Utc::now(),
            side: Side::BUY,
            price: dec!(0.5),
            size: dec!(100),
            sequence: 1,
        })
        .unwrap();

        // Add an ask at $0.52
        book.apply_delta(OrderDelta {
            token_id: "test_token".to_string(),
            timestamp: Utc::now(),
            side: Side::SELL,
            price: dec!(0.52),
            size: dec!(100),
            sequence: 2,
        })
        .unwrap();

        let spread = book.spread().unwrap();
        assert_eq!(spread, dec!(0.02)); // $0.52 - $0.50 = $0.02
    }

    #[test]
    fn test_market_impact() {
        // Test market impact calculation for a large order
        let mut book = OrderBook::new("test_token".to_string(), 10);

        // Add multiple ask levels (people selling at different prices)
        // $0.50 for 100 tokens, $0.51 for 100 tokens, $0.52 for 100 tokens
        for (i, price) in [dec!(0.50), dec!(0.51), dec!(0.52)].iter().enumerate() {
            book.apply_delta(OrderDelta {
                token_id: "test_token".to_string(),
                timestamp: Utc::now(),
                side: Side::SELL,
                price: *price,
                size: dec!(100),
                sequence: i as u64 + 1,
            })
            .unwrap();
        }

        // Try to buy 150 tokens (will need to hit multiple price levels)
        let impact = book.calculate_market_impact(Side::BUY, dec!(150)).unwrap();
        assert!(impact.average_price > dec!(0.50)); // Should be worse than best price
        assert!(impact.average_price < dec!(0.51)); // But not as bad as second level
    }

    #[test]
    fn test_apply_bid_delta_legacy() {
        let mut book = OrderBook::new("test_token".to_string(), 10);

        // Test adding a bid
        book.apply_bid_delta(
            Decimal::from_str("0.75").unwrap(),
            Decimal::from_str("100.0").unwrap(),
        );

        let best_bid = book.best_bid();
        assert!(best_bid.is_some());
        let bid = best_bid.unwrap();
        assert_eq!(bid.price, Decimal::from_str("0.75").unwrap());
        assert_eq!(bid.size, Decimal::from_str("100.0").unwrap());

        // Test updating the bid
        book.apply_bid_delta(
            Decimal::from_str("0.75").unwrap(),
            Decimal::from_str("150.0").unwrap(),
        );
        let updated_bid = book.best_bid().unwrap();
        assert_eq!(updated_bid.size, Decimal::from_str("150.0").unwrap());

        // Test removing the bid
        book.apply_bid_delta(Decimal::from_str("0.75").unwrap(), Decimal::ZERO);
        assert!(book.best_bid().is_none());
    }

    #[test]
    fn test_apply_ask_delta_legacy() {
        let mut book = OrderBook::new("test_token".to_string(), 10);

        // Test adding an ask
        book.apply_ask_delta(
            Decimal::from_str("0.76").unwrap(),
            Decimal::from_str("50.0").unwrap(),
        );

        let best_ask = book.best_ask();
        assert!(best_ask.is_some());
        let ask = best_ask.unwrap();
        assert_eq!(ask.price, Decimal::from_str("0.76").unwrap());
        assert_eq!(ask.size, Decimal::from_str("50.0").unwrap());

        // Test updating the ask
        book.apply_ask_delta(
            Decimal::from_str("0.76").unwrap(),
            Decimal::from_str("75.0").unwrap(),
        );
        let updated_ask = book.best_ask().unwrap();
        assert_eq!(updated_ask.size, Decimal::from_str("75.0").unwrap());

        // Test removing the ask
        book.apply_ask_delta(Decimal::from_str("0.76").unwrap(), Decimal::ZERO);
        assert!(book.best_ask().is_none());
    }

    #[test]
    fn test_liquidity_analysis() {
        let mut book = OrderBook::new("test_token".to_string(), 10);

        // Build order book using legacy methods
        book.apply_bid_delta(
            Decimal::from_str("0.75").unwrap(),
            Decimal::from_str("100.0").unwrap(),
        );
        book.apply_bid_delta(
            Decimal::from_str("0.74").unwrap(),
            Decimal::from_str("50.0").unwrap(),
        );
        book.apply_ask_delta(
            Decimal::from_str("0.76").unwrap(),
            Decimal::from_str("80.0").unwrap(),
        );
        book.apply_ask_delta(
            Decimal::from_str("0.77").unwrap(),
            Decimal::from_str("120.0").unwrap(),
        );

        // Test liquidity at specific price - when buying, we look at ask liquidity
        let buy_liquidity = book.liquidity_at_price(Decimal::from_str("0.76").unwrap(), Side::BUY);
        assert_eq!(buy_liquidity, Decimal::from_str("80.0").unwrap());

        // Test liquidity at specific price - when selling, we look at bid liquidity
        let sell_liquidity =
            book.liquidity_at_price(Decimal::from_str("0.75").unwrap(), Side::SELL);
        assert_eq!(sell_liquidity, Decimal::from_str("100.0").unwrap());

        // Test liquidity in range - when buying, we look at ask liquidity in range
        let buy_range_liquidity = book.liquidity_in_range(
            Decimal::from_str("0.74").unwrap(),
            Decimal::from_str("0.77").unwrap(),
            Side::BUY,
        );
        // Should include ask liquidity: 80 (0.76 ask) + 120 (0.77 ask) = 200
        assert_eq!(buy_range_liquidity, Decimal::from_str("200.0").unwrap());

        // Test liquidity in range - when selling, we look at bid liquidity in range
        let sell_range_liquidity = book.liquidity_in_range(
            Decimal::from_str("0.74").unwrap(),
            Decimal::from_str("0.77").unwrap(),
            Side::SELL,
        );
        // Should include bid liquidity: 50 (0.74 bid) + 100 (0.75 bid) = 150
        assert_eq!(sell_range_liquidity, Decimal::from_str("150.0").unwrap());
    }

    #[test]
    fn test_book_validation() {
        let mut book = OrderBook::new("test_token".to_string(), 10);

        // Empty book should be valid
        assert!(book.is_valid());

        // Add normal levels
        book.apply_bid_delta(
            Decimal::from_str("0.75").unwrap(),
            Decimal::from_str("100.0").unwrap(),
        );
        book.apply_ask_delta(
            Decimal::from_str("0.76").unwrap(),
            Decimal::from_str("80.0").unwrap(),
        );
        assert!(book.is_valid());

        // Create crossed book (invalid) - bid higher than ask
        book.apply_bid_delta(
            Decimal::from_str("0.77").unwrap(),
            Decimal::from_str("50.0").unwrap(),
        );
        assert!(!book.is_valid());
    }

    #[test]
    fn test_book_staleness() {
        let mut book = OrderBook::new("test_token".to_string(), 10);

        // Fresh book should not be stale
        assert!(!book.is_stale(Duration::from_secs(60))); // 60 second threshold

        // Add some data
        book.apply_bid_delta(
            Decimal::from_str("0.75").unwrap(),
            Decimal::from_str("100.0").unwrap(),
        );
        assert!(!book.is_stale(Duration::from_secs(60)));

        // Note: We can't easily test actual staleness without manipulating time,
        // but we can test the method exists and works with fresh data
    }

    #[test]
    fn test_incremental_best_price_retains_levels_beyond_view_depth() {
        for (side, prices) in [
            (Side::SELL, [dec!(0.50), dec!(0.60), dec!(0.70), dec!(0.80)]),
            (Side::BUY, [dec!(0.50), dec!(0.40), dec!(0.30), dec!(0.20)]),
        ] {
            for source in ["snapshot", "ws_snapshot", "deltas"] {
                for fast in [false, true] {
                    let mut book = OrderBook::new("test_token".into(), 2);
                    let apply = |book: &mut OrderBook, price, size, sequence| {
                        let delta = OrderDelta {
                            token_id: "test_token".into(),
                            timestamp: Utc::now(),
                            side,
                            price,
                            size,
                            sequence,
                        };
                        if fast {
                            book.apply_delta_fast(
                                FastOrderDelta::from_order_delta(&delta, None).unwrap(),
                            )
                            .unwrap();
                        } else {
                            book.apply_delta(delta).unwrap();
                        }
                    };
                    match source {
                        "snapshot" => {
                            let levels = prices[..3]
                                .iter()
                                .map(|&price| OrderSummary {
                                    price,
                                    size: dec!(10),
                                })
                                .collect();
                            let (bids, asks) = if side == Side::BUY {
                                (levels, vec![])
                            } else {
                                (vec![], levels)
                            };
                            book.apply_book_update(&BookUpdate {
                                asset_id: "test_token".into(),
                                market: "test".into(),
                                timestamp: 100,
                                bids,
                                asks,
                                hash: None,
                            })
                            .unwrap();
                        },
                        "ws_snapshot" => {
                            let levels: Vec<_> = prices[..3]
                                .iter()
                                .map(|&price| ParsedBookLevel {
                                    side,
                                    price_ticks: decimal_to_price_exact(price).unwrap(),
                                    size_units: decimal_to_qty(dec!(10)).unwrap(),
                                })
                                .collect();
                            book.apply_ws_book_snapshot_fast("test_token", 100, None, &levels)
                                .unwrap();
                        },
                        "deltas" => {
                            for (index, &price) in prices[..3].iter().enumerate() {
                                apply(&mut book, price, dec!(10), index as u64 + 1);
                            }
                        },
                        _ => unreachable!(),
                    }
                    let view = if side == Side::BUY {
                        book.bids(None)
                    } else {
                        book.asks(None)
                    };
                    assert_eq!(view.len(), 2);
                    apply(&mut book, prices[0], Decimal::ZERO, 4);
                    apply(&mut book, prices[3], dec!(10), 5);
                    apply(&mut book, prices[1], Decimal::ZERO, 6);
                    let best = if side == Side::BUY {
                        book.best_bid()
                    } else {
                        book.best_ask()
                    };
                    assert_eq!(
                        best.unwrap().price,
                        prices[2],
                        "{side:?} {source} fast={fast}"
                    );

                    // A new full snapshot still removes levels omitted from that snapshot.
                    book.apply_ws_book_snapshot_fast(
                        "test_token",
                        200,
                        None,
                        &[ParsedBookLevel {
                            side,
                            price_ticks: decimal_to_price_exact(prices[3]).unwrap(),
                            size_units: decimal_to_qty(dec!(10)).unwrap(),
                        }],
                    )
                    .unwrap();
                    apply(&mut book, prices[3], Decimal::ZERO, 7);
                    let best = if side == Side::BUY {
                        book.best_bid()
                    } else {
                        book.best_ask()
                    };
                    assert!(best.is_none());
                }
            }
        }
    }

    #[test]
    fn test_depth_limits_views_while_analytics_use_all_levels() {
        let mut book = OrderBook::new("test_token".into(), 2);
        for (index, price) in [dec!(0.50), dec!(0.60), dec!(0.70)].into_iter().enumerate() {
            book.apply_delta(OrderDelta {
                token_id: "test_token".into(),
                timestamp: Utc::now(),
                side: Side::SELL,
                price,
                size: dec!(10),
                sequence: index as u64 + 1,
            })
            .unwrap();
        }
        assert_eq!(book.asks(None).len(), 2);
        assert_eq!(book.asks(Some(100)).len(), 2);
        assert_eq!(book.asks_fast(Some(100)).len(), 2);
        assert_eq!(book.asks(Some(1)).len(), 1);
        assert_eq!(book.snapshot().asks.len(), 2);
        assert_eq!(book.analytics().ask_count, 3);
        assert_eq!(book.analytics().total_ask_size, dec!(30));
        assert_eq!(
            book.calculate_market_impact(Side::BUY, dec!(30))
                .unwrap()
                .size_filled,
            dec!(30)
        );
    }

    #[test]
    fn test_depth_management() {
        let mut book = OrderBook::new("test_token".to_string(), 3); // Only 3 levels

        // Add multiple levels
        book.apply_bid_delta(
            Decimal::from_str("0.75").unwrap(),
            Decimal::from_str("100.0").unwrap(),
        );
        book.apply_bid_delta(
            Decimal::from_str("0.74").unwrap(),
            Decimal::from_str("50.0").unwrap(),
        );
        book.apply_bid_delta(
            Decimal::from_str("0.73").unwrap(),
            Decimal::from_str("20.0").unwrap(),
        );

        book.apply_ask_delta(
            Decimal::from_str("0.76").unwrap(),
            Decimal::from_str("80.0").unwrap(),
        );
        book.apply_ask_delta(
            Decimal::from_str("0.77").unwrap(),
            Decimal::from_str("40.0").unwrap(),
        );
        book.apply_ask_delta(
            Decimal::from_str("0.78").unwrap(),
            Decimal::from_str("30.0").unwrap(),
        );

        // Should have levels on each side
        let bids = book.bids(Some(3));
        let asks = book.asks(Some(3));

        assert!(bids.len() <= 3);
        assert!(asks.len() <= 3);

        // Best levels should be there
        assert_eq!(
            book.best_bid().unwrap().price,
            Decimal::from_str("0.75").unwrap()
        );
        assert_eq!(
            book.best_ask().unwrap().price,
            Decimal::from_str("0.76").unwrap()
        );
    }

    #[test]
    fn test_fast_operations() {
        let mut book = OrderBook::new("test_token".to_string(), 10);

        // Test using legacy methods which call fast operations internally
        book.apply_bid_delta(
            Decimal::from_str("0.75").unwrap(),
            Decimal::from_str("100.0").unwrap(),
        );
        book.apply_ask_delta(
            Decimal::from_str("0.76").unwrap(),
            Decimal::from_str("80.0").unwrap(),
        );

        let best_bid_fast = book.best_bid_fast();
        let best_ask_fast = book.best_ask_fast();

        assert!(best_bid_fast.is_some());
        assert!(best_ask_fast.is_some());

        // Test fast spread and mid price
        let spread_fast = book.spread_fast();
        let mid_fast = book.mid_price_fast();

        assert!(spread_fast.is_some()); // Should have a spread
        assert!(mid_fast.is_some()); // Should have a mid price
    }
}
