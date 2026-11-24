//! Trade execution simulation and fill handling for the Polymarket client.
//!
//! This module is intended for local simulation, slippage checks, and fill-event
//! bookkeeping. It is not a hot-path matching or execution engine: market-order
//! simulation materializes book levels as `Decimal` values, creates UUID-backed
//! fill IDs, clones order identifiers, and stores owned fill history.

use crate::errors::{PolyfillError, Result};
use crate::types::*;
use crate::utils::math;
use alloy_primitives::Address;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Fill execution result
#[derive(Debug, Clone)]
pub struct FillResult {
    pub order_id: String,
    pub fills: Vec<FillEvent>,
    pub total_size: Decimal,
    pub average_price: Decimal,
    pub total_cost: Decimal,
    pub fees: Decimal,
    pub status: FillStatus,
    pub timestamp: DateTime<Utc>,
}

/// Fill execution status
#[derive(Debug, Clone, PartialEq)]
pub enum FillStatus {
    /// Order was fully filled
    Filled,
    /// Order was partially filled
    Partial,
    /// Order was not filled (insufficient liquidity)
    Unfilled,
    /// Order was rejected
    Rejected,
}

/// Fill simulation and bookkeeping engine.
///
/// `FillEngine` favors ergonomic `Decimal` outputs and owned `FillEvent` records
/// over allocation-sensitive execution. It should not be used as evidence that the
/// order-routing or market-data hot paths are allocation-free.
#[derive(Debug)]
pub struct FillEngine {
    /// Minimum fill size for market orders
    min_fill_size: Decimal,
    /// Maximum slippage tolerance (as percentage)
    max_slippage_pct: Decimal,
    /// Fee rate in basis points
    fee_rate_bps: u32,
    /// Track fills by order ID
    fills: HashMap<String, Vec<FillEvent>>,
}

impl FillEngine {
    /// Create a new fill simulation engine.
    pub fn new(min_fill_size: Decimal, max_slippage_pct: Decimal, fee_rate_bps: u32) -> Self {
        Self {
            min_fill_size,
            max_slippage_pct,
            fee_rate_bps,
            fills: HashMap::new(),
        }
    }

    /// Simulate executing a market order against an order book.
    /// Buy amounts are collateral budgets before fees. Sell amounts are share quantities.
    ///
    /// This method allocates and converts internal book levels back to `Decimal`
    /// via `OrderBook::asks`/`OrderBook::bids`, then builds owned `FillEvent`
    /// records. It is suitable for simulation and reporting, not latency-sensitive
    /// execution benchmarking.
    pub fn execute_market_order(
        &mut self,
        order: &MarketOrderRequest,
        book: &crate::book::OrderBook,
    ) -> Result<FillResult> {
        let start_time = Utc::now();

        // Validate order
        self.validate_market_order(order)?;

        // Get available liquidity
        let levels = match order.side {
            Side::BUY => book.asks(None),
            Side::SELL => book.bids(None),
        };

        if levels.is_empty() {
            return Ok(FillResult {
                order_id: order
                    .client_id
                    .clone()
                    .unwrap_or_else(|| "market_order".to_string()),
                fills: Vec::new(),
                total_size: Decimal::ZERO,
                average_price: Decimal::ZERO,
                total_cost: Decimal::ZERO,
                fees: Decimal::ZERO,
                status: FillStatus::Unfilled,
                timestamp: start_time,
            });
        }

        // Execute fills
        let mut fills = Vec::new();
        let mut remaining_amount = order.amount;
        let mut total_cost = Decimal::ZERO;
        let mut total_size = Decimal::ZERO;

        for level in levels {
            if remaining_amount.is_zero() {
                break;
            }

            let (fill_size, fill_cost) = match order.side {
                Side::BUY => {
                    // Spend collateral first so rounding fractional shares cannot
                    // leave a spurious remaining budget or overspend it.
                    let cost = remaining_amount.min(level.size * level.price);
                    (cost / level.price, cost)
                },
                Side::SELL => {
                    let size = remaining_amount.min(level.size);
                    (size, size * level.price)
                },
            };

            // Calculate fee
            let fee = self.calculate_fee(fill_cost);

            let fill = FillEvent {
                id: uuid::Uuid::new_v4().to_string(),
                order_id: order
                    .client_id
                    .clone()
                    .unwrap_or_else(|| "market_order".to_string()),
                token_id: order.token_id.clone(),
                side: order.side,
                price: level.price,
                size: fill_size,
                timestamp: Utc::now(),
                maker_address: Address::ZERO, // TODO: Get from level
                taker_address: Address::ZERO, // TODO: Get from order
                fee,
            };

            fills.push(fill);
            total_cost += fill_cost;
            total_size += fill_size;
            remaining_amount -= match order.side {
                Side::BUY => fill_cost,
                Side::SELL => fill_size,
            };
        }

        // Check slippage
        if let Some(slippage) = self.calculate_slippage(order, &fills) {
            if slippage > self.max_slippage_pct {
                warn!(
                    "Slippage {}% exceeds maximum {}%",
                    slippage, self.max_slippage_pct
                );
                return Ok(FillResult {
                    order_id: order
                        .client_id
                        .clone()
                        .unwrap_or_else(|| "market_order".to_string()),
                    fills: Vec::new(),
                    total_size: Decimal::ZERO,
                    average_price: Decimal::ZERO,
                    total_cost: Decimal::ZERO,
                    fees: Decimal::ZERO,
                    status: FillStatus::Rejected,
                    timestamp: start_time,
                });
            }
        }

        // Determine status
        let status = if remaining_amount.is_zero() {
            FillStatus::Filled
        } else if total_size >= self.min_fill_size {
            FillStatus::Partial
        } else {
            FillStatus::Unfilled
        };

        let average_price = if total_size.is_zero() {
            Decimal::ZERO
        } else {
            total_cost / total_size
        };

        let total_fees: Decimal = fills.iter().map(|f| f.fee).sum();

        let result = FillResult {
            order_id: order
                .client_id
                .clone()
                .unwrap_or_else(|| "market_order".to_string()),
            fills,
            total_size,
            average_price,
            total_cost,
            fees: total_fees,
            status,
            timestamp: start_time,
        };

        // Store fills for tracking
        if !result.fills.is_empty() {
            self.fills
                .insert(result.order_id.clone(), result.fills.clone());
        }

        info!(
            "Market order executed: {} {} @ {} (avg: {})",
            result.total_size,
            order.side.as_str(),
            order.amount,
            result.average_price
        );

        Ok(result)
    }

    /// Simulate executing a limit order.
    pub fn execute_limit_order(
        &mut self,
        order: &OrderRequest,
        book: &crate::book::OrderBook,
    ) -> Result<FillResult> {
        let start_time = Utc::now();

        // Validate order
        self.validate_limit_order(order)?;

        // Check if order can be filled immediately
        let can_fill = match order.side {
            Side::BUY => {
                if let Some(best_ask) = book.best_ask() {
                    order.price >= best_ask.price
                } else {
                    false
                }
            },
            Side::SELL => {
                if let Some(best_bid) = book.best_bid() {
                    order.price <= best_bid.price
                } else {
                    false
                }
            },
        };

        if !can_fill {
            return Ok(FillResult {
                order_id: order
                    .client_id
                    .clone()
                    .unwrap_or_else(|| "limit_order".to_string()),
                fills: Vec::new(),
                total_size: Decimal::ZERO,
                average_price: Decimal::ZERO,
                total_cost: Decimal::ZERO,
                fees: Decimal::ZERO,
                status: FillStatus::Unfilled,
                timestamp: start_time,
            });
        }

        // Simulate immediate fill
        let fill = FillEvent {
            id: uuid::Uuid::new_v4().to_string(),
            order_id: order
                .client_id
                .clone()
                .unwrap_or_else(|| "limit_order".to_string()),
            token_id: order.token_id.clone(),
            side: order.side,
            price: order.price,
            size: order.size,
            timestamp: Utc::now(),
            maker_address: Address::ZERO,
            taker_address: Address::ZERO,
            fee: self.calculate_fee(order.price * order.size),
        };

        let result = FillResult {
            order_id: order
                .client_id
                .clone()
                .unwrap_or_else(|| "limit_order".to_string()),
            fills: vec![fill],
            total_size: order.size,
            average_price: order.price,
            total_cost: order.price * order.size,
            fees: self.calculate_fee(order.price * order.size),
            status: FillStatus::Filled,
            timestamp: start_time,
        };

        // Store fills for tracking
        self.fills
            .insert(result.order_id.clone(), result.fills.clone());

        info!(
            "Limit order executed: {} {} @ {}",
            result.total_size,
            order.side.as_str(),
            result.average_price
        );

        Ok(result)
    }

    /// Calculate slippage for a market order
    fn calculate_slippage(
        &self,
        order: &MarketOrderRequest,
        fills: &[FillEvent],
    ) -> Option<Decimal> {
        if fills.is_empty() {
            return None;
        }

        let total_cost: Decimal = fills.iter().map(|f| f.price * f.size).sum();
        let total_size: Decimal = fills.iter().map(|f| f.size).sum();
        let average_price = total_cost / total_size;

        // Get reference price (best bid/ask)
        let reference_price = match order.side {
            Side::BUY => fills.first()?.price,  // Best ask
            Side::SELL => fills.first()?.price, // Best bid
        };

        Some(math::calculate_slippage(
            reference_price,
            average_price,
            order.side,
        ))
    }

    /// Calculate fee for a trade
    fn calculate_fee(&self, notional: Decimal) -> Decimal {
        notional * Decimal::from(self.fee_rate_bps) / Decimal::from(10_000)
    }

    /// Validate market order parameters
    fn validate_market_order(&self, order: &MarketOrderRequest) -> Result<()> {
        if order.amount.is_zero() {
            return Err(PolyfillError::order(
                "Market order amount cannot be zero",
                crate::errors::OrderErrorKind::InvalidSize,
            ));
        }

        if order.amount < self.min_fill_size {
            return Err(PolyfillError::order(
                format!(
                    "Order size {} below minimum {}",
                    order.amount, self.min_fill_size
                ),
                crate::errors::OrderErrorKind::SizeConstraint,
            ));
        }

        Ok(())
    }

    /// Validate limit order parameters
    fn validate_limit_order(&self, order: &OrderRequest) -> Result<()> {
        if order.size.is_zero() {
            return Err(PolyfillError::order(
                "Limit order size cannot be zero",
                crate::errors::OrderErrorKind::InvalidSize,
            ));
        }

        if order.price.is_zero() {
            return Err(PolyfillError::order(
                "Limit order price cannot be zero",
                crate::errors::OrderErrorKind::InvalidPrice,
            ));
        }

        if order.size < self.min_fill_size {
            return Err(PolyfillError::order(
                format!(
                    "Order size {} below minimum {}",
                    order.size, self.min_fill_size
                ),
                crate::errors::OrderErrorKind::SizeConstraint,
            ));
        }

        Ok(())
    }

    /// Get fills for an order
    pub fn get_fills(&self, order_id: &str) -> Option<&[FillEvent]> {
        self.fills.get(order_id).map(|f| f.as_slice())
    }

    /// Get all fills
    pub fn get_all_fills(&self) -> Vec<&FillEvent> {
        self.fills.values().flatten().collect()
    }

    /// Clear fills for an order
    pub fn clear_fills(&mut self, order_id: &str) {
        self.fills.remove(order_id);
    }

    /// Get fill statistics
    pub fn get_stats(&self) -> FillStats {
        let total_fills = self.fills.values().flatten().count();
        let total_volume: Decimal = self.fills.values().flatten().map(|f| f.size).sum();
        let total_fees: Decimal = self.fills.values().flatten().map(|f| f.fee).sum();

        FillStats {
            total_orders: self.fills.len(),
            total_fills,
            total_volume,
            total_fees,
        }
    }
}

/// Fill statistics
#[derive(Debug, Clone)]
pub struct FillStats {
    pub total_orders: usize,
    pub total_fills: usize,
    pub total_volume: Decimal,
    pub total_fees: Decimal,
}

/// Fill event processor for real-time updates
#[derive(Debug)]
pub struct FillProcessor {
    /// Pending fills by order ID
    pending_fills: HashMap<String, Vec<FillEvent>>,
    /// Processed fills
    processed_fills: Vec<FillEvent>,
    /// Maximum pending fills to keep in memory
    max_pending: usize,
}

impl FillProcessor {
    /// Create a new fill processor
    pub fn new(max_pending: usize) -> Self {
        Self {
            pending_fills: HashMap::new(),
            processed_fills: Vec::new(),
            max_pending,
        }
    }

    /// Process a fill event
    pub fn process_fill(&mut self, fill: FillEvent) -> Result<()> {
        // Validate fill
        self.validate_fill(&fill)?;

        // Add to pending fills
        self.pending_fills
            .entry(fill.order_id.clone())
            .or_default()
            .push(fill.clone());

        // Move to processed if complete
        if self.is_order_complete(&fill.order_id) {
            if let Some(fills) = self.pending_fills.remove(&fill.order_id) {
                self.processed_fills.extend(fills);
            }
        }

        // Cleanup if too many pending
        if self.pending_fills.len() > self.max_pending {
            self.cleanup_old_pending();
        }

        debug!(
            "Processed fill: {} {} @ {}",
            fill.size,
            fill.side.as_str(),
            fill.price
        );

        Ok(())
    }

    /// Validate a fill event
    fn validate_fill(&self, fill: &FillEvent) -> Result<()> {
        if fill.size.is_zero() {
            return Err(PolyfillError::order(
                "Fill size cannot be zero",
                crate::errors::OrderErrorKind::InvalidSize,
            ));
        }

        if fill.price.is_zero() {
            return Err(PolyfillError::order(
                "Fill price cannot be zero",
                crate::errors::OrderErrorKind::InvalidPrice,
            ));
        }

        Ok(())
    }

    /// Check if an order is complete
    fn is_order_complete(&self, _order_id: &str) -> bool {
        // Simplified implementation - in practice you'd check against order book
        false
    }

    /// Cleanup old pending fills
    fn cleanup_old_pending(&mut self) {
        // Remove oldest pending fills
        let to_remove = self.pending_fills.len() - self.max_pending;
        let mut keys: Vec<_> = self.pending_fills.keys().cloned().collect();
        keys.sort(); // Simple ordering - in practice you'd use timestamps

        for key in keys.iter().take(to_remove) {
            self.pending_fills.remove(key);
        }
    }

    /// Get pending fills for an order
    pub fn get_pending_fills(&self, order_id: &str) -> Option<&[FillEvent]> {
        self.pending_fills.get(order_id).map(|f| f.as_slice())
    }

    /// Get processed fills
    pub fn get_processed_fills(&self) -> &[FillEvent] {
        &self.processed_fills
    }

    /// Get fill statistics
    pub fn get_stats(&self) -> FillProcessorStats {
        let total_pending: Decimal = self.pending_fills.values().flatten().map(|f| f.size).sum();
        let total_processed: Decimal = self.processed_fills.iter().map(|f| f.size).sum();

        FillProcessorStats {
            pending_orders: self.pending_fills.len(),
            pending_fills: self.pending_fills.values().flatten().count(),
            pending_volume: total_pending,
            processed_fills: self.processed_fills.len(),
            processed_volume: total_processed,
        }
    }
}

/// Fill processor statistics
#[derive(Debug, Clone)]
pub struct FillProcessorStats {
    pub pending_orders: usize,
    pub pending_fills: usize,
    pub pending_volume: Decimal,
    pub processed_fills: usize,
    pub processed_volume: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_fill_engine_creation() {
        let engine = FillEngine::new(dec!(1), dec!(5), 10);
        assert_eq!(engine.min_fill_size, dec!(1));
        assert_eq!(engine.max_slippage_pct, dec!(5));
        assert_eq!(engine.fee_rate_bps, 10);
    }

    #[test]
    fn test_market_order_validation() {
        let engine = FillEngine::new(dec!(1), dec!(5), 10);

        let valid_order = MarketOrderRequest {
            token_id: "test".to_string(),
            side: Side::BUY,
            amount: dec!(100),
            slippage_tolerance: None,
            client_id: None,
        };
        assert!(engine.validate_market_order(&valid_order).is_ok());

        let invalid_order = MarketOrderRequest {
            token_id: "test".to_string(),
            side: Side::BUY,
            amount: dec!(0),
            slippage_tolerance: None,
            client_id: None,
        };
        assert!(engine.validate_market_order(&invalid_order).is_err());
    }

    #[test]
    fn test_fee_calculation() {
        let engine = FillEngine::new(dec!(1), dec!(5), 10);
        let fee = engine.calculate_fee(dec!(1000));
        assert_eq!(fee, dec!(1)); // 10 bps = 0.1% = 1 on 1000
    }

    #[test]
    fn test_fill_processor() {
        let mut processor = FillProcessor::new(100);

        let fill = FillEvent {
            id: "fill1".to_string(),
            order_id: "order1".to_string(),
            token_id: "test".to_string(),
            side: Side::BUY,
            price: dec!(0.5),
            size: dec!(100),
            timestamp: Utc::now(),
            maker_address: Address::ZERO,
            taker_address: Address::ZERO,
            fee: dec!(0.1),
        };

        assert!(processor.process_fill(fill).is_ok());
        assert_eq!(processor.pending_fills.len(), 1);
    }

    #[test]
    fn test_fill_engine_advanced_creation() {
        // Test that we can create a fill engine with parameters
        let _engine = FillEngine::new(dec!(1.0), dec!(0.05), 50); // min_fill_size, max_slippage, fee_rate_bps

        // Test basic properties exist (we can't access private fields directly)
        // But we can test that the engine was created successfully
        // Engine creation successful
    }

    #[test]
    fn test_fill_processor_basic_operations() {
        let mut processor = FillProcessor::new(100); // max_pending

        // Test that we can create a fill event and process it
        let fill_event = FillEvent {
            id: "fill_1".to_string(),
            order_id: "order_1".to_string(),
            side: Side::BUY,
            size: dec!(25),
            price: dec!(0.75),
            timestamp: chrono::Utc::now(),
            token_id: "token_1".to_string(),
            maker_address: alloy_primitives::Address::ZERO,
            taker_address: alloy_primitives::Address::ZERO,
            fee: dec!(0.01),
        };

        let result = processor.process_fill(fill_event);
        assert!(result.is_ok());

        // Check that the fill was added to pending
        assert_eq!(processor.pending_fills.len(), 1);
    }
}
