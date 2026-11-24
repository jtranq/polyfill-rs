//! Utility functions for the Polymarket client
//!
//! This module contains optimized utility functions for performance-critical
//! operations in trading environments.

use crate::errors::{PolyfillError, Result};
use ::url::Url;
use alloy_primitives::{Address, U256};
use base64::{engine::general_purpose::URL_SAFE, Engine};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::Sha256;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// High-precision timestamp utilities
pub mod time {
    use super::*;

    /// Get current Unix timestamp in seconds
    #[inline]
    pub fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
    }

    /// Get current Unix timestamp in milliseconds
    #[inline]
    pub fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64
    }

    /// Get current Unix timestamp in microseconds
    #[inline]
    pub fn now_micros() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_micros() as u64
    }

    /// Get current Unix timestamp in nanoseconds
    #[inline]
    pub fn now_nanos() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos()
    }

    /// Convert DateTime to Unix timestamp in seconds
    #[inline]
    pub fn datetime_to_secs(dt: DateTime<Utc>) -> u64 {
        dt.timestamp() as u64
    }

    /// Convert Unix timestamp to DateTime
    #[inline]
    pub fn secs_to_datetime(timestamp: u64) -> DateTime<Utc> {
        DateTime::from_timestamp(timestamp as i64, 0).unwrap_or_else(Utc::now)
    }
}

/// Cryptographic utilities for signing and authentication
pub mod crypto {
    use super::*;

    /// Build HMAC-SHA256 signature for API authentication
    pub fn build_hmac_signature<T>(
        secret: &str,
        timestamp: u64,
        method: &str,
        path: &str,
        body: Option<&T>,
    ) -> Result<String>
    where
        T: ?Sized + Serialize,
    {
        let decoded = URL_SAFE
            .decode(secret)
            .map_err(|e| PolyfillError::config(format!("Invalid secret format: {}", e)))?;

        let message = match body {
            None => format!("{timestamp}{method}{path}"),
            Some(data) => {
                let json = serde_json::to_string(data)?;
                format!("{timestamp}{method}{path}{json}")
            },
        };

        let mut mac = HmacSha256::new_from_slice(&decoded)
            .map_err(|e| PolyfillError::internal("HMAC initialization failed", e))?;

        mac.update(message.as_bytes());
        let result = mac.finalize();

        Ok(URL_SAFE.encode(result.into_bytes()))
    }

    /// Generate a secure random nonce
    pub fn generate_nonce() -> U256 {
        use rand::RngCore;
        let mut rng = rand::thread_rng();
        let mut bytes = [0u8; 32];
        rng.fill_bytes(&mut bytes);
        U256::from_be_bytes(bytes)
    }

    /// Generate a secure random salt
    pub fn generate_salt() -> u64 {
        use rand::RngCore;
        let mut rng = rand::thread_rng();
        rng.next_u64()
    }
}

/// Price and size calculation utilities
pub mod math {
    use super::*;
    use crate::types::{Price, Qty, SCALE_FACTOR};
    use rust_decimal::prelude::*;

    // ========================================================================
    // LEGACY DECIMAL FUNCTIONS (for backward compatibility)
    // ========================================================================
    //
    // These are kept for API compatibility, but internally we should use
    // the fixed-point versions below for better performance.

    /// Round price to tick size (LEGACY - use fixed-point version when possible)
    #[inline]
    pub fn round_to_tick(price: Decimal, tick_size: Decimal) -> Decimal {
        if tick_size.is_zero() {
            return price;
        }
        (price / tick_size).round() * tick_size
    }

    /// Calculate notional value (price * size) (LEGACY - use fixed-point version when possible)
    #[inline]
    pub fn notional(price: Decimal, size: Decimal) -> Decimal {
        price * size
    }

    /// Calculate spread as percentage (LEGACY - use fixed-point version when possible)
    #[inline]
    pub fn spread_pct(bid: Decimal, ask: Decimal) -> Option<Decimal> {
        if bid.is_zero() || ask <= bid {
            return None;
        }
        Some((ask - bid) / bid * Decimal::from(100))
    }

    /// Calculate mid price (LEGACY - use fixed-point version when possible)
    #[inline]
    pub fn mid_price(bid: Decimal, ask: Decimal) -> Option<Decimal> {
        if bid.is_zero() || ask.is_zero() || ask <= bid {
            return None;
        }
        Some((bid + ask) / Decimal::from(2))
    }

    // ========================================================================
    // HIGH-PERFORMANCE FIXED-POINT FUNCTIONS
    // ========================================================================
    //
    // These functions operate on our internal Price/Qty types and are
    // optimized for maximum performance. They avoid all Decimal operations
    // and memory allocations.
    //
    // Performance comparison (approximate):
    // - Decimal operations: 20-100ns + allocation overhead
    // - Fixed-point operations: 1-5ns, no allocations
    //
    // That's a 10-50x speedup on the critical path!

    /// Round price to tick size (FAST VERSION)
    ///
    /// This is much faster than the Decimal version because it's just
    /// integer division and multiplication.
    ///
    /// Example: round_to_tick_fast(6543, 10) = 6540 (rounds to nearest 10 ticks)
    #[inline]
    pub fn round_to_tick_fast(price_ticks: Price, tick_size_ticks: Price) -> Price {
        if tick_size_ticks == 0 {
            return price_ticks;
        }
        // Integer division automatically truncates, then multiply back
        // For proper rounding, we add half the tick size before dividing
        let half_tick = tick_size_ticks / 2;
        ((price_ticks + half_tick) / tick_size_ticks) * tick_size_ticks
    }

    /// Calculate notional value (price * size) (FAST VERSION)
    ///
    /// Returns the result in the same scale as our quantities.
    /// This avoids the expensive Decimal multiplication.
    ///
    /// Example: notional_fast(6543, 1000000) = 6543000000 (representing $654.30)
    #[inline]
    pub fn notional_fast(price_ticks: Price, size_units: Qty) -> i64 {
        // Convert price to i64 to avoid overflow
        let price_i64 = price_ticks as i64;
        // Multiply and scale appropriately
        // Both price and size are scaled by SCALE_FACTOR, so result is scaled by SCALE_FACTOR^2
        // We divide by SCALE_FACTOR to get back to normal scale
        (price_i64 * size_units) / SCALE_FACTOR
    }

    /// Calculate spread as percentage (FAST VERSION)
    ///
    /// Returns the spread as a percentage in basis points (1/100th of a percent).
    /// This avoids floating-point arithmetic entirely.
    ///
    /// Example: spread_pct_fast(6500, 6700) = Some(307) (representing 3.07%)
    #[inline]
    pub fn spread_pct_fast(bid_ticks: Price, ask_ticks: Price) -> Option<u32> {
        if bid_ticks == 0 || ask_ticks <= bid_ticks {
            return None;
        }

        let spread = ask_ticks - bid_ticks;
        // Calculate percentage in basis points (multiply by 10000 for 4 decimal places)
        // We use u64 for intermediate calculation to avoid overflow
        let spread_bps = ((spread as u64) * 10000) / (bid_ticks as u64);

        // Convert back to u32 (should always fit since spreads are typically small)
        Some(spread_bps as u32)
    }

    /// Calculate mid price (FAST VERSION)
    ///
    /// Returns the midpoint between bid and ask in ticks.
    /// Much faster than the Decimal version.
    ///
    /// Example: mid_price_fast(6500, 6700) = Some(6600)
    #[inline]
    pub fn mid_price_fast(bid_ticks: Price, ask_ticks: Price) -> Option<Price> {
        if bid_ticks == 0 || ask_ticks == 0 || ask_ticks <= bid_ticks {
            return None;
        }

        // Use u64 to avoid overflow in addition
        let sum = (bid_ticks as u64) + (ask_ticks as u64);
        Some((sum / 2) as Price)
    }

    /// Calculate spread in ticks (FAST VERSION)
    ///
    /// Simple subtraction - much faster than Decimal operations.
    ///
    /// Example: spread_fast(6500, 6700) = Some(200) (representing $0.02 spread)
    #[inline]
    pub fn spread_fast(bid_ticks: Price, ask_ticks: Price) -> Option<Price> {
        if ask_ticks <= bid_ticks {
            return None;
        }
        Some(ask_ticks - bid_ticks)
    }

    /// Check if price is within valid range (FAST VERSION)
    ///
    /// Much faster than converting to Decimal and back.
    ///
    /// Example: is_valid_price_fast(6543, 1, 10000) = true
    #[inline]
    pub fn is_valid_price_fast(price_ticks: Price, min_tick: Price, max_tick: Price) -> bool {
        price_ticks >= min_tick && price_ticks <= max_tick
    }

    /// Convert decimal to token units (6 decimal places)
    #[inline]
    pub fn decimal_to_token_units(amount: Decimal) -> u64 {
        let scaled = amount * Decimal::from(1_000_000);
        scaled.to_u64().unwrap_or(0)
    }

    /// Convert token units back to decimal
    #[inline]
    pub fn token_units_to_decimal(units: u64) -> Decimal {
        Decimal::from(units) / Decimal::from(1_000_000)
    }

    /// Check if price is within valid range [tick_size, 1-tick_size]
    #[inline]
    pub fn is_valid_price(price: Decimal, tick_size: Decimal) -> bool {
        price >= tick_size && price <= (Decimal::ONE - tick_size)
    }

    /// Calculate adverse slippage as a fraction, where 0.02 means 2%.
    pub fn calculate_slippage(
        target_price: Decimal,
        executed_price: Decimal,
        side: crate::types::Side,
    ) -> Decimal {
        match side {
            crate::types::Side::BUY => {
                if executed_price > target_price {
                    (executed_price - target_price) / target_price
                } else {
                    Decimal::ZERO
                }
            },
            crate::types::Side::SELL => {
                if executed_price < target_price {
                    (target_price - executed_price) / target_price
                } else {
                    Decimal::ZERO
                }
            },
        }
    }
}

/// Network and retry utilities
pub mod retry {
    use super::*;
    use std::future::Future;
    use tokio::time::{sleep, Duration};

    /// Exponential backoff configuration
    #[derive(Debug, Clone)]
    pub struct RetryConfig {
        pub max_attempts: usize,
        pub initial_delay: Duration,
        pub max_delay: Duration,
        pub backoff_factor: f64,
        pub jitter: bool,
    }

    impl Default for RetryConfig {
        fn default() -> Self {
            Self {
                max_attempts: 3,
                initial_delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(10),
                backoff_factor: 2.0,
                jitter: true,
            }
        }
    }

    /// Retry a future with exponential backoff
    pub async fn with_retry<F, Fut, T>(config: &RetryConfig, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut delay = config.initial_delay;
        let mut last_error = None;

        for attempt in 0..config.max_attempts {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    last_error = Some(err.clone());

                    if !err.is_retryable() || attempt == config.max_attempts - 1 {
                        return Err(err);
                    }

                    // Add jitter if enabled
                    let actual_delay = if config.jitter {
                        let jitter_factor = rand::random::<f64>() * 0.1; // ±10%
                        let jitter = 1.0 + (jitter_factor - 0.05);
                        Duration::from_nanos((delay.as_nanos() as f64 * jitter) as u64)
                    } else {
                        delay
                    };

                    sleep(actual_delay).await;

                    // Exponential backoff
                    delay = std::cmp::min(
                        Duration::from_nanos(
                            (delay.as_nanos() as f64 * config.backoff_factor) as u64,
                        ),
                        config.max_delay,
                    );
                },
            }
        }

        Err(last_error.unwrap_or_else(|| {
            PolyfillError::internal(
                "Retry loop failed",
                std::io::Error::other("No error captured"),
            )
        }))
    }
}

/// Address and token ID utilities
pub mod address {
    use super::*;

    /// Validate and parse Ethereum address
    pub fn parse_address(addr: &str) -> Result<Address> {
        Address::from_str(addr)
            .map_err(|e| PolyfillError::validation(format!("Invalid address format: {}", e)))
    }

    /// Validate token ID format
    pub fn validate_token_id(token_id: &str) -> Result<()> {
        if token_id.is_empty() {
            return Err(PolyfillError::validation("Token ID cannot be empty"));
        }

        // Token IDs should be numeric strings
        if !token_id.chars().all(|c| c.is_ascii_digit()) {
            return Err(PolyfillError::validation("Token ID must be numeric"));
        }

        Ok(())
    }

    /// Convert token ID to U256
    pub fn token_id_to_u256(token_id: &str) -> Result<U256> {
        validate_token_id(token_id)?;
        U256::from_str_radix(token_id, 10)
            .map_err(|e| PolyfillError::validation(format!("Invalid token ID: {}", e)))
    }
}

/// URL building utilities
pub mod url {
    use super::*;

    /// Build API endpoint URL
    pub fn build_endpoint(base_url: &str, path: &str) -> Result<String> {
        let base = base_url.trim_end_matches('/');
        let path = path.trim_start_matches('/');
        Ok(format!("{}/{}", base, path))
    }

    /// Add query parameters to URL
    pub fn add_query_params(mut url: url::Url, params: &[(&str, &str)]) -> url::Url {
        {
            let mut query_pairs = url.query_pairs_mut();
            for (key, value) in params {
                query_pairs.append_pair(key, value);
            }
        }
        url
    }
}

/// Rate limiting utilities
pub mod rate_limit {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Simple token bucket rate limiter
    #[derive(Debug)]
    pub struct TokenBucket {
        capacity: usize,
        tokens: Arc<Mutex<usize>>,
        refill_rate: Duration,
        last_refill: Arc<Mutex<SystemTime>>,
    }

    impl TokenBucket {
        pub fn new(capacity: usize, refill_per_second: usize) -> Self {
            Self {
                capacity,
                tokens: Arc::new(Mutex::new(capacity)),
                refill_rate: Duration::from_secs(1) / refill_per_second as u32,
                last_refill: Arc::new(Mutex::new(SystemTime::now())),
            }
        }

        /// Try to consume a token, return true if successful
        pub fn try_consume(&self) -> bool {
            self.refill();

            let mut tokens = self.tokens.lock().unwrap();
            if *tokens > 0 {
                *tokens -= 1;
                true
            } else {
                false
            }
        }

        fn refill(&self) {
            let now = SystemTime::now();
            let mut last_refill = self.last_refill.lock().unwrap();
            let elapsed = now.duration_since(*last_refill).unwrap_or_default();

            if elapsed >= self.refill_rate {
                let tokens_to_add = elapsed.as_nanos() / self.refill_rate.as_nanos();
                let mut tokens = self.tokens.lock().unwrap();
                *tokens = std::cmp::min(self.capacity, *tokens + tokens_to_add as usize);
                *last_refill = now;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_to_tick() {
        use math::round_to_tick;

        let price = Decimal::from_str("0.567").unwrap();
        let tick = Decimal::from_str("0.01").unwrap();
        let rounded = round_to_tick(price, tick);
        assert_eq!(rounded, Decimal::from_str("0.57").unwrap());
    }

    #[test]
    fn test_mid_price() {
        use math::mid_price;

        let bid = Decimal::from_str("0.50").unwrap();
        let ask = Decimal::from_str("0.52").unwrap();
        let mid = mid_price(bid, ask).unwrap();
        assert_eq!(mid, Decimal::from_str("0.51").unwrap());
    }

    #[test]
    fn test_token_units_conversion() {
        use math::{decimal_to_token_units, token_units_to_decimal};

        let amount = Decimal::from_str("1.234567").unwrap();
        let units = decimal_to_token_units(amount);
        assert_eq!(units, 1_234_567);

        let back = token_units_to_decimal(units);
        assert_eq!(back, amount);
    }

    #[test]
    fn test_address_validation() {
        use address::parse_address;

        let valid = "0x1234567890123456789012345678901234567890";
        assert!(parse_address(valid).is_ok());

        let invalid = "invalid_address";
        assert!(parse_address(invalid).is_err());
    }
}
