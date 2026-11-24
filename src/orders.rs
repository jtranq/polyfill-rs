//! Order creation and signing functionality
//!
//! This module handles the complex process of creating and signing orders
//! for the Polymarket CLOB, including EIP-712 signature generation.

use crate::auth::{
    sign_order_message, sign_order_message_with_domain, sign_poly1271_order_message_with_domain,
    PreparedOrderDomain, SignedOrderMessage,
};
use crate::errors::{PolyfillError, Result};
use crate::types::{
    CreateOrderOptions, MarketOrderArgs, OrderArgs, OrderType, Side, SignedOrderRequest,
    SCALE_FACTOR,
};
use alloy_primitives::{keccak256, Address, B256, U256};
use alloy_signer_local::PrivateKeySigner;
use rand::Rng;
use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy::{AwayFromZero, MidpointTowardZero, ToZero};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

pub const BYTES32_ZERO: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

/// Signature types for orders
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SigType {
    /// ECDSA EIP712 signatures signed by EOAs
    Eoa = 0,
    /// EIP712 signatures signed by EOAs that own Polymarket Proxy wallets
    PolyProxy = 1,
    /// EIP712 signatures signed by EOAs that own Polymarket Gnosis safes
    PolyGnosisSafe = 2,
    /// EIP-1271 smart contract wallet signatures (V2 orders only)
    Poly1271 = 3,
}

/// Rounding configuration for different tick sizes
#[derive(Clone, Copy)]
pub struct RoundConfig {
    price: u32,
    size: u32,
    amount: u32,
}

/// Contract configuration
pub struct ContractConfig {
    pub exchange: String,
    pub collateral: String,
    pub conditional_tokens: String,
}

/// Order builder for creating and signing orders
#[derive(Clone)]
pub struct OrderBuilder {
    signer: PrivateKeySigner,
    signer_address: Address,
    signer_checksum: String,
    sig_type: SigType,
    funder: Address,
    funder_checksum: String,
}

/// Prepared low-latency order path for a single market/token configuration.
#[derive(Clone)]
pub struct PreparedOrderPath {
    builder: OrderBuilder,
    chain_id: u64,
    token_id: String,
    token_id_u256: U256,
    round_config: RoundConfig,
    domain: PreparedOrderDomain,
    builder_bytes: B256,
    builder_code: String,
    metadata_bytes: B256,
    metadata: String,
}

const POLYGON_PROXY_FACTORY: &str = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052";
const POLYGON_SAFE_FACTORY: &str = "0xaacFeEa03eb1561C4e67d661e40682Bd20E3541b";
const PROXY_INIT_CODE_HASH: &str =
    "0xd21df8dc65880a8606f09fe0ce3df9b8869287ab0b058be05aa9e8af6330a00b";
const SAFE_INIT_CODE_HASH: &str =
    "0x2bce2127ff07fb632d16c8347c4ebf501f4841168bed00d9e6ef715ddb6fcecf";

const ROUND_CONFIG_0_1: RoundConfig = RoundConfig {
    price: 1,
    size: 2,
    amount: 3,
};
const ROUND_CONFIG_0_01: RoundConfig = RoundConfig {
    price: 2,
    size: 2,
    amount: 4,
};
const ROUND_CONFIG_0_001: RoundConfig = RoundConfig {
    price: 3,
    size: 2,
    amount: 5,
};
const ROUND_CONFIG_0_0001: RoundConfig = RoundConfig {
    price: 4,
    size: 2,
    amount: 6,
};
const TOKEN_UNIT_SCALE: Decimal = Decimal::from_parts(1_000_000, 0, 0, false, 0);

/// Get contract configuration for chain
pub fn get_contract_config(chain_id: u64, neg_risk: bool) -> Option<ContractConfig> {
    match (chain_id, neg_risk) {
        (137, false) => Some(ContractConfig {
            exchange: "0xE111180000d2663C0091e4f400237545B87B996B".to_string(),
            collateral: "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB".to_string(),
            conditional_tokens: "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045".to_string(),
        }),
        (137, true) => Some(ContractConfig {
            exchange: "0xe2222d279d744050d28e00520010520000310F59".to_string(),
            collateral: "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB".to_string(),
            conditional_tokens: "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045".to_string(),
        }),
        _ => None,
    }
}

fn exchange_address_for(chain_id: u64, neg_risk: bool) -> Result<Address> {
    let contract_config = get_contract_config(chain_id, neg_risk).ok_or_else(|| {
        PolyfillError::config("No contract found with given chain_id and neg_risk")
    })?;

    Address::from_str(&contract_config.exchange)
        .map_err(|e| PolyfillError::config(format!("Invalid exchange address: {}", e)))
}

fn parse_token_id(token_id: &str) -> Result<U256> {
    U256::from_str_radix(token_id, 10)
        .map_err(|e| PolyfillError::validation(format!("Incorrect tokenId format: {}", e)))
}

fn parse_optional_bytes32(name: &str, value: Option<&str>) -> Result<(B256, String)> {
    let normalized = normalize_optional_bytes32(name, value)?;
    let parsed = B256::from_str(&normalized)
        .map_err(|e| PolyfillError::validation(format!("Invalid {name} bytes32 value: {e}")))?;

    Ok((parsed, normalized))
}

pub fn sig_type_from_u8(signature_type: u8) -> Result<SigType> {
    match signature_type {
        0 => Ok(SigType::Eoa),
        1 => Ok(SigType::PolyProxy),
        2 => Ok(SigType::PolyGnosisSafe),
        3 => Ok(SigType::Poly1271),
        other => Err(PolyfillError::validation(format!(
            "Unsupported signature_type {other}"
        ))),
    }
}

pub fn derive_proxy_wallet(eoa_address: Address, chain_id: u64) -> Result<Address> {
    if chain_id != 137 {
        return Err(PolyfillError::config(
            "Proxy wallet auto-derivation is only configured for Polygon mainnet",
        ));
    }

    let factory = Address::from_str(POLYGON_PROXY_FACTORY)
        .map_err(|e| PolyfillError::config(format!("Invalid proxy factory address: {e}")))?;
    let init_code_hash = B256::from_str(PROXY_INIT_CODE_HASH)
        .map_err(|e| PolyfillError::config(format!("Invalid proxy init code hash: {e}")))?;
    let salt = keccak256(eoa_address);
    Ok(factory.create2(salt, init_code_hash))
}

pub fn derive_safe_wallet(eoa_address: Address, chain_id: u64) -> Result<Address> {
    if chain_id != 137 {
        return Err(PolyfillError::config(
            "Safe wallet auto-derivation is only configured for Polygon mainnet",
        ));
    }

    let factory = Address::from_str(POLYGON_SAFE_FACTORY)
        .map_err(|e| PolyfillError::config(format!("Invalid safe factory address: {e}")))?;
    let init_code_hash = B256::from_str(SAFE_INIT_CODE_HASH)
        .map_err(|e| PolyfillError::config(format!("Invalid safe init code hash: {e}")))?;
    let mut padded = [0_u8; 32];
    padded[12..].copy_from_slice(eoa_address.as_slice());
    let salt = keccak256(padded);
    Ok(factory.create2(salt, init_code_hash))
}

pub fn resolve_funder(
    signer_address: Address,
    chain_id: u64,
    sig_type: SigType,
    funder: Option<Address>,
) -> Result<Option<Address>> {
    match (sig_type, funder) {
        (SigType::Eoa, Some(_)) => Err(PolyfillError::validation(
            "funder cannot be set for EOA signature_type",
        )),
        (SigType::PolyProxy, None) => derive_proxy_wallet(signer_address, chain_id).map(Some),
        (SigType::PolyGnosisSafe, None) => derive_safe_wallet(signer_address, chain_id).map(Some),
        (SigType::Poly1271, None) => Err(PolyfillError::validation(
            "funder is required for Poly1271 signature_type",
        )),
        (_, Some(Address::ZERO)) => Err(PolyfillError::validation("funder cannot be zero address")),
        (_, explicit) => Ok(explicit),
    }
}

/// Generate a random seed for order salt
fn generate_seed() -> u64 {
    let mut rng = rand::thread_rng();
    let y: f64 = rng.gen();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();
    (timestamp as f64 * y) as u64
}

/// Convert decimal to token units (multiply by 1e6)
fn decimal_to_token_units(amt: Decimal) -> Result<U256> {
    let mut amt = TOKEN_UNIT_SCALE * amt;
    if amt.scale() > 0 {
        amt = amt.round_dp_with_strategy(0, MidpointTowardZero);
    }
    let units: u128 = amt
        .try_into()
        .map_err(|_| PolyfillError::validation(format!("Invalid token amount {amt}")))?;
    Ok(U256::from(units))
}

fn parse_round_config(tick_size: Decimal) -> Result<&'static RoundConfig> {
    let scaled = tick_size * Decimal::from(SCALE_FACTOR);
    if !scaled.is_integer() {
        return Err(PolyfillError::validation(format!(
            "Unsupported tick size {tick_size}"
        )));
    }

    let tick_size_ticks: u32 = scaled
        .try_into()
        .map_err(|_| PolyfillError::validation(format!("Unsupported tick size {tick_size}")))?;

    match tick_size_ticks {
        1000 => Ok(&ROUND_CONFIG_0_1),
        100 => Ok(&ROUND_CONFIG_0_01),
        10 => Ok(&ROUND_CONFIG_0_001),
        1 => Ok(&ROUND_CONFIG_0_0001),
        _ => Err(PolyfillError::validation(format!(
            "Unsupported tick size {tick_size}"
        ))),
    }
}

pub(crate) fn validate_bytes32_hex(field: &str, value: &str) -> Result<()> {
    if value == BYTES32_ZERO {
        return Ok(());
    }

    if !value.starts_with("0x") {
        return Err(PolyfillError::validation(format!(
            "{field} must be a 0x-prefixed 32-byte hex string"
        )));
    }

    if value.len() != 66 {
        return Err(PolyfillError::validation(format!(
            "{field} must be exactly 32 bytes (64 hex chars)"
        )));
    }

    if !value
        .as_bytes()
        .iter()
        .skip(2)
        .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(PolyfillError::validation(format!(
            "{field} must contain only hexadecimal characters"
        )));
    }

    Ok(())
}

fn normalize_optional_bytes32(field: &str, value: Option<&str>) -> Result<String> {
    let value = value.unwrap_or(BYTES32_ZERO);
    validate_bytes32_hex(field, value)?;
    Ok(value.to_string())
}

pub fn adjust_buy_amount_for_fees(
    amount: Decimal,
    price: Decimal,
    user_usdc_balance: Decimal,
    fee_rate: Decimal,
    fee_exponent: u32,
    builder_taker_fee_rate: Decimal,
) -> Result<Decimal> {
    if price <= Decimal::ZERO {
        return Err(PolyfillError::validation(
            "Market buy fee adjustment requires a positive price",
        ));
    }

    let base = price * (Decimal::ONE - price);
    let base_f64: f64 = base
        .try_into()
        .map_err(|_| PolyfillError::validation(format!("Invalid fee base {base}")))?;
    let exp_f64: f64 = Decimal::from(fee_exponent)
        .try_into()
        .map_err(|_| PolyfillError::validation(format!("Invalid fee exponent {fee_exponent}")))?;
    let platform_fee_rate = fee_rate
        * Decimal::try_from(base_f64.powf(exp_f64)).map_err(|_| {
            PolyfillError::validation(format!(
                "Invalid platform fee rate for price {price} and exponent {fee_exponent}"
            ))
        })?;

    let platform_fee = amount / price * platform_fee_rate;
    let total_cost = amount + platform_fee + amount * builder_taker_fee_rate;

    let raw = if user_usdc_balance <= total_cost {
        let divisor = Decimal::ONE + platform_fee_rate / price + builder_taker_fee_rate;
        user_usdc_balance / divisor
    } else {
        amount
    };

    let adjusted = raw.trunc_with_scale(6);
    if adjusted.is_zero() {
        return Err(PolyfillError::validation(format!(
            "user_usdc_balance {user_usdc_balance} too small to cover fees at price {price}; \
             fee-adjusted amount truncated to zero"
        )));
    }

    Ok(adjusted)
}

impl OrderBuilder {
    /// Create a new order builder
    pub fn new(
        signer: PrivateKeySigner,
        sig_type: Option<SigType>,
        funder: Option<Address>,
    ) -> Self {
        let sig_type = sig_type.unwrap_or(SigType::Eoa);
        let signer_address = signer.address();
        let signer_checksum = signer_address.to_checksum(None);
        let funder = funder.unwrap_or(signer_address);
        let funder_checksum = funder.to_checksum(None);

        OrderBuilder {
            signer,
            signer_address,
            signer_checksum,
            sig_type,
            funder,
            funder_checksum,
        }
    }

    /// Get signature type as u8
    pub fn get_sig_type(&self) -> u8 {
        self.sig_type as u8
    }

    /// Prepare reusable order-path state for one market/token.
    ///
    /// This caches tick-size rounding, exchange address parsing, token ID parsing, normalized
    /// bytes32 fields, and the EIP-712 domain. Use this for latency-sensitive repeated order
    /// creation on the same token.
    pub fn prepare_order_path(
        &self,
        chain_id: u64,
        token_id: impl Into<String>,
        tick_size: Decimal,
        neg_risk: bool,
        builder_code: Option<&str>,
        metadata: Option<&str>,
    ) -> Result<PreparedOrderPath> {
        let token_id = token_id.into();
        let token_id_u256 = parse_token_id(&token_id)?;
        let round_config = *parse_round_config(tick_size)?;
        let exchange = exchange_address_for(chain_id, neg_risk)?;
        let domain = PreparedOrderDomain::new(chain_id, exchange);
        let (builder_bytes, builder_code) = parse_optional_bytes32("builder_code", builder_code)?;
        let (metadata_bytes, metadata) = parse_optional_bytes32("metadata", metadata)?;

        Ok(PreparedOrderPath {
            builder: self.clone(),
            chain_id,
            token_id,
            token_id_u256,
            round_config,
            domain,
            builder_bytes,
            builder_code,
            metadata_bytes,
            metadata,
        })
    }

    /// Fix amount rounding according to configuration
    fn fix_amount_rounding(&self, mut amt: Decimal, round_config: &RoundConfig) -> Decimal {
        if amt.scale() > round_config.amount {
            amt = amt.round_dp_with_strategy(round_config.amount + 4, AwayFromZero);
            if amt.scale() > round_config.amount {
                amt = amt.round_dp_with_strategy(round_config.amount, ToZero);
            }
        }
        amt
    }

    /// Get order amounts (maker and taker) for a regular order
    fn get_order_amounts(
        &self,
        side: Side,
        size: Decimal,
        price: Decimal,
        round_config: &RoundConfig,
    ) -> Result<(U256, U256)> {
        let raw_price = price.round_dp_with_strategy(round_config.price, MidpointTowardZero);

        let amounts = match side {
            Side::BUY => {
                let raw_taker_amt = size.round_dp_with_strategy(round_config.size, ToZero);
                let raw_maker_amt = raw_taker_amt * raw_price;
                let raw_maker_amt = self.fix_amount_rounding(raw_maker_amt, round_config);
                (
                    decimal_to_token_units(raw_maker_amt)?,
                    decimal_to_token_units(raw_taker_amt)?,
                )
            },
            Side::SELL => {
                let raw_maker_amt = size.round_dp_with_strategy(round_config.size, ToZero);
                let raw_taker_amt = raw_maker_amt * raw_price;
                let raw_taker_amt = self.fix_amount_rounding(raw_taker_amt, round_config);

                (
                    decimal_to_token_units(raw_maker_amt)?,
                    decimal_to_token_units(raw_taker_amt)?,
                )
            },
        };

        Ok(amounts)
    }

    /// Get order amounts for a market order
    fn get_market_order_amounts(
        &self,
        side: Side,
        amount: Decimal,
        price: Decimal,
        round_config: &RoundConfig,
    ) -> Result<(U256, U256)> {
        let raw_price = price.round_dp_with_strategy(round_config.price, MidpointTowardZero);

        let amounts = match side {
            Side::BUY => {
                let raw_maker_amt = amount.round_dp_with_strategy(round_config.size, ToZero);
                let raw_taker_amt =
                    self.fix_amount_rounding(raw_maker_amt / raw_price, round_config);

                (
                    decimal_to_token_units(raw_maker_amt)?,
                    decimal_to_token_units(raw_taker_amt)?,
                )
            },
            Side::SELL => {
                let raw_maker_amt = amount.round_dp_with_strategy(round_config.size, ToZero);
                let raw_taker_amt =
                    self.fix_amount_rounding(raw_maker_amt * raw_price, round_config);

                (
                    decimal_to_token_units(raw_maker_amt)?,
                    decimal_to_token_units(raw_taker_amt)?,
                )
            },
        };

        Ok(amounts)
    }

    /// Calculate market price from REST order book levels in worst-first order.
    /// Bids must be ascending and asks descending.
    pub fn calculate_market_price(
        &self,
        positions: &[crate::types::BookLevel],
        amount_to_match: Decimal,
        side: Side,
        order_type: OrderType,
    ) -> Result<Decimal> {
        let mut sum = Decimal::ZERO;
        let mut last_price = None;

        for level in positions.iter().rev() {
            sum += match side {
                Side::BUY => level.size * level.price,
                Side::SELL => level.size,
            };
            last_price = Some(level.price);
            if sum >= amount_to_match {
                return Ok(level.price);
            }
        }

        match (order_type, last_price) {
            (OrderType::FAK, Some(price)) => Ok(price),
            _ => Err(PolyfillError::order(
                format!(
                    "Not enough liquidity to create market order with amount {}",
                    amount_to_match
                ),
                crate::errors::OrderErrorKind::InsufficientBalance,
            )),
        }
    }

    /// Create a market order
    pub fn create_market_order(
        &self,
        chain_id: u64,
        order_args: &MarketOrderArgs,
        price: Decimal,
        options: &CreateOrderOptions,
    ) -> Result<SignedOrderRequest> {
        if !matches!(order_args.order_type, OrderType::FOK | OrderType::FAK) {
            return Err(PolyfillError::validation(
                "Market orders only support FOK and FAK order types",
            ));
        }

        let tick_size = options
            .tick_size
            .ok_or_else(|| PolyfillError::validation("Cannot create order without tick size"))?;
        let round_config = parse_round_config(tick_size)?;

        let (maker_amount, taker_amount) =
            self.get_market_order_amounts(order_args.side, order_args.amount, price, round_config)?;

        let neg_risk = options
            .neg_risk
            .ok_or_else(|| PolyfillError::validation("Cannot create order without neg_risk"))?;

        let exchange_address = exchange_address_for(chain_id, neg_risk)?;

        self.build_signed_order(
            order_args.token_id.clone(),
            order_args.side,
            chain_id,
            exchange_address,
            maker_amount,
            taker_amount,
            0,
            order_args.builder_code.as_deref(),
            order_args.metadata.as_deref(),
        )
    }

    /// Create a regular order
    pub fn create_order(
        &self,
        chain_id: u64,
        order_args: &OrderArgs,
        options: &CreateOrderOptions,
    ) -> Result<SignedOrderRequest> {
        let tick_size = options
            .tick_size
            .ok_or_else(|| PolyfillError::validation("Cannot create order without tick size"))?;
        let round_config = parse_round_config(tick_size)?;

        let (maker_amount, taker_amount) = self.get_order_amounts(
            order_args.side,
            order_args.size,
            order_args.price,
            round_config,
        )?;

        let neg_risk = options
            .neg_risk
            .ok_or_else(|| PolyfillError::validation("Cannot create order without neg_risk"))?;

        let exchange_address = exchange_address_for(chain_id, neg_risk)?;

        self.build_signed_order(
            order_args.token_id.clone(),
            order_args.side,
            chain_id,
            exchange_address,
            maker_amount,
            taker_amount,
            order_args.expiration.unwrap_or(0),
            order_args.builder_code.as_deref(),
            order_args.metadata.as_deref(),
        )
    }

    /// Build and sign an order
    #[allow(clippy::too_many_arguments)]
    fn build_signed_order(
        &self,
        token_id: String,
        side: Side,
        chain_id: u64,
        exchange: Address,
        maker_amount: U256,
        taker_amount: U256,
        expiration: u64,
        builder_code: Option<&str>,
        metadata: Option<&str>,
    ) -> Result<SignedOrderRequest> {
        let seed = generate_seed();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let u256_token_id = parse_token_id(&token_id)?;
        let (builder_bytes, builder) = parse_optional_bytes32("builder_code", builder_code)?;
        let (metadata_bytes, metadata) = parse_optional_bytes32("metadata", metadata)?;

        let signer_address = self.order_signer_address();
        let signer_checksum = self.order_signer_checksum();
        let order = SignedOrderMessage {
            salt: U256::from(seed),
            maker: self.funder,
            signer: signer_address,
            token_id: u256_token_id,
            maker_amount,
            taker_amount,
            side: side as u8,
            signature_type: self.sig_type as u8,
            timestamp: U256::from(timestamp),
            metadata: metadata_bytes,
            builder: builder_bytes,
        };

        let signature = match self.sig_type {
            SigType::Poly1271 => sign_poly1271_order_message_with_domain(
                &self.signer,
                order,
                &PreparedOrderDomain::new(chain_id, exchange),
                self.funder,
                chain_id,
            )?,
            _ => sign_order_message(&self.signer, order, chain_id, exchange)?,
        };

        Ok(SignedOrderRequest {
            salt: seed,
            maker: self.funder_checksum.clone(),
            signer: signer_checksum,
            token_id,
            maker_amount: maker_amount.to_string(),
            taker_amount: taker_amount.to_string(),
            expiration: expiration.to_string(),
            side: side.as_str().to_string(),
            signature_type: self.sig_type as u8,
            timestamp: timestamp.to_string(),
            metadata,
            builder,
            signature,
        })
    }
}

impl OrderBuilder {
    fn order_signer_address(&self) -> Address {
        match self.sig_type {
            SigType::Poly1271 => self.funder,
            _ => self.signer_address,
        }
    }

    fn order_signer_checksum(&self) -> String {
        match self.sig_type {
            SigType::Poly1271 => self.funder_checksum.clone(),
            _ => self.signer_checksum.clone(),
        }
    }
}

impl PreparedOrderPath {
    /// Create and sign a limit order using the cached market/token context.
    pub fn create_limit_order(
        &self,
        side: Side,
        price: Decimal,
        size: Decimal,
        expiration: Option<u64>,
    ) -> Result<SignedOrderRequest> {
        let (maker_amount, taker_amount) =
            self.builder
                .get_order_amounts(side, size, price, &self.round_config)?;

        self.build_signed_order(side, maker_amount, taker_amount, expiration.unwrap_or(0))
    }

    /// Create and sign a market order using the cached market/token context.
    pub fn create_market_order(
        &self,
        side: Side,
        amount: Decimal,
        price: Decimal,
        order_type: OrderType,
    ) -> Result<SignedOrderRequest> {
        if !matches!(order_type, OrderType::FOK | OrderType::FAK) {
            return Err(PolyfillError::validation(
                "Market orders only support FOK and FAK order types",
            ));
        }

        let (maker_amount, taker_amount) =
            self.builder
                .get_market_order_amounts(side, amount, price, &self.round_config)?;

        self.build_signed_order(side, maker_amount, taker_amount, 0)
    }

    fn build_signed_order(
        &self,
        side: Side,
        maker_amount: U256,
        taker_amount: U256,
        expiration: u64,
    ) -> Result<SignedOrderRequest> {
        let seed = generate_seed();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let signer_address = self.builder.order_signer_address();
        let signer_checksum = self.builder.order_signer_checksum();
        let order = SignedOrderMessage {
            salt: U256::from(seed),
            maker: self.builder.funder,
            signer: signer_address,
            token_id: self.token_id_u256,
            maker_amount,
            taker_amount,
            side: side as u8,
            signature_type: self.builder.sig_type as u8,
            timestamp: U256::from(timestamp),
            metadata: self.metadata_bytes,
            builder: self.builder_bytes,
        };

        let signature = match self.builder.sig_type {
            SigType::Poly1271 => sign_poly1271_order_message_with_domain(
                &self.builder.signer,
                order,
                &self.domain,
                self.builder.funder,
                self.chain_id,
            )?,
            _ => sign_order_message_with_domain(&self.builder.signer, order, &self.domain)?,
        };

        Ok(SignedOrderRequest {
            salt: seed,
            maker: self.builder.funder_checksum.clone(),
            signer: signer_checksum,
            token_id: self.token_id.clone(),
            maker_amount: maker_amount.to_string(),
            taker_amount: taker_amount.to_string(),
            expiration: expiration.to_string(),
            side: side.as_str().to_string(),
            signature_type: self.builder.sig_type as u8,
            timestamp: timestamp.to_string(),
            metadata: self.metadata.clone(),
            builder: self.builder_code.clone(),
            signature,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_signer_local::PrivateKeySigner;
    use serde_json::Value;

    fn test_builder() -> OrderBuilder {
        let signer: PrivateKeySigner =
            "0x1234567890123456789012345678901234567890123456789012345678901234"
                .parse()
                .expect("valid private key");
        OrderBuilder::new(signer, None, None)
    }

    #[test]
    fn test_decimal_to_token_units() {
        let result = decimal_to_token_units(Decimal::from_str("1.5").unwrap()).unwrap();
        assert_eq!(result, U256::from(1_500_000));
    }

    #[test]
    fn test_generate_seed() {
        let seed1 = generate_seed();
        let seed2 = generate_seed();
        assert_ne!(seed1, seed2);
    }

    #[test]
    fn test_decimal_to_token_units_edge_cases() {
        // Test zero
        let result = decimal_to_token_units(Decimal::ZERO).unwrap();
        assert_eq!(result, U256::ZERO);

        // Test small decimal
        let result = decimal_to_token_units(Decimal::from_str("0.000001").unwrap()).unwrap();
        assert_eq!(result, U256::from(1));

        // Test large number
        let result = decimal_to_token_units(Decimal::from_str("1000.0").unwrap()).unwrap();
        assert_eq!(result, U256::from(1_000_000_000));
    }

    #[test]
    fn test_decimal_to_token_units_supports_amounts_above_u32() {
        let result = decimal_to_token_units(Decimal::from_str("5000").unwrap()).unwrap();
        assert_eq!(result, U256::from(5_000_000_000_u64));
    }

    #[test]
    fn test_decimal_to_token_units_rejects_negative_amounts() {
        let result = decimal_to_token_units(Decimal::from_str("-1").unwrap());
        assert!(matches!(result, Err(PolyfillError::Validation { .. })));
    }

    #[test]
    fn test_parse_round_config_accepts_exact_supported_tick_sizes() {
        let config = parse_round_config(Decimal::from_str("0.1").unwrap()).unwrap();
        assert_eq!(config.price, 1);

        let config = parse_round_config(Decimal::from_str("0.0100").unwrap()).unwrap();
        assert_eq!(config.price, 2);

        let config = parse_round_config(Decimal::from_str("0.001").unwrap()).unwrap();
        assert_eq!(config.price, 3);

        let config = parse_round_config(Decimal::from_str("0.00010").unwrap()).unwrap();
        assert_eq!(config.price, 4);
    }

    #[test]
    fn test_parse_round_config_rejects_fractional_or_unsupported_tick_sizes() {
        for tick_size in ["0.00005", "0.00009", "0.00011", "0.0002", "0", "-0.01"] {
            let result = parse_round_config(Decimal::from_str(tick_size).unwrap());
            assert!(matches!(result, Err(PolyfillError::Validation { .. })));
        }
    }

    #[test]
    fn test_get_contract_config() {
        // Test Polygon mainnet
        let config = get_contract_config(137, false).expect("polygon config");
        assert_eq!(
            config.exchange,
            "0xE111180000d2663C0091e4f400237545B87B996B"
        );
        assert_eq!(
            config.collateral,
            "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
        );
        assert_eq!(
            config.conditional_tokens,
            "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
        );

        // Test with neg risk
        let config_neg = get_contract_config(137, true).expect("neg risk polygon config");
        assert_eq!(
            config_neg.exchange,
            "0xe2222d279d744050d28e00520010520000310F59"
        );
        assert_eq!(
            config_neg.collateral,
            "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
        );

        // Test unsupported chain
        let config_unsupported = get_contract_config(999, false);
        assert!(config_unsupported.is_none());
    }

    #[test]
    fn test_signature_type_from_u8() {
        assert_eq!(sig_type_from_u8(0).unwrap(), SigType::Eoa);
        assert_eq!(sig_type_from_u8(1).unwrap(), SigType::PolyProxy);
        assert_eq!(sig_type_from_u8(2).unwrap(), SigType::PolyGnosisSafe);
        assert_eq!(sig_type_from_u8(3).unwrap(), SigType::Poly1271);
        assert!(sig_type_from_u8(4).is_err());
    }

    #[test]
    fn test_derive_polygon_funder_addresses() {
        let eoa = Address::from_str("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266").unwrap();
        assert_eq!(
            derive_safe_wallet(eoa, 137).unwrap(),
            Address::from_str("0xd93b25Cb943D14d0d34FBAf01fc93a0F8b5f6e47").unwrap()
        );
        assert_eq!(
            derive_proxy_wallet(eoa, 137).unwrap(),
            Address::from_str("0x365f0cA36ae1F641E02Fe3b7743673DA42A13a70").unwrap()
        );
    }

    #[test]
    fn test_normalize_optional_bytes32_defaults_to_zero() {
        assert_eq!(
            normalize_optional_bytes32("builder_code", None).unwrap(),
            BYTES32_ZERO
        );
    }

    #[test]
    fn test_normalize_optional_bytes32_rejects_invalid_hex() {
        let err = normalize_optional_bytes32("metadata", Some("deadbeef")).unwrap_err();
        assert!(matches!(err, PolyfillError::Validation { .. }));
    }

    #[test]
    fn test_create_order_serializes_v2_fields_without_legacy_fields() {
        let builder = test_builder();
        let order = builder
            .create_order(
                137,
                &OrderArgs {
                    token_id: "123456".to_string(),
                    price: Decimal::from_str("0.45").unwrap(),
                    size: Decimal::from_str("12.34").unwrap(),
                    side: Side::BUY,
                    expiration: Some(1_900_000_000),
                    builder_code: Some(BYTES32_ZERO.to_string()),
                    metadata: None,
                },
                &CreateOrderOptions {
                    tick_size: Some(Decimal::from_str("0.01").unwrap()),
                    neg_risk: Some(false),
                },
            )
            .unwrap();

        let serialized = serde_json::to_value(&order).unwrap();
        let object = serialized.as_object().unwrap();
        assert!(object.contains_key("timestamp"));
        assert!(object.contains_key("metadata"));
        assert!(object.contains_key("builder"));
        assert!(object.contains_key("expiration"));
        assert!(!object.contains_key("taker"));
        assert!(!object.contains_key("nonce"));
        assert!(!object.contains_key("feeRateBps"));
        assert_eq!(order.builder, BYTES32_ZERO);
        assert_eq!(order.metadata, BYTES32_ZERO);
    }

    #[test]
    fn test_poly1271_order_uses_deposit_wallet_as_signer_and_wrapped_signature() {
        let signer: PrivateKeySigner =
            "0x2222222222222222222222222222222222222222222222222222222222222222"
                .parse()
                .expect("valid private key");
        let funder = Address::from_str("0x000000000000000000000000000000000000d077").unwrap();
        let builder = OrderBuilder::new(signer, Some(SigType::Poly1271), Some(funder));

        let order = builder
            .create_order(
                137,
                &OrderArgs {
                    token_id: "123456".to_string(),
                    price: Decimal::from_str("0.50").unwrap(),
                    size: Decimal::from_str("10").unwrap(),
                    side: Side::BUY,
                    expiration: None,
                    builder_code: Some(BYTES32_ZERO.to_string()),
                    metadata: Some(BYTES32_ZERO.to_string()),
                },
                &CreateOrderOptions {
                    tick_size: Some(Decimal::from_str("0.01").unwrap()),
                    neg_risk: Some(false),
                },
            )
            .unwrap();

        let funder_checksum = funder.to_checksum(None);
        assert_eq!(order.maker, funder_checksum);
        assert_eq!(order.signer, funder_checksum);
        assert_eq!(order.signature_type, SigType::Poly1271 as u8);
        assert!(order.signature.starts_with("0x"));
        assert!(
            order.signature.len() > 600,
            "POLY_1271 signature should be ERC-7739 wrapped"
        );
    }

    #[test]
    fn test_prepared_order_path_creates_equivalent_limit_order_fields() {
        let builder = test_builder();
        let args = OrderArgs {
            token_id: "123456".to_string(),
            price: Decimal::from_str("0.45").unwrap(),
            size: Decimal::from_str("12.34").unwrap(),
            side: Side::BUY,
            expiration: Some(1_900_000_000),
            builder_code: Some(BYTES32_ZERO.to_string()),
            metadata: None,
        };
        let options = CreateOrderOptions {
            tick_size: Some(Decimal::from_str("0.01").unwrap()),
            neg_risk: Some(false),
        };

        let order = builder.create_order(137, &args, &options).unwrap();
        let prepared = builder
            .prepare_order_path(
                137,
                args.token_id.clone(),
                options.tick_size.unwrap(),
                options.neg_risk.unwrap(),
                args.builder_code.as_deref(),
                args.metadata.as_deref(),
            )
            .unwrap();
        let prepared_order = prepared
            .create_limit_order(args.side, args.price, args.size, args.expiration)
            .unwrap();

        assert_eq!(prepared_order.maker, order.maker);
        assert_eq!(prepared_order.signer, order.signer);
        assert_eq!(prepared_order.token_id, order.token_id);
        assert_eq!(prepared_order.maker_amount, order.maker_amount);
        assert_eq!(prepared_order.taker_amount, order.taker_amount);
        assert_eq!(prepared_order.expiration, order.expiration);
        assert_eq!(prepared_order.side, order.side);
        assert_eq!(prepared_order.signature_type, order.signature_type);
        assert_eq!(prepared_order.metadata, order.metadata);
        assert_eq!(prepared_order.builder, order.builder);
        assert!(prepared_order.signature.starts_with("0x"));
    }

    #[test]
    fn test_prepared_market_order_rejects_unsupported_order_type() {
        let builder = test_builder();
        let prepared = builder
            .prepare_order_path(
                137,
                "123456",
                Decimal::from_str("0.01").unwrap(),
                false,
                None,
                None,
            )
            .unwrap();

        let err = prepared
            .create_market_order(
                Side::BUY,
                Decimal::from_str("10").unwrap(),
                Decimal::from_str("0.45").unwrap(),
                OrderType::GTC,
            )
            .unwrap_err();

        assert!(matches!(err, PolyfillError::Validation { .. }));
    }

    #[test]
    fn test_create_market_order_supports_fak() {
        let builder = test_builder();
        let order = builder
            .create_market_order(
                137,
                &MarketOrderArgs {
                    token_id: "123456".to_string(),
                    amount: Decimal::from_str("10.0").unwrap(),
                    side: Side::BUY,
                    order_type: OrderType::FAK,
                    price_limit: None,
                    user_usdc_balance: None,
                    builder_code: None,
                    metadata: None,
                },
                Decimal::from_str("0.25").unwrap(),
                &CreateOrderOptions {
                    tick_size: Some(Decimal::from_str("0.01").unwrap()),
                    neg_risk: Some(false),
                },
            )
            .unwrap();

        assert_eq!(order.side, "BUY");
        assert!(!order.timestamp.is_empty());
    }

    #[test]
    fn test_adjust_buy_amount_for_fees_uses_builder_rate_decimal() {
        let adjusted = adjust_buy_amount_for_fees(
            Decimal::from_str("100").unwrap(),
            Decimal::from_str("0.5").unwrap(),
            Decimal::from_str("100").unwrap(),
            Decimal::ZERO,
            0,
            Decimal::from_str("0.01").unwrap(),
        )
        .unwrap();

        assert_eq!(adjusted, Decimal::from_str("99.009900").unwrap());
    }

    #[test]
    fn test_adjust_buy_amount_for_fees_rejects_zero_after_truncation() {
        let err = adjust_buy_amount_for_fees(
            Decimal::from_str("1").unwrap(),
            Decimal::from_str("0.5").unwrap(),
            Decimal::from_str("0.0000009").unwrap(),
            Decimal::ZERO,
            0,
            Decimal::ZERO,
        )
        .unwrap_err();

        assert!(matches!(err, PolyfillError::Validation { .. }));
    }

    #[test]
    fn test_market_order_amounts_differ_for_buy_and_sell() {
        let builder = test_builder();
        let round_config = parse_round_config(Decimal::from_str("0.01").unwrap()).unwrap();

        let (buy_maker, buy_taker) = builder
            .get_market_order_amounts(
                Side::BUY,
                Decimal::from_str("10").unwrap(),
                Decimal::from_str("0.25").unwrap(),
                round_config,
            )
            .unwrap();
        let (sell_maker, sell_taker) = builder
            .get_market_order_amounts(
                Side::SELL,
                Decimal::from_str("10").unwrap(),
                Decimal::from_str("0.25").unwrap(),
                round_config,
            )
            .unwrap();

        assert_eq!(buy_maker, U256::from(10_000_000));
        assert_eq!(buy_taker, U256::from(40_000_000));
        assert_eq!(sell_maker, U256::from(10_000_000));
        assert_eq!(sell_taker, U256::from(2_500_000));
    }

    #[test]
    fn test_calculate_market_price_sweeps_rest_levels_best_first() {
        let builder = test_builder();
        // REST bids ascend and asks descend, so the best level is last.
        for (side, prices, amounts) in [
            (Side::SELL, ["0.48", "0.50"], ["5", "10", "15", "20", "25"]),
            (
                Side::BUY,
                ["0.52", "0.50"],
                ["2.5", "5", "7.5", "10.2", "12.5"],
            ),
        ] {
            let levels: Vec<_> = prices
                .map(|price| crate::types::BookLevel {
                    price: Decimal::from_str(price).unwrap(),
                    size: Decimal::from(10),
                })
                .into_iter()
                .collect();
            for order_type in [OrderType::FOK, OrderType::FAK] {
                for (index, amount) in amounts.into_iter().enumerate() {
                    let result = builder.calculate_market_price(
                        &levels,
                        Decimal::from_str(amount).unwrap(),
                        side,
                        order_type,
                    );
                    if index == 4 && order_type == OrderType::FOK {
                        assert!(result.is_err());
                    } else {
                        let expected = if index < 2 {
                            levels[1].price
                        } else {
                            levels[0].price
                        };
                        assert_eq!(
                            result.unwrap(),
                            expected,
                            "{side:?} {order_type:?} {amount}"
                        );
                    }
                }
                assert!(builder
                    .calculate_market_price(&[], Decimal::ONE, side, order_type)
                    .is_err());
            }
        }
    }

    #[test]
    fn test_signed_order_json_uses_camel_case_wire_shape() {
        let builder = test_builder();
        let order = builder
            .create_order(
                137,
                &OrderArgs {
                    token_id: "123456".to_string(),
                    price: Decimal::from_str("0.55").unwrap(),
                    size: Decimal::from_str("5.0").unwrap(),
                    side: Side::SELL,
                    expiration: Some(1_900_000_000),
                    builder_code: None,
                    metadata: Some(BYTES32_ZERO.to_string()),
                },
                &CreateOrderOptions {
                    tick_size: Some(Decimal::from_str("0.01").unwrap()),
                    neg_risk: Some(true),
                },
            )
            .unwrap();

        let json = serde_json::to_value(order).unwrap();
        assert!(matches!(json.get("tokenId"), Some(Value::String(_))));
        assert!(matches!(json.get("makerAmount"), Some(Value::String(_))));
        assert!(matches!(json.get("takerAmount"), Some(Value::String(_))));
        assert!(matches!(json.get("signatureType"), Some(Value::Number(_))));
    }

    #[test]
    fn test_seed_generation_uniqueness() {
        let mut seeds = std::collections::HashSet::new();

        // Generate 1000 seeds and ensure they're all unique
        for _ in 0..1000 {
            let seed = generate_seed();
            assert!(seeds.insert(seed), "Duplicate seed generated");
        }
    }

    #[test]
    fn test_seed_generation_range() {
        for _ in 0..100 {
            let seed = generate_seed();
            // Seeds should be positive and within reasonable range
            assert!(seed > 0);
            assert!(seed < u64::MAX);
        }
    }
}
