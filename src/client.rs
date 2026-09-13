//! High-performance Rust client for Polymarket
//!
//! This module provides a production-ready client for interacting with
//! Polymarket, optimized for high-frequency trading environments.

use crate::auth::{
    create_l1_headers, create_l2_headers, create_l2_headers_with_body_bytes, PreparedApiCredentials,
};
use crate::errors::{PolyfillError, Result};
use crate::http_config::{create_colocated_client, create_internet_client, prewarm_connections};
use crate::types::{
    BuilderFeeRateResponse, CancelOrdersResponse, ClientConfig, ClobMarketInfo, CreateOrderOptions,
    MarketOrderArgs, OrderArgs, OrderType, PostOrder, PostOrderOptions, PostOrderResponse, Side,
    SignedOrderRequest,
};
use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, CONTENT_TYPE, USER_AGENT};
use reqwest::Client;
use reqwest::{Method, RequestBuilder, Response};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

// Re-export types for compatibility
pub use crate::types::{ApiCredentials as ApiCreds, MarketOrderArgs as ClientMarketOrderArgs};

#[derive(Debug, Clone, serde::Deserialize)]
struct MarketByTokenResponse {
    condition_id: String,
}

#[derive(Serialize)]
struct TokenRequest<'a> {
    token_id: &'a str,
}

#[derive(Serialize)]
struct PriceRequest<'a> {
    token_id: &'a str,
    side: &'a str,
}

#[derive(Deserialize)]
struct DataPage<T> {
    data: Vec<T>,
    next_cursor: String,
}

fn polymarket_default_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static(concat!("polyfill-rs/", env!("CARGO_PKG_VERSION"))),
    );
    headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers
}

fn build_http_client(
    host: &str,
    timeout: Option<Duration>,
    max_connections: Option<usize>,
) -> Client {
    let max_connections = max_connections.unwrap_or(10);
    let mut builder = reqwest::ClientBuilder::new()
        .no_proxy()
        .default_headers(polymarket_default_headers())
        .http2_adaptive_window(true)
        .http2_initial_stream_window_size(512 * 1024)
        .tcp_nodelay(true)
        .pool_max_idle_per_host(max_connections)
        .pool_idle_timeout(Duration::from_secs(90));

    if let Some(timeout) = timeout {
        builder = builder.timeout(timeout);
    }

    if let Ok(resolve_ip) = std::env::var("POLYMARKET_RESOLVE_IP") {
        if let Ok(ip) = resolve_ip.parse::<IpAddr>() {
            if let Some(hostname) = extract_hostname(host) {
                builder = builder.resolve(hostname, SocketAddr::new(ip, 443));
            }
        }
    }

    builder.build().unwrap_or_else(|_| {
        reqwest::ClientBuilder::new()
            .no_proxy()
            .default_headers(polymarket_default_headers())
            .build()
            .expect("Failed to build reqwest client")
    })
}

fn extract_hostname(host: &str) -> Option<&str> {
    host.trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .and_then(|authority| authority.split(':').next())
        .filter(|hostname| !hostname.is_empty())
}

/// Main client for interacting with Polymarket API
pub struct ClobClient {
    pub http_client: Client,
    pub base_url: String,
    chain_id: u64,
    signer: Option<PrivateKeySigner>,
    api_creds: Option<PreparedApiCredentials>,
    builder_code: Option<String>,
    order_builder: Option<crate::orders::OrderBuilder>,
    #[allow(dead_code)]
    connection_manager: Option<std::sync::Arc<crate::connection_manager::ConnectionManager>>,
}

#[derive(Default)]
struct ClientAuthConfig {
    signer: Option<PrivateKeySigner>,
    api_creds: Option<PreparedApiCredentials>,
    builder_code: Option<String>,
    sig_type: Option<crate::orders::SigType>,
    funder: Option<Address>,
}

impl ClobClient {
    async fn parse_json_response<T>(response: Response) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let mut bytes = response
            .bytes()
            .await
            .map_err(|e| PolyfillError::network(format!("Failed to read response body: {e}"), e))?
            .to_vec();

        crate::decode::fast_parse::parse_json_fast(&mut bytes)
    }

    fn build_client(
        host: &str,
        chain_id: u64,
        http_client: Client,
        auth: ClientAuthConfig,
    ) -> Self {
        let connection_manager = Some(std::sync::Arc::new(
            crate::connection_manager::ConnectionManager::new(
                http_client.clone(),
                host.to_string(),
            ),
        ));

        let order_builder = auth
            .signer
            .clone()
            .map(|signer| crate::orders::OrderBuilder::new(signer, auth.sig_type, auth.funder));

        Self {
            http_client,
            base_url: host.to_string(),
            chain_id,
            signer: auth.signer,
            api_creds: auth.api_creds,
            builder_code: auth.builder_code,
            order_builder,
            connection_manager,
        }
    }

    /// Create a new client with optimized HTTP/2 settings (benchmarked 11.4% faster)
    /// Connection prewarming is explicit through [`ClobClient::prewarm_connections`].
    pub fn new(host: &str) -> Self {
        let http_client = build_http_client(host, None, None);
        Self::build_client(host, 137, http_client, ClientAuthConfig::default())
    }

    /// Create a V2-native client from config.
    pub fn from_config(config: ClientConfig) -> Result<Self> {
        let signer = match config.private_key.as_deref() {
            Some(private_key) => Some(
                private_key
                    .parse::<PrivateKeySigner>()
                    .map_err(|e| PolyfillError::config(format!("Invalid private key: {e}")))?,
            ),
            None => None,
        };

        let sig_type = config
            .signature_type
            .map(crate::orders::sig_type_from_u8)
            .transpose()?;
        let explicit_funder = config
            .funder
            .as_deref()
            .map(Address::from_str)
            .transpose()
            .map_err(|e| PolyfillError::config(format!("Invalid funder address: {e}")))?;
        let funder = match (&signer, sig_type) {
            (Some(signer), Some(sig_type)) => crate::orders::resolve_funder(
                signer.address(),
                config.chain,
                sig_type,
                explicit_funder,
            )?,
            _ => explicit_funder,
        };

        let http_client =
            build_http_client(&config.base_url, config.timeout, config.max_connections);

        Ok(Self::build_client(
            &config.base_url,
            config.chain,
            http_client,
            ClientAuthConfig {
                signer,
                api_creds: config
                    .api_credentials
                    .map(PreparedApiCredentials::try_new)
                    .transpose()?,
                builder_code: config.builder_code,
                sig_type,
                funder,
            },
        ))
    }

    /// Create a client optimized for co-located environments
    pub fn new_colocated(host: &str) -> Self {
        let http_client = create_colocated_client().unwrap_or_else(|_| {
            reqwest::ClientBuilder::new()
                .no_proxy()
                .build()
                .expect("Failed to build reqwest client")
        });
        Self::build_client(host, 137, http_client, ClientAuthConfig::default())
    }

    /// Create a client optimized for internet connections
    pub fn new_internet(host: &str) -> Self {
        let http_client = create_internet_client().unwrap_or_else(|_| {
            reqwest::ClientBuilder::new()
                .no_proxy()
                .build()
                .expect("Failed to build reqwest client")
        });
        Self::build_client(host, 137, http_client, ClientAuthConfig::default())
    }

    /// Create a client with L1 headers (for authentication)
    #[deprecated(note = "Use ClobClient::from_config(ClientConfig) for authenticated clients")]
    pub fn with_l1_headers(host: &str, private_key: &str, chain_id: u64) -> Self {
        Self::from_config(ClientConfig {
            base_url: host.to_string(),
            chain: chain_id,
            private_key: Some(private_key.to_string()),
            ..ClientConfig::default()
        })
        .expect("failed to build authenticated client")
    }

    /// Create a client with L2 headers (for API key authentication)
    #[deprecated(note = "Use ClobClient::from_config(ClientConfig) for authenticated clients")]
    pub fn with_l2_headers(
        host: &str,
        private_key: &str,
        chain_id: u64,
        api_creds: ApiCreds,
    ) -> Self {
        Self::from_config(ClientConfig {
            base_url: host.to_string(),
            chain: chain_id,
            private_key: Some(private_key.to_string()),
            api_credentials: Some(api_creds),
            ..ClientConfig::default()
        })
        .expect("failed to build authenticated client")
    }

    /// Set API credentials
    pub fn set_api_creds(&mut self, api_creds: ApiCreds) -> Result<()> {
        self.api_creds = Some(PreparedApiCredentials::try_new(api_creds)?);
        Ok(())
    }

    /// Start background keep-alive to maintain warm connection
    /// Sends periodic lightweight requests to prevent connection drops
    pub async fn start_keepalive(&self, interval: std::time::Duration) {
        if let Some(manager) = &self.connection_manager {
            manager.start_keepalive(interval).await;
        }
    }

    /// Stop keep-alive background task
    pub async fn stop_keepalive(&self) {
        if let Some(manager) = &self.connection_manager {
            manager.stop_keepalive().await;
        }
    }

    /// Pre-warm connections to reduce first-request latency
    pub async fn prewarm_connections(&self) -> Result<()> {
        prewarm_connections(&self.http_client, &self.base_url)
            .await
            .map_err(|e| {
                PolyfillError::network(format!("Failed to prewarm connections: {}", e), e)
            })?;
        Ok(())
    }

    /// Get the wallet address
    pub fn get_address(&self) -> Option<String> {
        use alloy_primitives::hex;
        self.signer
            .as_ref()
            .map(|s| hex::encode_prefixed(s.address().as_slice()))
    }

    /// Get the collateral token address for the current chain
    pub fn get_collateral_address(&self) -> Option<String> {
        let config = crate::orders::get_contract_config(self.chain_id, false)?;
        Some(config.collateral)
    }

    /// Get the conditional tokens contract address for the current chain
    pub fn get_conditional_address(&self) -> Option<String> {
        let config = crate::orders::get_contract_config(self.chain_id, false)?;
        Some(config.conditional_tokens)
    }

    /// Get the exchange contract address for the current chain
    pub fn get_exchange_address(&self) -> Option<String> {
        let config = crate::orders::get_contract_config(self.chain_id, false)?;
        Some(config.exchange)
    }

    /// Test basic connectivity
    pub async fn get_ok(&self) -> bool {
        match self
            .http_client
            .get(format!("{}/ok", self.base_url))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Get server time
    pub async fn get_server_time(&self) -> Result<u64> {
        let response = self
            .http_client
            .get(format!("{}/time", self.base_url))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get server time",
            ));
        }

        let time_text = response.text().await?;
        let timestamp = time_text
            .trim()
            .parse::<u64>()
            .map_err(|e| PolyfillError::parse(format!("Invalid timestamp format: {}", e), None))?;

        Ok(timestamp)
    }

    /// Get order book for a token
    pub async fn get_order_book(&self, token_id: &str) -> Result<OrderBookSummary> {
        let response = self
            .http_client
            .get(format!("{}/book", self.base_url))
            .query(&[("token_id", token_id)])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get order book",
            ));
        }

        Self::parse_json_response(response).await
    }

    /// Get midpoint for a token
    pub async fn get_midpoint(&self, token_id: &str) -> Result<MidpointResponse> {
        let response = self
            .http_client
            .get(format!("{}/midpoint", self.base_url))
            .query(&[("token_id", token_id)])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get midpoint",
            ));
        }

        Self::parse_json_response(response).await
    }

    /// Get spread for a token
    pub async fn get_spread(&self, token_id: &str) -> Result<SpreadResponse> {
        let response = self
            .http_client
            .get(format!("{}/spread", self.base_url))
            .query(&[("token_id", token_id)])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get spread",
            ));
        }

        let spread: SpreadResponse = response.json().await?;
        Ok(spread)
    }

    /// Get spreads for multiple tokens (batch)
    pub async fn get_spreads(
        &self,
        token_ids: &[String],
    ) -> Result<std::collections::HashMap<String, Decimal>> {
        let request_data: Vec<TokenRequest<'_>> = token_ids
            .iter()
            .map(|id| TokenRequest { token_id: id })
            .collect();

        let response = self
            .http_client
            .post(format!("{}/spreads", self.base_url))
            .json(&request_data)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get batch spreads",
            ));
        }

        response
            .json::<std::collections::HashMap<String, Decimal>>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Get price for a token and side
    pub async fn get_price(&self, token_id: &str, side: Side) -> Result<PriceResponse> {
        let response = self
            .http_client
            .get(format!("{}/price", self.base_url))
            .query(&[("token_id", token_id), ("side", side.as_str())])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get price",
            ));
        }

        let price: PriceResponse = response.json().await?;
        Ok(price)
    }

    async fn get_market_by_token(&self, token_id: &str) -> Result<MarketByTokenResponse> {
        let response = self
            .http_client
            .get(format!("{}/markets-by-token/{}", self.base_url, token_id))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get market by token",
            ));
        }

        response
            .json::<MarketByTokenResponse>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {e}"), None))
    }

    /// Get V2 CLOB-level market info for a condition ID.
    pub async fn get_clob_market_info(&self, condition_id: &str) -> Result<ClobMarketInfo> {
        let response = self
            .http_client
            .get(format!("{}/clob-markets/{}", self.base_url, condition_id))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get clob market info",
            ));
        }

        response
            .json::<ClobMarketInfo>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {e}"), None))
    }

    /// Get V2 builder fee rates for a bytes32 builder code.
    pub async fn get_builder_fee_rate(&self, builder_code: &str) -> Result<BuilderFeeRateResponse> {
        crate::orders::validate_bytes32_hex("builder_code", builder_code)?;

        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let endpoint = format!("/fees/builder-fees/{builder_code}");
        let headers = create_l2_headers::<Value>(signer, api_creds, "GET", &endpoint, None)?;
        let req = self.create_request_with_headers(Method::GET, &endpoint, headers.into_iter());

        let response = req.send().await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            let message = if body.is_empty() {
                "Failed to get builder fee rate".to_string()
            } else {
                format!("Failed to get builder fee rate: {body}")
            };
            return Err(PolyfillError::api(status, message));
        }

        response
            .json::<BuilderFeeRateResponse>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {e}"), None))
    }

    fn validate_prices_history_asset_id(asset_id: &str) -> Result<()> {
        if asset_id.is_empty() {
            return Err(PolyfillError::validation(
                "asset_id is required (use the decimal token_id / asset_id)",
            ));
        }

        // Common footgun: passing a condition id (0x...) instead of the decimal asset id.
        if asset_id.starts_with("0x") || asset_id.starts_with("0X") {
            return Err(PolyfillError::validation(
                "`/prices-history` expects a decimal token_id/asset_id, not a hex condition_id",
            ));
        }

        if !asset_id.as_bytes().iter().all(u8::is_ascii_digit) {
            return Err(PolyfillError::validation(
                "asset_id must be a decimal string (token_id / asset_id)",
            ));
        }

        Ok(())
    }

    /// Get price history for a single outcome (`token_id` / `asset_id`) over a fixed interval.
    ///
    /// Important: the upstream API query parameter is named `market`, but it expects the
    /// decimal outcome asset id (not the hex `condition_id`).
    pub async fn get_prices_history_interval(
        &self,
        asset_id: &str,
        interval: PricesHistoryInterval,
        fidelity: Option<u32>,
    ) -> Result<PricesHistoryResponse> {
        Self::validate_prices_history_asset_id(asset_id)?;

        let mut request = self
            .http_client
            .get(format!("{}/prices-history", self.base_url))
            .query(&[("market", asset_id), ("interval", interval.as_str())]);

        if let Some(fidelity) = fidelity {
            request = request.query(&[("fidelity", fidelity)]);
        }

        let response = request.send().await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            let message = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| {
                    v.get("error")
                        .and_then(Value::as_str)
                        .map(|s| s.to_string())
                })
                .unwrap_or_else(|| {
                    if body.is_empty() {
                        "Failed to get prices history".to_string()
                    } else {
                        body
                    }
                });
            return Err(PolyfillError::api(status, message));
        }

        Ok(response.json::<PricesHistoryResponse>().await?)
    }

    /// Get price history for a single outcome (`token_id` / `asset_id`) over a timestamp range.
    ///
    /// `start_ts` and `end_ts` are Unix timestamps (seconds).
    pub async fn get_prices_history_range(
        &self,
        asset_id: &str,
        start_ts: u64,
        end_ts: u64,
        fidelity: Option<u32>,
    ) -> Result<PricesHistoryResponse> {
        Self::validate_prices_history_asset_id(asset_id)?;

        if start_ts >= end_ts {
            return Err(PolyfillError::validation(
                "start_ts must be < end_ts for prices history",
            ));
        }

        let mut request = self
            .http_client
            .get(format!("{}/prices-history", self.base_url))
            .query(&[("market", asset_id)])
            .query(&[("startTs", start_ts), ("endTs", end_ts)]);

        if let Some(fidelity) = fidelity {
            request = request.query(&[("fidelity", fidelity)]);
        }

        let response = request.send().await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            let message = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| {
                    v.get("error")
                        .and_then(Value::as_str)
                        .map(|s| s.to_string())
                })
                .unwrap_or_else(|| {
                    if body.is_empty() {
                        "Failed to get prices history".to_string()
                    } else {
                        body
                    }
                });
            return Err(PolyfillError::api(status, message));
        }

        Ok(response.json::<PricesHistoryResponse>().await?)
    }

    /// Get tick size for a token
    pub async fn get_tick_size(&self, token_id: &str) -> Result<Decimal> {
        let response = self
            .http_client
            .get(format!("{}/tick-size", self.base_url))
            .query(&[("token_id", token_id)])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get tick size",
            ));
        }

        let tick_size_response: Value = response.json().await?;
        let tick_size = tick_size_response["minimum_tick_size"]
            .as_str()
            .and_then(|s| Decimal::from_str(s).ok())
            .or_else(|| {
                tick_size_response["minimum_tick_size"]
                    .as_f64()
                    .map(|f| Decimal::from_f64(f).unwrap_or(Decimal::ZERO))
            })
            .ok_or_else(|| PolyfillError::parse("Invalid tick size format", None))?;

        Ok(tick_size)
    }

    /// Get maker fee rate (in bps) for a token
    pub async fn get_fee_rate_bps(&self, token_id: &str) -> Result<u32> {
        let response = self
            .http_client
            .get(format!("{}/fee-rate", self.base_url))
            .query(&[("token_id", token_id)])
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get fee rate",
            ));
        }

        let fee_rate: crate::types::FeeRateResponse = response
            .json()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))?;
        Ok(fee_rate.base_fee)
    }

    /// Create a new API key
    pub async fn create_api_key(&self, nonce: Option<U256>) -> Result<ApiCreds> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;

        let headers = create_l1_headers(signer, nonce)?;
        let req =
            self.create_request_with_headers(Method::POST, "/auth/api-key", headers.into_iter());

        let response = req.send().await?;
        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to create API key",
            ));
        }

        Ok(response.json::<ApiCreds>().await?)
    }

    /// Derive an existing API key
    pub async fn derive_api_key(&self, nonce: Option<U256>) -> Result<ApiCreds> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;

        let headers = create_l1_headers(signer, nonce)?;
        let req = self.create_request_with_headers(
            Method::GET,
            "/auth/derive-api-key",
            headers.into_iter(),
        );

        let response = req.send().await?;
        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to derive API key",
            ));
        }

        Ok(response.json::<ApiCreds>().await?)
    }

    /// Create or derive API key (try create first, fallback to derive)
    pub async fn create_or_derive_api_key(&self, nonce: Option<U256>) -> Result<ApiCreds> {
        match self.create_api_key(nonce).await {
            Ok(creds) => Ok(creds),
            // Only fall back to derive on API status errors (server responded).
            // Propagate network/parse/internal errors so callers can handle them appropriately.
            Err(PolyfillError::Api { .. }) => self.derive_api_key(nonce).await,
            Err(err) => Err(err),
        }
    }

    /// Get all API keys for the authenticated user
    pub async fn get_api_keys(&self) -> Result<Vec<String>> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::config("Signer not configured"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::config("API credentials not configured"))?;

        let method = Method::GET;
        let endpoint = "/auth/api-keys";
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let response = self
            .http_client
            .request(method, format!("{}{}", self.base_url, endpoint))
            .headers(
                headers
                    .into_iter()
                    .map(|(k, v)| (HeaderName::from_static(k), v.parse().unwrap()))
                    .collect(),
            )
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        let api_keys_response: crate::types::ApiKeysResponse = response
            .json()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))?;

        Ok(api_keys_response.api_keys)
    }

    /// Delete the current API key
    pub async fn delete_api_key(&self) -> Result<String> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::config("Signer not configured"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::config("API credentials not configured"))?;

        let method = Method::DELETE;
        let endpoint = "/auth/api-key";
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let response = self
            .http_client
            .request(method, format!("{}{}", self.base_url, endpoint))
            .headers(
                headers
                    .into_iter()
                    .map(|(k, v)| (HeaderName::from_static(k), v.parse().unwrap()))
                    .collect(),
            )
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        response
            .text()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Helper to create request with headers
    fn create_request_with_headers(
        &self,
        method: Method,
        endpoint: &str,
        headers: impl Iterator<Item = (&'static str, String)>,
    ) -> RequestBuilder {
        let req = self
            .http_client
            .request(method, format!("{}{}", self.base_url, endpoint));
        headers.fold(req, |r, (k, v)| r.header(HeaderName::from_static(k), v))
    }

    fn serialize_json_body<T: ?Sized + Serialize>(body: &T) -> Result<Vec<u8>> {
        serde_json::to_vec(body)
            .map_err(|e| PolyfillError::parse(format!("Failed to serialize body: {e}"), None))
    }

    fn create_request_with_json_bytes(
        &self,
        method: Method,
        endpoint: &str,
        headers: impl Iterator<Item = (&'static str, String)>,
        body_bytes: Vec<u8>,
    ) -> RequestBuilder {
        self.create_request_with_headers(method, endpoint, headers)
            .header(CONTENT_TYPE, "application/json")
            .body(body_bytes)
    }

    /// Get neg risk for a token
    pub async fn get_neg_risk(&self, token_id: &str) -> Result<bool> {
        let response = self
            .http_client
            .get(format!("{}/neg-risk", self.base_url))
            .query(&[("token_id", token_id)])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get neg risk",
            ));
        }

        let neg_risk_response: Value = response.json().await?;
        let neg_risk = neg_risk_response["neg_risk"]
            .as_bool()
            .ok_or_else(|| PolyfillError::parse("Invalid neg risk format", None))?;

        Ok(neg_risk)
    }

    /// Resolve tick size for an order
    async fn resolve_tick_size(
        &self,
        token_id: &str,
        tick_size: Option<Decimal>,
    ) -> Result<Decimal> {
        match tick_size {
            Some(t) => Ok(t),
            None => self.get_tick_size(token_id).await,
        }
    }

    /// Get filled order options
    async fn get_filled_order_options(
        &self,
        token_id: &str,
        options: Option<&CreateOrderOptions>,
    ) -> Result<CreateOrderOptions> {
        let (tick_size, neg_risk) = match options {
            Some(o) => (o.tick_size, o.neg_risk),
            None => (None, None),
        };

        let tick_size = self.resolve_tick_size(token_id, tick_size).await?;
        let neg_risk = match neg_risk {
            Some(nr) => nr,
            None => self.get_neg_risk(token_id).await?,
        };

        Ok(CreateOrderOptions {
            tick_size: Some(tick_size),
            neg_risk: Some(neg_risk),
        })
    }

    /// Check if price is in valid range
    fn is_price_in_range(&self, price: Decimal, tick_size: Decimal) -> bool {
        let min_price = tick_size;
        let max_price = Decimal::ONE - tick_size;
        price >= min_price && price <= max_price
    }

    async fn get_clob_market_info_for_token(&self, token_id: &str) -> Result<ClobMarketInfo> {
        let market = self.get_market_by_token(token_id).await?;
        self.get_clob_market_info(&market.condition_id).await
    }

    /// Prepare reusable order-path state for repeated orders on one token.
    ///
    /// This resolves tick-size/neg-risk once, then caches fixed order-path inputs such as the
    /// parsed token ID, exchange address, normalized bytes32 values, and EIP-712 domain.
    pub async fn prepare_order_path(
        &self,
        token_id: &str,
        options: Option<&CreateOrderOptions>,
        builder_code: Option<&str>,
        metadata: Option<&str>,
    ) -> Result<crate::orders::PreparedOrderPath> {
        let order_builder = self
            .order_builder
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Order builder not initialized"))?;

        let create_order_options = self.get_filled_order_options(token_id, options).await?;
        let builder_code = builder_code.or(self.builder_code.as_deref());

        order_builder.prepare_order_path(
            self.chain_id,
            token_id,
            create_order_options.tick_size.expect("Should be filled"),
            create_order_options.neg_risk.expect("Should be filled"),
            builder_code,
            metadata,
        )
    }

    /// Create an order
    pub async fn create_order(
        &self,
        order_args: &OrderArgs,
        options: Option<&CreateOrderOptions>,
    ) -> Result<SignedOrderRequest> {
        let order_builder = self
            .order_builder
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Order builder not initialized"))?;

        let create_order_options = self
            .get_filled_order_options(&order_args.token_id, options)
            .await?;
        let mut order_args = order_args.clone();
        if order_args.builder_code.is_none() {
            order_args.builder_code = self.builder_code.clone();
        }

        if !self.is_price_in_range(
            order_args.price,
            create_order_options.tick_size.expect("Should be filled"),
        ) {
            return Err(PolyfillError::validation(
                "Price is not in range of tick_size",
            ));
        }

        order_builder.create_order(self.chain_id, &order_args, &create_order_options)
    }

    /// Calculate market price from order book
    async fn calculate_market_price(
        &self,
        token_id: &str,
        side: Side,
        amount: Decimal,
        order_type: OrderType,
    ) -> Result<Decimal> {
        let book = self.get_order_book(token_id).await?;
        let order_builder = self
            .order_builder
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Order builder not initialized"))?;

        // Convert OrderSummary to BookLevel
        let levels: Vec<crate::types::BookLevel> = match side {
            Side::BUY => book
                .asks
                .into_iter()
                .map(|s| crate::types::BookLevel {
                    price: s.price,
                    size: s.size,
                })
                .collect(),
            Side::SELL => book
                .bids
                .into_iter()
                .map(|s| crate::types::BookLevel {
                    price: s.price,
                    size: s.size,
                })
                .collect(),
        };

        order_builder.calculate_market_price(&levels, amount, side, order_type)
    }

    /// Create a market order
    pub async fn create_market_order(
        &self,
        order_args: &MarketOrderArgs,
        options: Option<&CreateOrderOptions>,
    ) -> Result<SignedOrderRequest> {
        let order_builder = self
            .order_builder
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Order builder not initialized"))?;

        let create_order_options = self
            .get_filled_order_options(&order_args.token_id, options)
            .await?;
        if !matches!(order_args.order_type, OrderType::FOK | OrderType::FAK) {
            return Err(PolyfillError::validation(
                "Market orders only support FOK and FAK order types",
            ));
        }

        let mut order_args = order_args.clone();
        if order_args.builder_code.is_none() {
            order_args.builder_code = self.builder_code.clone();
        }

        let market_price = self
            .calculate_market_price(
                &order_args.token_id,
                order_args.side,
                order_args.amount,
                order_args.order_type,
            )
            .await?;

        let price = match order_args.price_limit {
            Some(limit) => {
                let limit_ok = match order_args.side {
                    Side::BUY => market_price <= limit,
                    Side::SELL => market_price >= limit,
                };
                if !limit_ok {
                    return Err(PolyfillError::validation(format!(
                        "Calculated market price {market_price} violates price_limit {limit}"
                    )));
                }
                limit
            },
            None => market_price,
        };

        if order_args.side == Side::BUY {
            if let Some(user_balance) = order_args.user_usdc_balance {
                let market_info = self
                    .get_clob_market_info_for_token(&order_args.token_id)
                    .await?;
                let fee_details = market_info.fd.unwrap_or(crate::types::ClobFeeDetails {
                    r: Decimal::ZERO,
                    e: 0,
                    to: false,
                });
                let builder_taker_fee_rate = match order_args.builder_code.as_deref() {
                    Some(code) if code != crate::orders::BYTES32_ZERO => {
                        let rate = self.get_builder_fee_rate(code).await?;
                        Decimal::from(rate.builder_taker_fee_rate_bps) / Decimal::from(10_000_u32)
                    },
                    _ => Decimal::ZERO,
                };

                order_args.amount = crate::orders::adjust_buy_amount_for_fees(
                    order_args.amount,
                    price,
                    user_balance,
                    fee_details.r,
                    fee_details.e,
                    builder_taker_fee_rate,
                )?;
            }
        }

        if !self.is_price_in_range(
            price,
            create_order_options.tick_size.expect("Should be filled"),
        ) {
            return Err(PolyfillError::validation(
                "Price is not in range of tick_size",
            ));
        }

        order_builder.create_market_order(self.chain_id, &order_args, price, &create_order_options)
    }

    /// Post an order to the exchange
    pub async fn post_order(
        &self,
        order: SignedOrderRequest,
        options: Option<&PostOrderOptions>,
    ) -> Result<PostOrderResponse> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;
        let options = options.copied().unwrap_or_default();

        if options.post_only && matches!(options.order_type, OrderType::FOK | OrderType::FAK) {
            return Err(PolyfillError::validation(
                "post_only is not supported for FOK/FAK orders",
            ));
        }
        let expiration = order.expiration.parse::<u64>().map_err(|e| {
            PolyfillError::validation(format!(
                "Invalid order expiration '{}': {e}",
                order.expiration
            ))
        })?;
        if expiration > 0 && options.order_type != OrderType::GTD {
            return Err(PolyfillError::validation(
                "expiration is only supported for GTD orders",
            ));
        }

        // Owner field must reference the credential principal identifier
        // to maintain consistency with the authentication context layer
        let body = PostOrder::new(order, api_creds.api_key.clone(), options);
        let body_bytes = Self::serialize_json_body(&body)?;

        let headers = create_l2_headers_with_body_bytes(
            signer,
            api_creds,
            "POST",
            "/order",
            Some(&body_bytes),
        )?;
        let req = self.create_request_with_json_bytes(
            Method::POST,
            "/order",
            headers.into_iter(),
            body_bytes,
        );

        let response = req.send().await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            let message = if body.is_empty() {
                "Failed to post order".to_string()
            } else {
                format!("Failed to post order: {}", body)
            };
            return Err(PolyfillError::api(status, message));
        }

        response
            .json::<PostOrderResponse>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {e}"), None))
    }

    /// Create and post an order in one call
    pub async fn create_and_post_order(
        &self,
        order_args: &OrderArgs,
        create_options: Option<&CreateOrderOptions>,
        post_options: Option<&PostOrderOptions>,
    ) -> Result<PostOrderResponse> {
        let order = self.create_order(order_args, create_options).await?;
        self.post_order(order, post_options).await
    }

    /// Create and post a market order in one call.
    pub async fn create_and_post_market_order(
        &self,
        order_args: &MarketOrderArgs,
        create_options: Option<&CreateOrderOptions>,
        post_options: Option<&PostOrderOptions>,
    ) -> Result<PostOrderResponse> {
        let post_options = post_options.copied().unwrap_or(PostOrderOptions {
            order_type: order_args.order_type,
            post_only: false,
            defer_exec: false,
        });
        let order = self.create_market_order(order_args, create_options).await?;
        self.post_order(order, Some(&post_options)).await
    }

    /// Cancel an order
    pub async fn cancel(&self, order_id: &str) -> Result<CancelOrdersResponse> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let body = std::collections::HashMap::from([("orderID", order_id)]);
        let body_bytes = Self::serialize_json_body(&body)?;

        let headers = create_l2_headers_with_body_bytes(
            signer,
            api_creds,
            "DELETE",
            "/order",
            Some(&body_bytes),
        )?;
        let req = self.create_request_with_json_bytes(
            Method::DELETE,
            "/order",
            headers.into_iter(),
            body_bytes,
        );

        let response = req.send().await?;
        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to cancel order",
            ));
        }

        response
            .json::<CancelOrdersResponse>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {e}"), None))
    }

    /// Cancel multiple orders
    pub async fn cancel_orders(&self, order_ids: &[String]) -> Result<CancelOrdersResponse> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let body_bytes = Self::serialize_json_body(order_ids)?;
        let headers = create_l2_headers_with_body_bytes(
            signer,
            api_creds,
            "DELETE",
            "/orders",
            Some(&body_bytes),
        )?;
        let req = self.create_request_with_json_bytes(
            Method::DELETE,
            "/orders",
            headers.into_iter(),
            body_bytes,
        );

        let response = req.send().await?;
        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to cancel orders",
            ));
        }

        response
            .json::<CancelOrdersResponse>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {e}"), None))
    }

    /// Cancel all orders
    pub async fn cancel_all(&self) -> Result<CancelOrdersResponse> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let headers = create_l2_headers::<Value>(signer, api_creds, "DELETE", "/cancel-all", None)?;
        let req =
            self.create_request_with_headers(Method::DELETE, "/cancel-all", headers.into_iter());

        let response = req.send().await?;
        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to cancel all orders",
            ));
        }

        response
            .json::<CancelOrdersResponse>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {e}"), None))
    }

    /// Get open orders with optional filtering
    ///
    /// This retrieves all open orders for the authenticated user. You can filter by:
    /// - Order ID (exact match)
    /// - Asset/Token ID (all orders for a specific token)
    /// - Market ID (all orders for a specific market)
    ///
    /// The response includes order status, fill information, and timestamps.
    pub async fn get_orders(
        &self,
        params: Option<&crate::types::OpenOrderParams>,
        next_cursor: Option<&str>,
    ) -> Result<Vec<crate::types::OpenOrder>> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let method = Method::GET;
        let endpoint = "/data/orders";
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let query_params = match params {
            None => Vec::new(),
            Some(p) => p.to_query_params(),
        };

        let mut next_cursor = next_cursor.unwrap_or("MA==").to_string(); // INITIAL_CURSOR
        let mut output = Vec::new();

        while next_cursor != "LTE=" {
            // END_CURSOR
            let req = self
                .http_client
                .request(method.clone(), format!("{}{}", self.base_url, endpoint))
                .query(&query_params)
                .query(&[("next_cursor", &next_cursor)]);

            let r = headers
                .clone()
                .into_iter()
                .fold(req, |r, (k, v)| r.header(HeaderName::from_static(k), v));

            let page = r
                .send()
                .await
                .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?
                .json::<DataPage<crate::types::OpenOrder>>()
                .await
                .map_err(|e| {
                    PolyfillError::parse(format!("Failed to parse response: {}", e), None)
                })?;

            next_cursor = page.next_cursor;
            output.extend(page.data);
        }

        Ok(output)
    }

    /// Get trade history with optional filtering
    ///
    /// This retrieves historical trades for the authenticated user. You can filter by:
    /// - Trade ID (exact match)
    /// - Maker address (trades where you were the maker)
    /// - Market ID (trades in a specific market)
    /// - Asset/Token ID (trades for a specific token)
    /// - Time range (before/after timestamps)
    ///
    /// Trades are returned in reverse chronological order (newest first).
    pub async fn get_trades(
        &self,
        trade_params: Option<&crate::types::TradeParams>,
        next_cursor: Option<&str>,
    ) -> Result<Vec<Value>> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let method = Method::GET;
        let endpoint = "/data/trades";
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let query_params = match trade_params {
            None => Vec::new(),
            Some(p) => p.to_query_params(),
        };

        let mut next_cursor = next_cursor.unwrap_or("MA==").to_string(); // INITIAL_CURSOR
        let mut output = Vec::new();

        while next_cursor != "LTE=" {
            // END_CURSOR
            let req = self
                .http_client
                .request(method.clone(), format!("{}{}", self.base_url, endpoint))
                .query(&query_params)
                .query(&[("next_cursor", &next_cursor)]);

            let r = headers
                .clone()
                .into_iter()
                .fold(req, |r, (k, v)| r.header(HeaderName::from_static(k), v));

            let page = r
                .send()
                .await
                .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?
                .json::<DataPage<Value>>()
                .await
                .map_err(|e| {
                    PolyfillError::parse(format!("Failed to parse response: {}", e), None)
                })?;

            next_cursor = page.next_cursor;
            output.extend(page.data);
        }

        Ok(output)
    }

    /// Get balance and allowance information for all assets
    ///
    /// This returns the current balance and allowance for each asset in your account.
    /// Balance is how much you own, allowance is how much the exchange can spend on your behalf.
    ///
    /// You need both balance and allowance to place orders - the exchange needs permission
    /// to move your tokens when orders are filled.
    pub async fn get_balance_allowance(
        &self,
        params: Option<crate::types::BalanceAllowanceParams>,
    ) -> Result<Value> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let mut params = params.unwrap_or_default();
        if params.signature_type.is_none() {
            params.set_signature_type(
                self.order_builder
                    .as_ref()
                    .expect("OrderBuilder not set")
                    .get_sig_type(),
            );
        }

        let query_params = params.to_query_params();

        let method = Method::GET;
        let endpoint = "/balance-allowance";
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let response = self
            .http_client
            .request(method, format!("{}{}", self.base_url, endpoint))
            .headers(
                headers
                    .into_iter()
                    .map(|(k, v)| (HeaderName::from_static(k), v.parse().unwrap()))
                    .collect(),
            )
            .query(&query_params)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        response
            .json::<Value>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Set up notifications for order fills and other events
    ///
    /// This configures push notifications so you get alerted when:
    /// - Your orders get filled
    /// - Your orders get cancelled
    /// - Market conditions change significantly
    ///
    /// The signature proves you own the account and want to receive notifications.
    pub async fn get_notifications(&self) -> Result<Value> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let method = Method::GET;
        let endpoint = "/notifications";
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let response = self
            .http_client
            .request(method, format!("{}{}", self.base_url, endpoint))
            .headers(
                headers
                    .into_iter()
                    .map(|(k, v)| (HeaderName::from_static(k), v.parse().unwrap()))
                    .collect(),
            )
            .query(&[(
                "signature_type",
                &self
                    .order_builder
                    .as_ref()
                    .expect("OrderBuilder not set")
                    .get_sig_type()
                    .to_string(),
            )])
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        response
            .json::<Value>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Get midpoints for multiple tokens in a single request
    ///
    /// This is much more efficient than calling get_midpoint() multiple times.
    /// Instead of N round trips, you make just 1 request and get all the midpoints back.
    ///
    /// Midpoints are returned as a HashMap where the key is the token_id and the value
    /// is the midpoint price (or None if there's no valid midpoint).
    pub async fn get_midpoints(
        &self,
        token_ids: &[String],
    ) -> Result<std::collections::HashMap<String, Decimal>> {
        let request_data: Vec<TokenRequest<'_>> = token_ids
            .iter()
            .map(|id| TokenRequest { token_id: id })
            .collect();

        let response = self
            .http_client
            .post(format!("{}/midpoints", self.base_url))
            .json(&request_data)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get batch midpoints",
            ));
        }

        let midpoints: std::collections::HashMap<String, Decimal> = response.json().await?;
        Ok(midpoints)
    }

    /// Get bid/ask/mid prices for multiple tokens in a single request
    ///
    /// This gives you the full price picture for multiple tokens at once.
    /// Much more efficient than individual calls, especially when you're tracking
    /// a portfolio or comparing multiple markets.
    ///
    /// Returns bid (best buy price), ask (best sell price), and mid (average) for each token.
    pub async fn get_prices(
        &self,
        book_params: &[crate::types::BookParams],
    ) -> Result<std::collections::HashMap<String, std::collections::HashMap<Side, Decimal>>> {
        let request_data: Vec<PriceRequest<'_>> = book_params
            .iter()
            .map(|params| PriceRequest {
                token_id: &params.token_id,
                side: params.side.as_str(),
            })
            .collect();

        let response = self
            .http_client
            .post(format!("{}/prices", self.base_url))
            .json(&request_data)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get batch prices",
            ));
        }

        let prices: std::collections::HashMap<String, std::collections::HashMap<Side, Decimal>> =
            response.json().await?;
        Ok(prices)
    }

    /// Get order book for multiple tokens (batch) - reference implementation compatible
    pub async fn get_order_books(&self, token_ids: &[String]) -> Result<Vec<OrderBookSummary>> {
        let request_data: Vec<TokenRequest<'_>> = token_ids
            .iter()
            .map(|id| TokenRequest { token_id: id })
            .collect();

        let response = self
            .http_client
            .post(format!("{}/books", self.base_url))
            .json(&request_data)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        response
            .json::<Vec<OrderBookSummary>>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Get single order by ID
    pub async fn get_order(&self, order_id: &str) -> Result<crate::types::OpenOrder> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::config("Signer not configured"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::config("API credentials not configured"))?;

        let method = Method::GET;
        let endpoint = &format!("/data/order/{}", order_id);
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let response = self
            .http_client
            .request(method, format!("{}{}", self.base_url, endpoint))
            .headers(
                headers
                    .into_iter()
                    .map(|(k, v)| (HeaderName::from_static(k), v.parse().unwrap()))
                    .collect(),
            )
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        response
            .json::<crate::types::OpenOrder>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Get last trade price for a token
    pub async fn get_last_trade_price(&self, token_id: &str) -> Result<Value> {
        let response = self
            .http_client
            .get(format!("{}/last-trade-price", self.base_url))
            .query(&[("token_id", token_id)])
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        response
            .json::<Value>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Get last trade prices for multiple tokens
    pub async fn get_last_trade_prices(&self, token_ids: &[String]) -> Result<Value> {
        let request_data: Vec<TokenRequest<'_>> = token_ids
            .iter()
            .map(|id| TokenRequest { token_id: id })
            .collect();

        let response = self
            .http_client
            .post(format!("{}/last-trades-prices", self.base_url))
            .json(&request_data)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        response
            .json::<Value>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Cancel market orders with optional filters
    pub async fn cancel_market_orders(
        &self,
        market: Option<&str>,
        asset_id: Option<&str>,
    ) -> Result<Value> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::config("Signer not configured"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::config("API credentials not configured"))?;

        let method = Method::DELETE;
        let endpoint = "/cancel-market-orders";
        let body = std::collections::HashMap::from([
            ("market", market.unwrap_or("")),
            ("asset_id", asset_id.unwrap_or("")),
        ]);
        let body_bytes = Self::serialize_json_body(&body)?;

        let headers = create_l2_headers_with_body_bytes(
            signer,
            api_creds,
            method.as_str(),
            endpoint,
            Some(&body_bytes),
        )?;

        let response = self
            .create_request_with_json_bytes(method, endpoint, headers.into_iter(), body_bytes)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        response
            .json::<Value>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Drop (delete) notifications by IDs
    pub async fn drop_notifications(&self, ids: &[String]) -> Result<Value> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::config("Signer not configured"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::config("API credentials not configured"))?;

        let method = Method::DELETE;
        let endpoint = "/notifications";
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let response = self
            .http_client
            .request(method, format!("{}{}", self.base_url, endpoint))
            .headers(
                headers
                    .into_iter()
                    .map(|(k, v)| (HeaderName::from_static(k), v.parse().unwrap()))
                    .collect(),
            )
            .query(&[("ids", ids.join(","))])
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        response
            .json::<Value>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Update balance allowance
    pub async fn update_balance_allowance(
        &self,
        params: Option<crate::types::BalanceAllowanceParams>,
    ) -> Result<Value> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::config("Signer not configured"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::config("API credentials not configured"))?;

        let mut params = params.unwrap_or_default();
        if params.signature_type.is_none() {
            params.set_signature_type(
                self.order_builder
                    .as_ref()
                    .expect("OrderBuilder not set")
                    .get_sig_type(),
            );
        }

        let query_params = params.to_query_params();

        let method = Method::GET;
        let endpoint = "/balance-allowance/update";
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let response = self
            .http_client
            .request(method, format!("{}{}", self.base_url, endpoint))
            .headers(
                headers
                    .into_iter()
                    .map(|(k, v)| (HeaderName::from_static(k), v.parse().unwrap()))
                    .collect(),
            )
            .query(&query_params)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        response
            .json::<Value>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Check if an order is scoring
    pub async fn is_order_scoring(&self, order_id: &str) -> Result<bool> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::config("Signer not configured"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::config("API credentials not configured"))?;

        let method = Method::GET;
        let endpoint = "/order-scoring";
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let response = self
            .http_client
            .request(method, format!("{}{}", self.base_url, endpoint))
            .headers(
                headers
                    .into_iter()
                    .map(|(k, v)| (HeaderName::from_static(k), v.parse().unwrap()))
                    .collect(),
            )
            .query(&[("order_id", order_id)])
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        let result: Value = response
            .json()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))?;

        Ok(result["scoring"].as_bool().unwrap_or(false))
    }

    /// Check if multiple orders are scoring
    pub async fn are_orders_scoring(
        &self,
        order_ids: &[&str],
    ) -> Result<std::collections::HashMap<String, bool>> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::config("Signer not configured"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::config("API credentials not configured"))?;

        let method = Method::POST;
        let endpoint = "/orders-scoring";
        let body_bytes = Self::serialize_json_body(order_ids)?;
        let headers = create_l2_headers_with_body_bytes(
            signer,
            api_creds,
            method.as_str(),
            endpoint,
            Some(&body_bytes),
        )?;

        let response = self
            .create_request_with_json_bytes(method, endpoint, headers.into_iter(), body_bytes)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        response
            .json::<std::collections::HashMap<String, bool>>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    // ============================================================================
    // RFQ (Market Maker) endpoints
    // ============================================================================

    /// Create an RFQ request.
    pub async fn create_rfq_request(
        &self,
        request: &crate::types::RfqCreateRequest,
    ) -> Result<crate::types::RfqCreateRequestResponse> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let method = Method::POST;
        let endpoint = "/rfq/request";
        let body_bytes = Self::serialize_json_body(request)?;
        let headers = create_l2_headers_with_body_bytes(
            signer,
            api_creds,
            method.as_str(),
            endpoint,
            Some(&body_bytes),
        )?;

        let response = self
            .create_request_with_json_bytes(method, endpoint, headers.into_iter(), body_bytes)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to create RFQ request",
            ));
        }

        response
            .json()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Cancel an RFQ request.
    pub async fn cancel_rfq_request(&self, request_id: &str) -> Result<()> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let method = Method::DELETE;
        let endpoint = "/rfq/request";
        let body = crate::types::RfqCancelRequest {
            request_id: request_id.to_string(),
        };
        let body_bytes = Self::serialize_json_body(&body)?;
        let headers = create_l2_headers_with_body_bytes(
            signer,
            api_creds,
            method.as_str(),
            endpoint,
            Some(&body_bytes),
        )?;

        let response = self
            .create_request_with_json_bytes(method, endpoint, headers.into_iter(), body_bytes)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to cancel RFQ request",
            ));
        }

        Ok(())
    }

    /// Get RFQ requests (requester).
    pub async fn get_rfq_requests(
        &self,
        params: Option<&crate::types::RfqRequestsParams>,
    ) -> Result<crate::types::RfqListResponse<crate::types::RfqRequestData>> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let method = Method::GET;
        let endpoint = "/rfq/data/requests";
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let query_params = params.cloned().unwrap_or_default().to_query_params();

        let response = self
            .create_request_with_headers(method, endpoint, headers.into_iter())
            .query(&query_params)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get RFQ requests",
            ));
        }

        response
            .json()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Create an RFQ quote.
    pub async fn create_rfq_quote(
        &self,
        quote: &crate::types::RfqCreateQuote,
    ) -> Result<crate::types::RfqCreateQuoteResponse> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let method = Method::POST;
        let endpoint = "/rfq/quote";
        let body_bytes = Self::serialize_json_body(quote)?;
        let headers = create_l2_headers_with_body_bytes(
            signer,
            api_creds,
            method.as_str(),
            endpoint,
            Some(&body_bytes),
        )?;

        let response = self
            .create_request_with_json_bytes(method, endpoint, headers.into_iter(), body_bytes)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to create RFQ quote",
            ));
        }

        response
            .json()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Cancel an RFQ quote.
    pub async fn cancel_rfq_quote(&self, quote_id: &str) -> Result<()> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let method = Method::DELETE;
        let endpoint = "/rfq/quote";
        let body = crate::types::RfqCancelQuote {
            quote_id: quote_id.to_string(),
        };
        let body_bytes = Self::serialize_json_body(&body)?;
        let headers = create_l2_headers_with_body_bytes(
            signer,
            api_creds,
            method.as_str(),
            endpoint,
            Some(&body_bytes),
        )?;

        let response = self
            .create_request_with_json_bytes(method, endpoint, headers.into_iter(), body_bytes)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to cancel RFQ quote",
            ));
        }

        Ok(())
    }

    /// Get quotes for the requester.
    pub async fn get_rfq_requester_quotes(
        &self,
        params: Option<&crate::types::RfqQuotesParams>,
    ) -> Result<crate::types::RfqListResponse<crate::types::RfqQuoteData>> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let method = Method::GET;
        let endpoint = "/rfq/data/requester/quotes";
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let query_params = params.cloned().unwrap_or_default().to_query_params();

        let response = self
            .create_request_with_headers(method, endpoint, headers.into_iter())
            .query(&query_params)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get RFQ requester quotes",
            ));
        }

        response
            .json()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Get quotes for the quoter.
    pub async fn get_rfq_quoter_quotes(
        &self,
        params: Option<&crate::types::RfqQuotesParams>,
    ) -> Result<crate::types::RfqListResponse<crate::types::RfqQuoteData>> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let method = Method::GET;
        let endpoint = "/rfq/data/quoter/quotes";
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let query_params = params.cloned().unwrap_or_default().to_query_params();

        let response = self
            .create_request_with_headers(method, endpoint, headers.into_iter())
            .query(&query_params)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get RFQ quoter quotes",
            ));
        }

        response
            .json()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Get best quote for a request.
    pub async fn get_rfq_best_quote(&self, request_id: &str) -> Result<crate::types::RfqQuoteData> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let method = Method::GET;
        let endpoint = "/rfq/data/best-quote";
        let headers =
            create_l2_headers::<Value>(signer, api_creds, method.as_str(), endpoint, None)?;

        let response = self
            .create_request_with_headers(method, endpoint, headers.into_iter())
            .query(&[("requestId", request_id)])
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to get RFQ best quote",
            ));
        }

        response
            .json()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Accept the best quote and post the resulting order.
    pub async fn accept_rfq_quote(
        &self,
        body: &crate::types::RfqOrderExecutionRequest,
    ) -> Result<()> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let method = Method::POST;
        let endpoint = "/rfq/request/accept";
        let body_bytes = Self::serialize_json_body(body)?;
        let headers = create_l2_headers_with_body_bytes(
            signer,
            api_creds,
            method.as_str(),
            endpoint,
            Some(&body_bytes),
        )?;

        let response = self
            .create_request_with_json_bytes(method, endpoint, headers.into_iter(), body_bytes)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to accept RFQ quote",
            ));
        }

        Ok(())
    }

    /// Approve the accepted quote's order (Quoter).
    pub async fn approve_rfq_order(
        &self,
        body: &crate::types::RfqOrderExecutionRequest,
    ) -> Result<crate::types::RfqApproveOrderResponse> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("Signer not set"))?;
        let api_creds = self
            .api_creds
            .as_ref()
            .ok_or_else(|| PolyfillError::auth("API credentials not set"))?;

        let method = Method::POST;
        let endpoint = "/rfq/quote/approve";
        let body_bytes = Self::serialize_json_body(body)?;
        let headers = create_l2_headers_with_body_bytes(
            signer,
            api_creds,
            method.as_str(),
            endpoint,
            Some(&body_bytes),
        )?;

        let response = self
            .create_request_with_json_bytes(method, endpoint, headers.into_iter(), body_bytes)
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        if !response.status().is_success() {
            return Err(PolyfillError::api(
                response.status().as_u16(),
                "Failed to approve RFQ order",
            ));
        }

        response
            .json()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Get sampling markets with pagination
    pub async fn get_sampling_markets(
        &self,
        next_cursor: Option<&str>,
    ) -> Result<crate::types::MarketsResponse> {
        let next_cursor = next_cursor.unwrap_or("MA=="); // INITIAL_CURSOR

        let response = self
            .http_client
            .get(format!("{}/sampling-markets", self.base_url))
            .query(&[("next_cursor", next_cursor)])
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        Self::parse_json_response(response).await
    }

    /// Get sampling simplified markets with pagination
    pub async fn get_sampling_simplified_markets(
        &self,
        next_cursor: Option<&str>,
    ) -> Result<crate::types::SimplifiedMarketsResponse> {
        let next_cursor = next_cursor.unwrap_or("MA=="); // INITIAL_CURSOR

        let response = self
            .http_client
            .get(format!("{}/sampling-simplified-markets", self.base_url))
            .query(&[("next_cursor", next_cursor)])
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        Self::parse_json_response(response).await
    }

    /// Get markets with pagination
    pub async fn get_markets(
        &self,
        next_cursor: Option<&str>,
    ) -> Result<crate::types::MarketsResponse> {
        let next_cursor = next_cursor.unwrap_or("MA=="); // INITIAL_CURSOR

        let response = self
            .http_client
            .get(format!("{}/markets", self.base_url))
            .query(&[("next_cursor", next_cursor)])
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        Self::parse_json_response(response).await
    }

    /// Get simplified markets with pagination
    pub async fn get_simplified_markets(
        &self,
        next_cursor: Option<&str>,
    ) -> Result<crate::types::SimplifiedMarketsResponse> {
        let next_cursor = next_cursor.unwrap_or("MA=="); // INITIAL_CURSOR

        let response = self
            .http_client
            .get(format!("{}/simplified-markets", self.base_url))
            .query(&[("next_cursor", next_cursor)])
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        Self::parse_json_response(response).await
    }

    /// Get single market by condition ID
    pub async fn get_market(&self, condition_id: &str) -> Result<crate::types::Market> {
        let response = self
            .http_client
            .get(format!("{}/markets/{}", self.base_url, condition_id))
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        response
            .json::<crate::types::Market>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }

    /// Get market trades events
    pub async fn get_market_trades_events(&self, condition_id: &str) -> Result<Value> {
        let response = self
            .http_client
            .get(format!(
                "{}/live-activity/events/{}",
                self.base_url, condition_id
            ))
            .send()
            .await
            .map_err(|e| PolyfillError::network(format!("Request failed: {}", e), e))?;

        response
            .json::<Value>()
            .await
            .map_err(|e| PolyfillError::parse(format!("Failed to parse response: {}", e), None))
    }
}

// Re-export types from the canonical location in types.rs
pub use crate::types::{
    CancelOrdersResponse as TypedCancelOrdersResponse, ClobMarketInfo as TypedClobMarketInfo,
    CreateOrderOptions as TypedCreateOrderOptions, Market, MarketsResponse, MidpointResponse,
    NegRiskResponse, OrderBookSummary, OrderSummary, PostOrderOptions as TypedPostOrderOptions,
    PostOrderResponse as TypedPostOrderResponse, PriceResponse, PricesHistoryInterval,
    PricesHistoryResponse, Rewards, SpreadResponse, TickSizeResponse, Token,
};

// Re-export for compatibility
pub type PolyfillClient = ClobClient;

#[cfg(test)]
mod tests {
    use super::{ClobClient, OrderArgs as ClientOrderArgs};
    use crate::types::{
        CreateOrderOptions, OrderType, PostOrderOptions, PricesHistoryInterval, RfqCreateQuote,
        RfqCreateRequest, RfqOrderExecutionRequest, RfqQuotesParams, RfqRequestsParams, Side,
        SignedOrderRequest,
    };
    use crate::{ApiCredentials, ClientConfig, PolyfillError};
    use mockito::{Matcher, Server};
    use rust_decimal::Decimal;
    use serde_json::json;
    use std::str::FromStr;
    use tokio;

    fn create_test_client(base_url: &str) -> ClobClient {
        ClobClient::new(base_url)
    }

    fn create_test_client_with_auth(base_url: &str) -> ClobClient {
        ClobClient::from_config(ClientConfig {
            base_url: base_url.to_string(),
            chain: 137,
            private_key: Some(
                "0x1234567890123456789012345678901234567890123456789012345678901234".to_string(),
            ),
            ..ClientConfig::default()
        })
        .expect("test auth client")
    }

    fn create_test_client_with_l2_auth(base_url: &str) -> ClobClient {
        let api_creds = ApiCredentials {
            api_key: "test_key".to_string(),
            // URL-safe base64 so HMAC header generation succeeds.
            secret: "dGVzdF9zZWNyZXRfa2V5XzEyMzQ1".to_string(),
            passphrase: "test_passphrase".to_string(),
        };

        ClobClient::from_config(ClientConfig {
            base_url: base_url.to_string(),
            chain: 137,
            private_key: Some(
                "0x1234567890123456789012345678901234567890123456789012345678901234".to_string(),
            ),
            api_credentials: Some(api_creds),
            ..ClientConfig::default()
        })
        .expect("test l2 auth client")
    }

    fn sample_signed_order() -> SignedOrderRequest {
        SignedOrderRequest {
            salt: 42,
            maker: "0x1111111111111111111111111111111111111111".to_string(),
            signer: "0x2222222222222222222222222222222222222222".to_string(),
            token_id: "123".to_string(),
            maker_amount: "100".to_string(),
            taker_amount: "250".to_string(),
            expiration: "1900000000".to_string(),
            side: "BUY".to_string(),
            signature_type: 0,
            timestamp: "1713916800000".to_string(),
            metadata: "0x0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
            builder: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
            signature: "0xdeadbeef".to_string(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_client_creation() {
        let client = create_test_client("https://test.example.com");
        assert_eq!(client.base_url, "https://test.example.com");
        assert!(client.signer.is_none());
        assert!(client.api_creds.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_client_from_config_with_signer() {
        let client = create_test_client_with_auth("https://test.example.com");
        assert_eq!(client.base_url, "https://test.example.com");
        assert!(client.signer.is_some());
        assert_eq!(client.chain_id, 137);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_client_from_config_with_api_credentials() {
        let api_creds = ApiCredentials {
            api_key: "test_key".to_string(),
            secret: "dGVzdF9zZWNyZXRfa2V5XzEyMzQ1".to_string(),
            passphrase: "test_passphrase".to_string(),
        };

        let client = ClobClient::from_config(ClientConfig {
            base_url: "https://test.example.com".to_string(),
            chain: 137,
            private_key: Some(
                "0x1234567890123456789012345678901234567890123456789012345678901234".to_string(),
            ),
            api_credentials: Some(api_creds.clone()),
            ..ClientConfig::default()
        })
        .expect("configured client");

        assert_eq!(client.base_url, "https://test.example.com");
        assert!(client.signer.is_some());
        assert!(client.api_creds.is_some());
        assert_eq!(client.chain_id, 137);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_set_api_creds() {
        let mut client = create_test_client("https://test.example.com");
        assert!(client.api_creds.is_none());

        let api_creds = ApiCredentials {
            api_key: "test_key".to_string(),
            secret: "dGVzdF9zZWNyZXRfa2V5XzEyMzQ1".to_string(),
            passphrase: "test_passphrase".to_string(),
        };

        client.set_api_creds(api_creds.clone()).unwrap();
        assert!(client.api_creds.is_some());
        assert_eq!(client.api_creds.unwrap().api_key, "test_key");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_from_config_rejects_invalid_api_secret() {
        let api_creds = ApiCredentials {
            api_key: "test_key".to_string(),
            secret: "not valid base64!".to_string(),
            passphrase: "test_passphrase".to_string(),
        };

        let err = match ClobClient::from_config(ClientConfig {
            base_url: "https://test.example.com".to_string(),
            chain: 137,
            private_key: Some(
                "0x1234567890123456789012345678901234567890123456789012345678901234".to_string(),
            ),
            api_credentials: Some(api_creds),
            ..ClientConfig::default()
        }) {
            Ok(_) => panic!("expected invalid API credentials to fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("Failed to decode base64 secret"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_set_api_creds_rejects_invalid_api_secret() {
        let mut client = create_test_client("https://test.example.com");
        let api_creds = ApiCredentials {
            api_key: "test_key".to_string(),
            secret: "not valid base64!".to_string(),
            passphrase: "test_passphrase".to_string(),
        };

        let err = client.set_api_creds(api_creds).unwrap_err();

        assert!(client.api_creds.is_none());
        assert!(err.to_string().contains("Failed to decode base64 secret"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_sampling_markets_success() {
        let mut server = Server::new_async().await;
        let mock_response = r#"{
            "limit": 10,
            "count": 2, 
            "next_cursor": null,
            "data": [
                {
                    "condition_id": "0x123",
                    "tokens": [
                        {"token_id": "0x456", "outcome": "Yes", "price": 0.5, "winner": false},
                        {"token_id": "0x789", "outcome": "No", "price": 0.5, "winner": false}
                    ],
                    "rewards": {
                        "rates": null,
                        "min_size": 1.0,
                        "max_spread": 0.1,
                        "event_start_date": null,
                        "event_end_date": null,
                        "in_game_multiplier": null,
                        "reward_epoch": null
                    },
                    "min_incentive_size": null,
                    "max_incentive_spread": null,
                    "active": true,
                    "closed": false,
                    "question_id": "0x123",
                    "minimum_order_size": 1.0,
                    "minimum_tick_size": 0.01,
                    "description": "Test market",
                    "category": "test",
                    "end_date_iso": null,
                    "game_start_time": null,
                    "question": "Will this test pass?",
                    "market_slug": "test-market",
                    "seconds_delay": 0,
                    "icon": "",
                    "fpmm": ""
                }
            ]
        }"#;

        let mock = server
            .mock("GET", "/sampling-markets")
            .match_query(Matcher::UrlEncoded("next_cursor".into(), "MA==".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let result = client.get_sampling_markets(None).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let markets = result.unwrap();
        assert_eq!(markets.data.len(), 1);
        assert_eq!(markets.data[0].question, "Will this test pass?");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_sampling_markets_with_cursor() {
        let mut server = Server::new_async().await;
        let mock_response = r#"{
            "limit": 5,
            "count": 0,
            "next_cursor": null,
            "data": []
        }"#;

        let mock = server
            .mock("GET", "/sampling-markets")
            .match_query(Matcher::AllOf(vec![Matcher::UrlEncoded(
                "next_cursor".into(),
                "test_cursor".into(),
            )]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let result = client.get_sampling_markets(Some("test_cursor")).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let markets = result.unwrap();
        assert_eq!(markets.data.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_order_book_success() {
        let mut server = Server::new_async().await;
        let mock_response = r#"{
            "market": "0x123",
            "asset_id": "0x123",
            "hash": "0xabc123",
            "timestamp": "1234567890",
            "bids": [
                {"price": "0.75", "size": "100.0"}
            ],
            "asks": [
                {"price": "0.76", "size": "50.0"}
            ],
            "min_order_size": "1",
            "neg_risk": false,
            "tick_size": "0.01",
            "last_trade_price": "0.755"
        }"#;

        let mock = server
            .mock("GET", "/book")
            .match_query(Matcher::UrlEncoded("token_id".into(), "0x123".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let result = client.get_order_book("0x123").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let book = result.unwrap();
        assert_eq!(book.market, "0x123");
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.asks.len(), 1);
        assert_eq!(book.min_order_size, Decimal::from_str("1").unwrap());
        assert!(!book.neg_risk);
        assert_eq!(book.tick_size, Decimal::from_str("0.01").unwrap());
        assert_eq!(
            book.last_trade_price,
            Some(Decimal::from_str("0.755").unwrap())
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_midpoint_success() {
        let mut server = Server::new_async().await;
        let mock_response = r#"{
            "mid": "0.755"
        }"#;

        let mock = server
            .mock("GET", "/midpoint")
            .match_query(Matcher::UrlEncoded("token_id".into(), "0x123".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let result = client.get_midpoint("0x123").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.mid, Decimal::from_str("0.755").unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_spread_success() {
        let mut server = Server::new_async().await;
        let mock_response = r#"{
            "spread": "0.01"
        }"#;

        let mock = server
            .mock("GET", "/spread")
            .match_query(Matcher::UrlEncoded("token_id".into(), "0x123".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let result = client.get_spread("0x123").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.spread, Decimal::from_str("0.01").unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_price_success() {
        let mut server = Server::new_async().await;
        let mock_response = r#"{
            "price": "0.76"
        }"#;

        let mock = server
            .mock("GET", "/price")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("token_id".into(), "0x123".into()),
                Matcher::UrlEncoded("side".into(), "BUY".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let result = client.get_price("0x123", Side::BUY).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.price, Decimal::from_str("0.76").unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_prices_history_interval_rejects_hex_condition_id() {
        let client = create_test_client("https://test.example.com");
        let result = client
            .get_prices_history_interval("0xdeadbeef", PricesHistoryInterval::OneDay, None)
            .await;
        assert!(matches!(result, Err(PolyfillError::Validation { .. })));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_prices_history_interval_success() {
        let mut server = Server::new_async().await;
        let mock_response = r#"{"history":[{"t":1}]}"#;

        let mock = server
            .mock("GET", "/prices-history")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("market".into(), "12345".into()),
                Matcher::UrlEncoded("interval".into(), "1d".into()),
                Matcher::UrlEncoded("fidelity".into(), "5".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let response = client
            .get_prices_history_interval("12345", PricesHistoryInterval::OneDay, Some(5))
            .await
            .unwrap();

        mock.assert_async().await;
        assert_eq!(response.history.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_tick_size_success() {
        let mut server = Server::new_async().await;
        let mock_response = r#"{
            "minimum_tick_size": "0.01"
        }"#;

        let mock = server
            .mock("GET", "/tick-size")
            .match_query(Matcher::UrlEncoded("token_id".into(), "0x123".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let result = client.get_tick_size("0x123").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let tick_size = result.unwrap();
        assert_eq!(tick_size, Decimal::from_str("0.01").unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_neg_risk_success() {
        let mut server = Server::new_async().await;
        let mock_response = r#"{
            "neg_risk": false
        }"#;

        let mock = server
            .mock("GET", "/neg-risk")
            .match_query(Matcher::UrlEncoded("token_id".into(), "0x123".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let result = client.get_neg_risk("0x123").await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let neg_risk = result.unwrap();
        assert!(!neg_risk);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_api_error_handling() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/book")
            .match_query(Matcher::UrlEncoded(
                "token_id".into(),
                "invalid_token".into(),
            ))
            .with_status(404)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error": "Market not found"}"#)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let result = client.get_order_book("invalid_token").await;

        mock.assert_async().await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        // The error should be either Network or Api error
        assert!(
            matches!(error, PolyfillError::Network { .. })
                || matches!(error, PolyfillError::Api { .. })
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_network_error_handling() {
        // Test with invalid URL to simulate network error
        let client = create_test_client("http://invalid-host-that-does-not-exist.com");
        let result = client.get_order_book("0x123").await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(error, PolyfillError::Network { .. }));
    }

    #[test]
    fn test_client_url_validation() {
        let client = create_test_client("https://test.example.com");
        assert_eq!(client.base_url, "https://test.example.com");

        let client2 = create_test_client("http://localhost:8080");
        assert_eq!(client2.base_url, "http://localhost:8080");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_midpoints_batch() {
        let mut server = Server::new_async().await;
        let mock_response = r#"{
            "0x123": "0.755",
            "0x456": "0.623"
        }"#;

        let mock = server
            .mock("POST", "/midpoints")
            .match_body(Matcher::JsonString(
                r#"[{"token_id":"0x123"},{"token_id":"0x456"}]"#.to_string(),
            ))
            .with_header("content-type", "application/json")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let token_ids = vec!["0x123".to_string(), "0x456".to_string()];
        let result = client.get_midpoints(&token_ids).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let midpoints = result.unwrap();
        assert_eq!(midpoints.len(), 2);
        assert_eq!(
            midpoints.get("0x123").unwrap(),
            &Decimal::from_str("0.755").unwrap()
        );
        assert_eq!(
            midpoints.get("0x456").unwrap(),
            &Decimal::from_str("0.623").unwrap()
        );
    }

    #[test]
    fn test_client_configuration() {
        let client = create_test_client("https://test.example.com");

        // Test initial state
        assert!(client.signer.is_none());
        assert!(client.api_creds.is_none());

        // Test with auth
        let auth_client = create_test_client_with_auth("https://test.example.com");
        assert!(auth_client.signer.is_some());
        assert_eq!(auth_client.chain_id, 137);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_ok() {
        let mut server = Server::new_async().await;
        let mock_response = r#"{"status": "ok"}"#;

        let mock = server
            .mock("GET", "/ok")
            .with_header("content-type", "application/json")
            .with_status(200)
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let result = client.get_ok().await;

        mock.assert_async().await;
        assert!(result);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_prices_batch() {
        let mut server = Server::new_async().await;
        let mock_response = r#"{
            "0x123": {
                "BUY": "0.755",
                "SELL": "0.745"
            },
            "0x456": {
                "BUY": "0.623",
                "SELL": "0.613"
            }
        }"#;

        let mock = server
            .mock("POST", "/prices")
            .match_body(Matcher::JsonString(
                r#"[{"token_id":"0x123","side":"BUY"},{"token_id":"0x456","side":"SELL"}]"#
                    .to_string(),
            ))
            .with_header("content-type", "application/json")
            .with_status(200)
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let book_params = vec![
            crate::types::BookParams {
                token_id: "0x123".to_string(),
                side: Side::BUY,
            },
            crate::types::BookParams {
                token_id: "0x456".to_string(),
                side: Side::SELL,
            },
        ];
        let result = client.get_prices(&book_params).await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let prices = result.unwrap();
        assert_eq!(prices.len(), 2);
        assert!(prices.contains_key("0x123"));
        assert!(prices.contains_key("0x456"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_server_time() {
        let mut server = Server::new_async().await;
        let mock_response = "1234567890"; // Plain text response

        let mock = server
            .mock("GET", "/time")
            .with_status(200)
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let result = client.get_server_time().await;

        mock.assert_async().await;
        assert!(result.is_ok());
        let timestamp = result.unwrap();
        assert_eq!(timestamp, 1234567890);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_or_derive_api_key() {
        let mut server = Server::new_async().await;
        let mock_response = r#"{
            "apiKey": "test-api-key-123",
            "secret": "test-secret-456",
            "passphrase": "test-passphrase"
        }"#;

        // Mock both create and derive endpoints since the method tries both
        let create_mock = server
            .mock("POST", "/auth/api-key")
            .with_header("content-type", "application/json")
            .with_status(200)
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client_with_auth(&server.url());
        let result = client.create_or_derive_api_key(None).await;

        create_mock.assert_async().await;
        assert!(result.is_ok());
        let api_creds = result.unwrap();
        assert_eq!(api_creds.api_key, "test-api-key-123");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_or_derive_api_key_falls_back_on_api_error() {
        let mut server = Server::new_async().await;

        // Create fails with a status error -> should fall back to derive.
        let create_mock = server
            .mock("POST", "/auth/api-key")
            .with_status(400)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"key exists"}"#)
            .create_async()
            .await;

        let derive_mock = server
            .mock("GET", "/auth/derive-api-key")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"apiKey":"derived-api-key","secret":"derived-secret","passphrase":"derived-pass"}"#,
            )
            .create_async()
            .await;

        let client = create_test_client_with_auth(&server.url());
        let result = client.create_or_derive_api_key(None).await;

        create_mock.assert_async().await;
        derive_mock.assert_async().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().api_key, "derived-api-key");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_or_derive_api_key_does_not_fallback_on_non_api_error() {
        let mut server = Server::new_async().await;

        // Create returns 200 but with invalid JSON -> not an API status error.
        let create_mock = server
            .mock("POST", "/auth/api-key")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("not-json")
            .create_async()
            .await;

        // If we incorrectly fall back, this would be called.
        let derive_mock = server
            .mock("GET", "/auth/derive-api-key")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"apiKey":"derived-api-key","secret":"derived-secret","passphrase":"derived-pass"}"#,
            )
            .expect(0)
            .create_async()
            .await;

        let client = create_test_client_with_auth(&server.url());
        let result = client.create_or_derive_api_key(None).await;

        create_mock.assert_async().await;
        derive_mock.assert_async().await;
        assert!(result.is_err());
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_order_books_batch() {
        let mut server = Server::new_async().await;
        let mock_response = r#"[
            {
                "market": "0x123",
                "asset_id": "0x123",
                "hash": "test-hash",
                "timestamp": "1234567890",
                "bids": [{"price": "0.75", "size": "100.0"}],
                "asks": [{"price": "0.76", "size": "50.0"}],
                "min_order_size": "1",
                "neg_risk": false,
                "tick_size": "0.01",
                "last_trade_price": null
            }
        ]"#;

        let mock = server
            .mock("POST", "/books")
            .match_body(Matcher::JsonString(r#"[{"token_id":"0x123"}]"#.to_string()))
            .with_header("content-type", "application/json")
            .with_status(200)
            .with_body(mock_response)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let token_ids = vec!["0x123".to_string()];
        let result = client.get_order_books(&token_ids).await;

        mock.assert_async().await;
        if let Err(e) = &result {
            println!("Error: {:?}", e);
        }
        assert!(result.is_ok());
        let books = result.unwrap();
        assert_eq!(books.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_order_args_creation() {
        // Test OrderArgs creation and default values
        let order_args = ClientOrderArgs::new(
            "0x123",
            Decimal::from_str("0.75").unwrap(),
            Decimal::from_str("100.0").unwrap(),
            Side::BUY,
        );

        assert_eq!(order_args.token_id, "0x123");
        assert_eq!(order_args.price, Decimal::from_str("0.75").unwrap());
        assert_eq!(order_args.size, Decimal::from_str("100.0").unwrap());
        assert_eq!(order_args.side, Side::BUY);

        // Test default
        let default_args = ClientOrderArgs::default();
        assert_eq!(default_args.token_id, "");
        assert_eq!(default_args.price, Decimal::ZERO);
        assert_eq!(default_args.size, Decimal::ZERO);
        assert_eq!(default_args.side, Side::BUY);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_prepare_order_path_uses_client_builder_code() {
        let mut server = Server::new_async().await;
        let tick_size_mock = server
            .mock("GET", "/tick-size")
            .match_query(Matcher::UrlEncoded("token_id".into(), "123456".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"minimum_tick_size":"0.01"}"#)
            .create_async()
            .await;

        let client = ClobClient::from_config(ClientConfig {
            base_url: server.url(),
            chain: 137,
            private_key: Some(
                "0x1234567890123456789012345678901234567890123456789012345678901234".to_string(),
            ),
            builder_code: Some(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            ),
            ..ClientConfig::default()
        })
        .expect("test auth client with builder code");
        let options = CreateOrderOptions {
            tick_size: Some(Decimal::from_str("0.01").unwrap()),
            neg_risk: Some(false),
        };

        let prepared = client
            .prepare_order_path("123456", Some(&options), None, None)
            .await
            .unwrap();
        let order = prepared
            .create_limit_order(
                Side::BUY,
                Decimal::from_str("0.45").unwrap(),
                Decimal::from_str("12.34").unwrap(),
                Some(1_900_000_000),
            )
            .unwrap();

        tick_size_mock.assert_async().await;
        assert_eq!(order.token_id, "123456");
        assert_eq!(
            order.builder,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(order.metadata, crate::orders::BYTES32_ZERO);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_clob_market_info_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/clob-markets/condition-1")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "c":"0x1111111111111111111111111111111111111111111111111111111111111111",
                    "gst":"ready",
                    "t":[{"t":"123","o":"YES"},{"t":"456","o":"NO"}],
                    "mos":"5",
                    "mts":"0.01",
                    "rfqe":true,
                    "itode":false,
                    "ibce":false,
                    "nr":false,
                    "fd":{"r":"0.01","e":2,"to":false},
                    "oas":"3600"
                }"#,
            )
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let info = client.get_clob_market_info("condition-1").await.unwrap();

        mock.assert_async().await;
        assert_eq!(
            info.c.as_deref(),
            Some("0x1111111111111111111111111111111111111111111111111111111111111111")
        );
        assert_eq!(info.t.len(), 2);
        assert_eq!(info.mos, Decimal::from_str("5").unwrap());
        assert_eq!(info.mts, Decimal::from_str("0.01").unwrap());
        assert_eq!(info.tbf, Decimal::ZERO);
        assert_eq!(info.fd.unwrap().e, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_builder_fee_rate_uses_v2_endpoint() {
        let mut server = Server::new_async().await;
        let builder_code = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mock = server
            .mock("GET", format!("/fees/builder-fees/{builder_code}").as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "builder_maker_fee_rate_bps": 5,
                    "builder_taker_fee_rate_bps": 12
                }"#,
            )
            .create_async()
            .await;

        let client = create_test_client_with_l2_auth(&server.url());
        let response = client.get_builder_fee_rate(builder_code).await.unwrap();

        mock.assert_async().await;
        assert_eq!(response.builder_maker_fee_rate_bps, 5);
        assert_eq!(response.builder_taker_fee_rate_bps, 12);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_post_order_uses_v2_wire_shape_and_typed_response() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/order")
            .match_body(Matcher::JsonString(
                json!({
                    "order": {
                        "salt": 42,
                        "maker": "0x1111111111111111111111111111111111111111",
                        "signer": "0x2222222222222222222222222222222222222222",
                        "tokenId": "123",
                        "makerAmount": "100",
                        "takerAmount": "250",
                        "expiration": "1900000000",
                        "side": "BUY",
                        "signatureType": 0,
                        "timestamp": "1713916800000",
                        "metadata": "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "builder": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "signature": "0xdeadbeef"
                    },
                    "owner": "test_key",
                    "orderType": "GTD",
                    "postOnly": true,
                    "deferExec": true
                })
                .to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "success":true,
                    "orderID":"order-1",
                    "status":"live",
                    "makingAmount":"100",
                    "takingAmount":"250",
                    "transactionsHashes":["0xabc"],
                    "tradeIds":["trade-1"],
                    "errorMsg":""
                }"#,
            )
            .create_async()
            .await;

        let client = create_test_client_with_l2_auth(&server.url());
        let response = client
            .post_order(
                sample_signed_order(),
                Some(&PostOrderOptions {
                    order_type: OrderType::GTD,
                    post_only: true,
                    defer_exec: true,
                }),
            )
            .await
            .unwrap();

        mock.assert_async().await;
        assert!(response.success);
        assert_eq!(response.order_id, "order-1");
        assert_eq!(response.status, "live");
        assert_eq!(response.transactions_hashes, vec!["0xabc".to_string()]);
        assert_eq!(response.trade_ids, vec!["trade-1".to_string()]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_post_order_rejects_post_only_for_fak() {
        let client = create_test_client_with_l2_auth("https://test.example.com");
        let err = client
            .post_order(
                sample_signed_order(),
                Some(&PostOrderOptions {
                    order_type: OrderType::FAK,
                    post_only: true,
                    defer_exec: false,
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, PolyfillError::Validation { .. }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_post_order_rejects_expiration_for_non_gtd() {
        let client = create_test_client_with_l2_auth("https://test.example.com");
        let err = client
            .post_order(
                sample_signed_order(),
                Some(&PostOrderOptions {
                    order_type: OrderType::GTC,
                    post_only: false,
                    defer_exec: false,
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, PolyfillError::Validation { .. }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_cancel_endpoints_parse_typed_responses() {
        let mut server = Server::new_async().await;
        let cancel_mock = server
            .mock("DELETE", "/order")
            .match_body(Matcher::JsonString(r#"{"orderID":"order-1"}"#.to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"canceled":["order-1"],"notCanceled":{}}"#)
            .create_async()
            .await;
        let cancel_orders_mock = server
            .mock("DELETE", "/orders")
            .match_body(Matcher::JsonString(r#"["order-1","order-2"]"#.to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"canceled":["order-1"],"notCanceled":{"order-2":"already filled"}}"#)
            .create_async()
            .await;
        let cancel_all_mock = server
            .mock("DELETE", "/cancel-all")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"canceled":["order-9"],"notCanceled":{}}"#)
            .create_async()
            .await;

        let client = create_test_client_with_l2_auth(&server.url());
        let cancel = client.cancel("order-1").await.unwrap();
        let cancel_many = client
            .cancel_orders(&["order-1".to_string(), "order-2".to_string()])
            .await
            .unwrap();
        let cancel_all = client.cancel_all().await.unwrap();

        cancel_mock.assert_async().await;
        cancel_orders_mock.assert_async().await;
        cancel_all_mock.assert_async().await;
        assert_eq!(cancel.canceled, vec!["order-1".to_string()]);
        assert_eq!(
            cancel_many.not_canceled.get("order-2"),
            Some(&"already filled".to_string())
        );
        assert_eq!(cancel_all.canceled, vec!["order-9".to_string()]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_orders_parses_typed_pages() {
        let mut server = Server::new_async().await;
        let first_page = server
            .mock("GET", "/data/orders")
            .match_query(Matcher::UrlEncoded("next_cursor".into(), "MA==".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "data": [{
                        "associate_trades": ["trade-1"],
                        "id": "order-1",
                        "status": "LIVE",
                        "market": "market-1",
                        "original_size": "10",
                        "outcome": "Yes",
                        "maker_address": "0x1111111111111111111111111111111111111111",
                        "owner": "0x2222222222222222222222222222222222222222",
                        "price": "0.55",
                        "side": "BUY",
                        "size_matched": "1.5",
                        "asset_id": "asset-1",
                        "expiration": "0",
                        "type": "GTC",
                        "created_at": "1713916800"
                    }],
                    "next_cursor": "CURSOR1"
                }"#,
            )
            .create_async()
            .await;
        let second_page = server
            .mock("GET", "/data/orders")
            .match_query(Matcher::UrlEncoded("next_cursor".into(), "CURSOR1".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "data": [{
                        "associate_trades": [],
                        "id": "order-2",
                        "status": "LIVE",
                        "market": "market-1",
                        "original_size": "20",
                        "outcome": "No",
                        "maker_address": "0x1111111111111111111111111111111111111111",
                        "owner": "0x2222222222222222222222222222222222222222",
                        "price": "0.45",
                        "side": "SELL",
                        "size_matched": "0",
                        "asset_id": "asset-2",
                        "expiration": "0",
                        "type": "GTC",
                        "created_at": "1713916810"
                    }],
                    "next_cursor": "LTE="
                }"#,
            )
            .create_async()
            .await;

        let client = create_test_client_with_l2_auth(&server.url());
        let orders = client.get_orders(None, None).await.unwrap();

        first_page.assert_async().await;
        second_page.assert_async().await;
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].id, "order-1");
        assert_eq!(orders[1].id, "order-2");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_trades_flattens_paginated_data() {
        let mut server = Server::new_async().await;
        let first_page = server
            .mock("GET", "/data/trades")
            .match_query(Matcher::UrlEncoded("next_cursor".into(), "MA==".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "data": [{"id": "trade-1", "price": "0.55"}],
                    "next_cursor": "CURSOR1"
                }"#,
            )
            .create_async()
            .await;
        let second_page = server
            .mock("GET", "/data/trades")
            .match_query(Matcher::UrlEncoded("next_cursor".into(), "CURSOR1".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "data": [
                        {"id": "trade-2", "price": "0.45"},
                        {"id": "trade-3", "price": "0.60"}
                    ],
                    "next_cursor": "LTE="
                }"#,
            )
            .create_async()
            .await;

        let client = create_test_client_with_l2_auth(&server.url());
        let trades = client.get_trades(None, None).await.unwrap();

        first_page.assert_async().await;
        second_page.assert_async().await;
        assert_eq!(trades.len(), 3);
        assert_eq!(trades[0]["id"], "trade-1");
        assert_eq!(trades[1]["id"], "trade-2");
        assert_eq!(trades[2]["id"], "trade-3");
        assert!(trades.iter().all(|trade| trade.is_object()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_fee_rate_bps_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/fee-rate")
            .match_query(Matcher::UrlEncoded("token_id".into(), "123".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"base_fee":1000}"#)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let rate = client.get_fee_rate_bps("123").await.unwrap();

        mock.assert_async().await;
        assert_eq!(rate, 1000);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rfq_endpoints_happy_path() {
        let mut server = Server::new_async().await;

        // create_rfq_request
        let create_request = RfqCreateRequest {
            asset_in: "some_asset_in".to_string(),
            asset_out: "some_asset_out".to_string(),
            amount_in: "100".to_string(),
            amount_out: "200".to_string(),
            user_type: 0,
        };
        let create_request_mock = server
            .mock("POST", "/rfq/request")
            .match_body(Matcher::JsonString(
                json!({
                    "assetIn": "some_asset_in",
                    "assetOut": "some_asset_out",
                    "amountIn": "100",
                    "amountOut": "200",
                    "userType": 0
                })
                .to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"requestId":"req123","expiry":1744936318}"#)
            .create_async()
            .await;

        // cancel_rfq_request
        let cancel_request_mock = server
            .mock("DELETE", "/rfq/request")
            .match_body(Matcher::JsonString(r#"{"requestId":"req123"}"#.to_string()))
            .with_status(200)
            .with_body("OK")
            .create_async()
            .await;

        // get_rfq_requests
        let rfq_requests_mock = server
            .mock("GET", "/rfq/data/requests")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("offset".into(), "MA==".into()),
                Matcher::UrlEncoded("limit".into(), "10".into()),
                Matcher::UrlEncoded("state".into(), "active".into()),
                Matcher::UrlEncoded("requestIds[]".into(), "req123".into()),
                Matcher::UrlEncoded("markets[]".into(), "some_market".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "data": [{
                        "requestId": "req123",
                        "userAddress": "0xabc",
                        "proxyAddress": "0xdef",
                        "condition": "some_condition_id",
                        "token": "some_token_id",
                        "complement": "some_complement",
                        "side": "BUY",
                        "sizeIn": 100,
                        "sizeOut": 200,
                        "price": 0.5,
                        "state": "active",
                        "expiry": 1744936318
                    }],
                    "next_cursor": "MA==",
                    "limit": 10,
                    "count": 1
                }"#,
            )
            .create_async()
            .await;

        // create_rfq_quote
        let create_quote = RfqCreateQuote {
            request_id: "req123".to_string(),
            asset_in: "some_asset_in".to_string(),
            asset_out: "some_asset_out".to_string(),
            amount_in: "100".to_string(),
            amount_out: "200".to_string(),
            user_type: 0,
        };
        let create_quote_mock = server
            .mock("POST", "/rfq/quote")
            .match_body(Matcher::JsonString(
                json!({
                    "requestId": "req123",
                    "assetIn": "some_asset_in",
                    "assetOut": "some_asset_out",
                    "amountIn": "100",
                    "amountOut": "200",
                    "userType": 0
                })
                .to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"quoteId":"q123"}"#)
            .create_async()
            .await;

        // cancel_rfq_quote
        let cancel_quote_mock = server
            .mock("DELETE", "/rfq/quote")
            .match_body(Matcher::JsonString(r#"{"quoteId":"q123"}"#.to_string()))
            .with_status(200)
            .with_body("OK")
            .create_async()
            .await;

        // get_rfq_requester_quotes
        let requester_quotes_mock = server
            .mock("GET", "/rfq/data/requester/quotes")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("offset".into(), "MA==".into()),
                Matcher::UrlEncoded("limit".into(), "10".into()),
                Matcher::UrlEncoded("state".into(), "active".into()),
                Matcher::UrlEncoded("quoteIds[]".into(), "q123".into()),
                Matcher::UrlEncoded("requestIds[]".into(), "req123".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "data": [{
                        "quoteId": "q123",
                        "requestId": "req123",
                        "userAddress": "0xabc",
                        "proxyAddress": "0xdef",
                        "condition": "some_condition_id",
                        "token": "some_token_id",
                        "complement": "some_complement",
                        "side": "BUY",
                        "sizeIn": 100,
                        "sizeOut": 200,
                        "price": 0.5,
                        "matchType": "matched",
                        "state": "active"
                    }],
                    "next_cursor": "MA==",
                    "limit": 10,
                    "count": 1
                }"#,
            )
            .create_async()
            .await;

        // get_rfq_quoter_quotes
        let quoter_quotes_mock = server
            .mock("GET", "/rfq/data/quoter/quotes")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "data": [],
                    "next_cursor": "MA==",
                    "limit": 10,
                    "count": 0
                }"#,
            )
            .create_async()
            .await;

        // get_rfq_best_quote
        let best_quote_mock = server
            .mock("GET", "/rfq/data/best-quote")
            .match_query(Matcher::UrlEncoded("requestId".into(), "req123".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "quoteId": "q123",
                    "requestId": "req123",
                    "userAddress": "0xabc",
                    "proxyAddress": "0xdef",
                    "condition": "some_condition_id",
                    "token": "some_token_id",
                    "complement": "some_complement",
                    "side": "BUY",
                    "sizeIn": 100,
                    "sizeOut": 200,
                    "price": 0.5,
                    "matchType": "matched",
                    "state": "active"
                }"#,
            )
            .create_async()
            .await;

        // accept_rfq_quote
        let exec = RfqOrderExecutionRequest {
            request_id: "req123".to_string(),
            quote_id: "q123".to_string(),
            maker: "0xmaker".to_string(),
            signer: "0xsigner".to_string(),
            taker: "0xtaker".to_string(),
            expiration: 1_740_000_000,
            nonce: "123".to_string(),
            fee_rate_bps: "1000".to_string(),
            side: "BUY".to_string(),
            token_id: "123".to_string(),
            maker_amount: "100".to_string(),
            taker_amount: "200".to_string(),
            signature_type: 2,
            signature: "0xsig".to_string(),
            salt: 42,
            owner: "owner".to_string(),
        };

        let accept_mock = server
            .mock("POST", "/rfq/request/accept")
            .match_body(Matcher::JsonString(
                json!({
                    "requestId": "req123",
                    "quoteId": "q123",
                    "maker": "0xmaker",
                    "signer": "0xsigner",
                    "taker": "0xtaker",
                    "expiration": 1740000000,
                    "nonce": "123",
                    "feeRateBps": "1000",
                    "side": "BUY",
                    "tokenId": "123",
                    "makerAmount": "100",
                    "takerAmount": "200",
                    "signatureType": 2,
                    "signature": "0xsig",
                    "salt": 42,
                    "owner": "owner"
                })
                .to_string(),
            ))
            .with_status(200)
            .with_body("OK")
            .create_async()
            .await;

        // approve_rfq_order
        let approve_mock = server
            .mock("POST", "/rfq/quote/approve")
            .match_body(Matcher::JsonString(
                json!({
                    "requestId": "req123",
                    "quoteId": "q123",
                    "maker": "0xmaker",
                    "signer": "0xsigner",
                    "taker": "0xtaker",
                    "expiration": 1740000000,
                    "nonce": "123",
                    "feeRateBps": "1000",
                    "side": "BUY",
                    "tokenId": "123",
                    "makerAmount": "100",
                    "takerAmount": "200",
                    "signatureType": 2,
                    "signature": "0xsig",
                    "salt": 42,
                    "owner": "owner"
                })
                .to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"tradeIds":["t1","t2"]}"#)
            .create_async()
            .await;

        let client = create_test_client_with_l2_auth(&server.url());

        let created = client.create_rfq_request(&create_request).await.unwrap();
        assert_eq!(created.request_id, "req123");
        assert_eq!(created.expiry, 1_744_936_318);
        create_request_mock.assert_async().await;

        client.cancel_rfq_request("req123").await.unwrap();
        cancel_request_mock.assert_async().await;

        let params = RfqRequestsParams {
            offset: Some("MA==".to_string()),
            limit: Some(10),
            state: Some("active".to_string()),
            request_ids: vec!["req123".to_string()],
            markets: vec!["some_market".to_string()],
            ..Default::default()
        };
        let requests = client.get_rfq_requests(Some(&params)).await.unwrap();
        assert_eq!(requests.data.len(), 1);
        assert_eq!(requests.data[0].request_id, "req123");
        rfq_requests_mock.assert_async().await;

        let quote = client.create_rfq_quote(&create_quote).await.unwrap();
        assert_eq!(quote.quote_id, "q123");
        create_quote_mock.assert_async().await;

        client.cancel_rfq_quote("q123").await.unwrap();
        cancel_quote_mock.assert_async().await;

        let quote_params = RfqQuotesParams {
            offset: Some("MA==".to_string()),
            limit: Some(10),
            state: Some("active".to_string()),
            quote_ids: vec!["q123".to_string()],
            request_ids: vec!["req123".to_string()],
            ..Default::default()
        };

        let requester_quotes = client
            .get_rfq_requester_quotes(Some(&quote_params))
            .await
            .unwrap();
        assert_eq!(requester_quotes.data.len(), 1);
        requester_quotes_mock.assert_async().await;

        let quoter_quotes = client.get_rfq_quoter_quotes(None).await.unwrap();
        assert_eq!(quoter_quotes.data.len(), 0);
        quoter_quotes_mock.assert_async().await;

        let best = client.get_rfq_best_quote("req123").await.unwrap();
        assert_eq!(best.quote_id, "q123");
        best_quote_mock.assert_async().await;

        client.accept_rfq_quote(&exec).await.unwrap();
        accept_mock.assert_async().await;

        let approved = client.approve_rfq_order(&exec).await.unwrap();
        assert_eq!(approved.trade_ids, vec!["t1".to_string(), "t2".to_string()]);
        approve_mock.assert_async().await;
    }
}
