//! Live authenticated submit-path benchmark against Polymarket CLOB.
//!
//! This benchmark intentionally uses an example binary instead of Criterion so
//! live request counts stay explicit and rate-limit/order-placement behavior is
//! predictable.
//!
//! Safe authenticated reads:
//! `cargo run --release --example live_submit_path_benchmark --features official-client-benchmark`
//!
//! Optional live order submit, followed by best-effort cancel on success:
//! `POLYMARKET_BENCH_LIVE_POST_ORDER=1 POLYMARKET_BENCH_TOKEN_ID=... cargo run --release --example live_submit_path_benchmark --features official-client-benchmark`

use std::str::FromStr;
use std::time::{Duration, Instant};

use alloy_signer_local::PrivateKeySigner;
use polyfill_rs::{
    types::{ApiCredentials, ClientConfig, CreateOrderOptions, OrderType, PostOrderOptions},
    ClobClient, OrderArgs, Side,
};
use rust_decimal::Decimal;

use polymarket_client_sdk_v2::{
    auth::{
        state::Authenticated as OfficialAuthenticated, Credentials as OfficialCredentials,
        LocalSigner, Normal, Signer, Uuid,
    },
    clob::{
        types::{
            request::OrdersRequest as OfficialOrdersRequest, OrderType as OfficialOrderType,
            Side as OfficialSide, SignatureType as OfficialSignatureType, TickSize,
        },
        Client as OfficialClient, Config as OfficialConfig,
    },
    types::{Address as OfficialAddress, Decimal as OfficialDecimal, U256},
    POLYGON,
};

type BenchResult<T> = Result<T, Box<dyn std::error::Error>>;

const OFFICIAL_SDK_REV: &str = "8ba5008733c3c03e92041eef8b1cb8495dbed718";

#[derive(Debug, Clone)]
struct LiveConfig {
    host: String,
    chain_id: u64,
    private_key: String,
    api_credentials: Option<ApiCredentials>,
    signature_type: Option<u8>,
    funder: Option<String>,
    iterations: usize,
    warmups: usize,
    delay: Duration,
    live_post_order: bool,
    live_post_iterations: usize,
    token_id: Option<String>,
    min_midpoint: Decimal,
    price: Decimal,
    size: Decimal,
    side: Side,
    tick_size: Decimal,
    neg_risk: bool,
    derive_api_credentials: bool,
}

#[derive(Debug, Clone, Copy)]
struct Stats {
    mean_ms: f64,
    std_dev_ms: f64,
    min_ms: f64,
    max_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn env_required(name: &str) -> BenchResult<String> {
    env_string(name).ok_or_else(|| format!("{name} must be set").into())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

fn env_decimal(name: &str, default: &str) -> BenchResult<Decimal> {
    Ok(Decimal::from_str(
        &std::env::var(name).unwrap_or_else(|_| default.to_string()),
    )?)
}

fn percentile(sorted: &[f64], percentile: f64) -> f64 {
    let rank = (percentile * sorted.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index]
}

fn calc_stats(times: &[Duration]) -> Option<Stats> {
    if times.is_empty() {
        return None;
    }

    let mut values: Vec<f64> = times
        .iter()
        .map(|duration| duration.as_micros() as f64 / 1000.0)
        .collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let mean_ms = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean_ms).powi(2))
        .sum::<f64>()
        / values.len() as f64;

    Some(Stats {
        mean_ms,
        std_dev_ms: variance.sqrt(),
        min_ms: values[0],
        max_ms: values[values.len() - 1],
        p50_ms: percentile(&values, 0.50),
        p95_ms: percentile(&values, 0.95),
        p99_ms: percentile(&values, 0.99),
    })
}

fn print_stats(name: &str, times: &[Duration], successes: usize, attempts: usize) {
    println!("{name}:");
    match calc_stats(times) {
        Some(stats) => {
            println!(
                "  p50/p95/p99: {:.1} / {:.1} / {:.1} ms",
                stats.p50_ms, stats.p95_ms, stats.p99_ms
            );
            println!(
                "  mean:        {:.1} ms +/- {:.1} ms",
                stats.mean_ms, stats.std_dev_ms
            );
            println!(
                "  range:       {:.1} - {:.1} ms",
                stats.min_ms, stats.max_ms
            );
            println!("  success:     {successes}/{attempts}");
        },
        None => println!("  success:     0/{attempts}"),
    }
}

impl LiveConfig {
    fn from_env() -> BenchResult<Self> {
        dotenvy::dotenv().ok();

        let api_secret =
            env_string("POLYMARKET_API_SECRET").or_else(|| env_string("POLYMARKET_SECRET"));
        let api_passphrase =
            env_string("POLYMARKET_API_PASSPHRASE").or_else(|| env_string("POLYMARKET_PASSPHRASE"));
        let api_credentials = match (env_string("POLYMARKET_API_KEY"), api_secret, api_passphrase) {
            (Some(api_key), Some(secret), Some(passphrase)) => Some(ApiCredentials {
                api_key,
                secret,
                passphrase,
            }),
            _ => None,
        };

        let side = match std::env::var("POLYMARKET_BENCH_SIDE")
            .unwrap_or_else(|_| "BUY".to_string())
            .to_ascii_uppercase()
            .as_str()
        {
            "BUY" => Side::BUY,
            "SELL" => Side::SELL,
            other => return Err(format!("unsupported POLYMARKET_BENCH_SIDE={other}").into()),
        };

        Ok(Self {
            host: std::env::var("POLYMARKET_BENCH_HOST")
                .or_else(|_| std::env::var("POLYMARKET_HOST"))
                .unwrap_or_else(|_| "https://clob.polymarket.com".to_string()),
            chain_id: env_u64("POLYMARKET_CHAIN_ID", 137),
            private_key: env_required("POLYMARKET_PRIVATE_KEY")?,
            api_credentials,
            signature_type: std::env::var("POLYMARKET_SIGNATURE_TYPE")
                .ok()
                .and_then(|value| value.parse().ok()),
            funder: env_string("POLYMARKET_FUNDER")
                .or_else(|| env_string("POLYMARKET_FUNDER_ADDRESS")),
            iterations: env_usize("POLYMARKET_BENCH_ITERATIONS", 20),
            warmups: env_usize("POLYMARKET_BENCH_WARMUPS", 3),
            delay: Duration::from_millis(env_u64("POLYMARKET_BENCH_DELAY_MS", 150)),
            live_post_order: env_bool("POLYMARKET_BENCH_LIVE_POST_ORDER", false),
            live_post_iterations: env_usize("POLYMARKET_BENCH_LIVE_POST_ITERATIONS", 1),
            token_id: env_string("POLYMARKET_BENCH_TOKEN_ID"),
            min_midpoint: env_decimal("POLYMARKET_BENCH_MIN_MIDPOINT", "0.05")?,
            price: env_decimal("POLYMARKET_BENCH_PRICE", "0.01")?,
            size: env_decimal("POLYMARKET_BENCH_SIZE", "5")?,
            side,
            tick_size: env_decimal("POLYMARKET_BENCH_TICK_SIZE", "0.0001")?,
            neg_risk: env_bool("POLYMARKET_BENCH_NEG_RISK", false),
            derive_api_credentials: env_bool("POLYMARKET_BENCH_DERIVE_API_CREDS", true),
        })
    }
}

async fn resolve_api_credentials(config: &LiveConfig) -> BenchResult<ApiCredentials> {
    if config.derive_api_credentials {
        let bootstrap = ClobClient::from_config(ClientConfig {
            base_url: config.host.clone(),
            chain: config.chain_id,
            private_key: Some(config.private_key.clone()),
            signature_type: config.signature_type,
            funder: config.funder.clone(),
            ..ClientConfig::default()
        })?;

        match bootstrap.create_or_derive_api_key(None).await {
            Ok(credentials) => return Ok(credentials),
            Err(err) if config.api_credentials.is_some() => {
                println!("derive API credentials failed; falling back to env credentials: {err}");
            },
            Err(err) => return Err(err.into()),
        }
    }

    config.api_credentials.clone().ok_or_else(|| {
        "API credentials must be set, or POLYMARKET_BENCH_DERIVE_API_CREDS must be true".into()
    })
}

fn polyfill_client(
    config: &LiveConfig,
    api_credentials: ApiCredentials,
) -> BenchResult<ClobClient> {
    Ok(ClobClient::from_config(ClientConfig {
        base_url: config.host.clone(),
        chain: config.chain_id,
        private_key: Some(config.private_key.clone()),
        api_credentials: Some(api_credentials),
        signature_type: config.signature_type,
        funder: config.funder.clone(),
        ..ClientConfig::default()
    })?)
}

async fn official_client(
    config: &LiveConfig,
) -> BenchResult<(
    OfficialClient<OfficialAuthenticated<Normal>>,
    PrivateKeySigner,
)> {
    let signer = LocalSigner::from_str(&config.private_key)?.with_chain_id(Some(POLYGON));
    let client = OfficialClient::new(&config.host, OfficialConfig::default())?;
    let mut auth = client.authentication_builder(&signer);

    if !config.derive_api_credentials {
        let api_credentials = config.api_credentials.as_ref().ok_or_else(|| {
            "API credentials must be set, or POLYMARKET_BENCH_DERIVE_API_CREDS must be true"
                .to_string()
        })?;
        let api_key = Uuid::parse_str(&api_credentials.api_key)?;
        let credentials = OfficialCredentials::new(
            api_key,
            api_credentials.secret.clone(),
            api_credentials.passphrase.clone(),
        );
        auth = auth.credentials(credentials);
    }

    if let Some(signature_type) = config.signature_type {
        auth = auth.signature_type(match signature_type {
            0 => OfficialSignatureType::Eoa,
            1 => OfficialSignatureType::Proxy,
            2 => OfficialSignatureType::GnosisSafe,
            3 => OfficialSignatureType::Poly1271,
            other => return Err(format!("unsupported POLYMARKET_SIGNATURE_TYPE={other}").into()),
        });
    }
    if let Some(funder) = &config.funder {
        auth = auth.funder(OfficialAddress::from_str(funder)?);
    }

    Ok((auth.authenticate().await?, signer))
}

async fn run_samples<F, Fut>(
    label: &str,
    warmups: usize,
    iterations: usize,
    delay: Duration,
    mut f: F,
) -> BenchResult<Vec<Duration>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = BenchResult<()>>,
{
    for index in 0..warmups {
        if let Err(err) = f().await {
            println!("  {label} warmup {} failed: {err}", index + 1);
        }
        tokio::time::sleep(delay).await;
    }

    let mut times = Vec::with_capacity(iterations);
    let mut successes = 0;
    for index in 0..iterations {
        let start = Instant::now();
        match f().await {
            Ok(()) => {
                successes += 1;
                times.push(start.elapsed());
            },
            Err(err) => {
                println!("  {label} iteration {} failed: {err}", index + 1);
            },
        }
        if index + 1 < iterations {
            tokio::time::sleep(delay).await;
        }
    }

    print_stats(label, &times, successes, iterations);
    Ok(times)
}

fn create_order_options(tick_size: Decimal, neg_risk: bool) -> CreateOrderOptions {
    CreateOrderOptions {
        tick_size: Some(tick_size),
        neg_risk: Some(neg_risk),
    }
}

async fn resolve_live_order_token(config: &LiveConfig, client: &ClobClient) -> BenchResult<String> {
    if let Some(token_id) = &config.token_id {
        return Ok(token_id.clone());
    }

    let markets = client.get_sampling_markets(None).await?;
    for market in markets
        .data
        .iter()
        .filter(|market| market.active && !market.closed)
    {
        for token in &market.tokens {
            let midpoint = match client.get_midpoint(&token.token_id).await {
                Ok(midpoint) => midpoint.mid,
                Err(_) => continue,
            };
            if midpoint >= config.min_midpoint {
                return Ok(token.token_id.clone());
            }
        }
    }

    Err(format!(
        "POLYMARKET_BENCH_TOKEN_ID was not set and no active token had midpoint >= {}",
        config.min_midpoint
    )
    .into())
}

fn order_args(config: &LiveConfig, token_id: String) -> OrderArgs {
    OrderArgs {
        token_id,
        price: config.price,
        size: config.size,
        side: config.side,
        expiration: None,
        builder_code: None,
        metadata: None,
    }
}

fn post_options() -> PostOrderOptions {
    PostOrderOptions {
        order_type: OrderType::GTC,
        post_only: true,
        defer_exec: false,
    }
}

fn official_tick_size(tick_size: Decimal) -> BenchResult<TickSize> {
    let value = tick_size.to_string();
    Ok(match value.as_str() {
        "0.1" => TickSize::Tenth,
        "0.01" => TickSize::Hundredth,
        "0.001" => TickSize::Thousandth,
        "0.0001" => TickSize::TenThousandth,
        _ => return Err(format!("unsupported POLYMARKET_BENCH_TICK_SIZE={value}").into()),
    })
}

#[tokio::main]
async fn main() -> BenchResult<()> {
    let config = LiveConfig::from_env()?;
    let api_credentials = resolve_api_credentials(&config).await?;
    let polyfill = polyfill_client(&config, api_credentials.clone())?;
    let (official, official_signer) = official_client(&config).await?;

    println!("Live authenticated Polymarket submit-path benchmark");
    println!("host: {}", config.host);
    println!("official SDK rev: {OFFICIAL_SDK_REV}");
    println!(
        "api credentials: {}",
        if config.derive_api_credentials {
            "derived during setup"
        } else {
            "loaded from env"
        }
    );
    println!(
        "safe reads: {} warmups, {} iterations, {}ms delay",
        config.warmups,
        config.iterations,
        config.delay.as_millis()
    );
    println!();

    run_samples(
        "polyfill get_orders",
        config.warmups,
        config.iterations,
        config.delay,
        || async {
            let orders = polyfill.get_orders(None, Some("MA==")).await?;
            std::hint::black_box(orders);
            Ok(())
        },
    )
    .await?;

    let official_orders_request = OfficialOrdersRequest::default();
    run_samples(
        "rs-clob-client-v2 orders",
        config.warmups,
        config.iterations,
        config.delay,
        || async {
            let orders = official
                .orders(&official_orders_request, Some("MA==".to_string()))
                .await?;
            std::hint::black_box(orders);
            Ok(())
        },
    )
    .await?;

    if !config.live_post_order {
        println!();
        println!("live order posting skipped; set POLYMARKET_BENCH_LIVE_POST_ORDER=1 to enable");
        return Ok(());
    }

    let live_token_id = resolve_live_order_token(&config, &polyfill).await?;
    let live_tick_size = polyfill
        .get_tick_size(&live_token_id)
        .await
        .unwrap_or(config.tick_size);
    let live_neg_risk = polyfill
        .get_neg_risk(&live_token_id)
        .await
        .unwrap_or(config.neg_risk);
    println!();
    println!(
        "LIVE ORDER POST ENABLED: {} iterations, token_id={}, tick_size={}, neg_risk={}, side={:?}, price={}, size={}, post_only=true",
        config.live_post_iterations,
        live_token_id,
        live_tick_size,
        live_neg_risk,
        config.side,
        config.price,
        config.size
    );
    println!("orders that return an order id are canceled immediately after timing is recorded");

    let args = order_args(&config, live_token_id.clone());
    let create_options = create_order_options(live_tick_size, live_neg_risk);
    let post_options = post_options();

    run_samples(
        "polyfill live create/sign/post",
        0,
        config.live_post_iterations,
        config.delay,
        || async {
            let response = polyfill
                .create_and_post_order(&args, Some(&create_options), Some(&post_options))
                .await?;
            let order_id = response.order_id.clone();
            std::hint::black_box(response);
            if !order_id.is_empty() {
                polyfill.cancel(&order_id).await?;
            }
            Ok(())
        },
    )
    .await?;

    let token_id = U256::from_str(&live_token_id)?;
    official.set_tick_size(token_id, official_tick_size(live_tick_size)?);
    official.set_neg_risk(token_id, live_neg_risk);
    let official_side = match config.side {
        Side::BUY => OfficialSide::Buy,
        Side::SELL => OfficialSide::Sell,
    };
    let official_price = OfficialDecimal::from_str(&config.price.to_string())?;
    let official_size = OfficialDecimal::from_str(&config.size.to_string())?;

    run_samples(
        "rs-clob-client-v2 live build/sign/post",
        0,
        config.live_post_iterations,
        config.delay,
        || async {
            let order = official
                .limit_order()
                .token_id(token_id)
                .side(official_side)
                .price(official_price)
                .size(official_size)
                .order_type(OfficialOrderType::GTC)
                .post_only(true)
                .build()
                .await?;
            let signed = official.sign(&official_signer, order).await?;
            let response = official.post_order(signed).await?;
            let order_id = response.order_id.clone();
            std::hint::black_box(response);
            if !order_id.is_empty() {
                official.cancel_order(&order_id).await?;
            }
            Ok(())
        },
    )
    .await?;

    Ok(())
}
