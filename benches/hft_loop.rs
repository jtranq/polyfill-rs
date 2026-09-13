//! End-to-end local HFT-loop benchmarks.
//!
//! These benches intentionally avoid live network I/O. They measure the local loop a trading
//! process actually controls: receive/apply a book update, make a simple best-ask decision, build
//! and sign an order, serialize the exact submit body, and build L2 auth headers.
//!
//! Run the polyfill benches with:
//! `cargo bench --bench hft_loop`
//!
//! Run the optional rs-clob-client-v2 comparison with:
//! `cargo bench --bench hft_loop --features official-client-benchmark`

use alloy_signer_local::PrivateKeySigner;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use polyfill_rs::{
    auth::{create_l2_headers_with_body_bytes, PreparedApiCredentials},
    orders::{OrderBuilder, BYTES32_ZERO},
    types::{ApiCredentials, OrderType, PostOrder, PostOrderOptions},
    OrderBookManager, Side, WsBookUpdateProcessor,
};
use rust_decimal::Decimal;
use std::sync::atomic::{AtomicU64, Ordering};

const ASSET_ID: &str = "12345678901234567890";
const MARKET: &str = "0xabc";
const CHAIN_ID: u64 = 137;
const PRIVATE_KEY: &str = "0x1234567890123456789012345678901234567890123456789012345678901234";
const START_TIMESTAMP: u64 = 1_000_000_000_000_000;

fn test_signer() -> PrivateKeySigner {
    PRIVATE_KEY.parse().expect("valid benchmark private key")
}

fn api_credentials() -> ApiCredentials {
    ApiCredentials {
        api_key: "benchmark-api-key".to_string(),
        secret: "dGVzdF9zZWNyZXRfa2V5XzEyMzQ1".to_string(),
        passphrase: "benchmark-passphrase".to_string(),
    }
}

fn price_decimal_from_ticks(ticks: u32) -> Decimal {
    Decimal::new(ticks as i64, 4)
}

#[derive(Clone, Copy)]
struct TimestampRange {
    start: usize,
    end: usize,
}

impl TimestampRange {
    fn find(bytes: &[u8]) -> Self {
        let needle = b"\"timestamp\":";
        let Some(pos) = bytes.windows(needle.len()).position(|w| w == needle) else {
            panic!("timestamp field not found in WS template JSON");
        };

        let start = pos + needle.len();
        let mut end = start;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }

        Self { start, end }
    }

    fn write_fixed_width(&self, bytes: &mut [u8], mut value: u64) {
        for idx in (self.start..self.end).rev() {
            bytes[idx] = b'0' + (value % 10) as u8;
            value /= 10;
        }
    }
}

fn price_string_from_ticks(ticks: u32) -> String {
    let whole = ticks / 10_000;
    let frac = ticks % 10_000;
    format!("{whole}.{frac:04}")
}

fn build_ws_book_template(levels_per_side: usize) -> Vec<u8> {
    let mut json = String::new();

    json.push_str("{\"event_type\":\"book\",\"asset_id\":\"");
    json.push_str(ASSET_ID);
    json.push_str("\",\"market\":\"");
    json.push_str(MARKET);
    json.push_str("\",\"timestamp\":");
    json.push_str(&START_TIMESTAMP.to_string());
    json.push_str(",\"bids\":[");

    for i in 0..levels_per_side {
        if i != 0 {
            json.push(',');
        }
        let bid_price = price_string_from_ticks(7_500 - i as u32);
        json.push_str("{\"price\":\"");
        json.push_str(&bid_price);
        json.push_str("\",\"size\":\"100.0000\"}");
    }

    json.push_str("],\"asks\":[");
    for i in 0..levels_per_side {
        if i != 0 {
            json.push(',');
        }
        let ask_price = price_string_from_ticks(7_501 + i as u32);
        json.push_str("{\"price\":\"");
        json.push_str(&ask_price);
        json.push_str("\",\"size\":\"100.0000\"}");
    }
    json.push_str("]}");

    json.into_bytes()
}

fn build_polyfill_order_context() -> (
    PrivateKeySigner,
    polyfill_rs::orders::PreparedOrderPath,
    ApiCredentials,
    PreparedApiCredentials,
    PostOrderOptions,
) {
    let signer = test_signer();
    let builder = OrderBuilder::new(signer.clone(), None, None);
    let prepared_order = builder
        .prepare_order_path(
            CHAIN_ID,
            ASSET_ID.to_string(),
            Decimal::new(1, 4),
            false,
            Some(BYTES32_ZERO),
            Some(BYTES32_ZERO),
        )
        .unwrap();
    let api_creds = api_credentials();
    let prepared_api_creds = PreparedApiCredentials::try_new(api_creds.clone()).unwrap();
    let post_options = PostOrderOptions {
        order_type: OrderType::GTD,
        post_only: false,
        defer_exec: false,
    };

    (
        signer,
        prepared_order,
        api_creds,
        prepared_api_creds,
        post_options,
    )
}

fn benchmark_polyfill_hft_loop(c: &mut Criterion) {
    let (signer, prepared_order, api_creds, prepared_api_creds, post_options) =
        build_polyfill_order_context();
    let books = OrderBookManager::new(64);
    books.get_or_create_book(ASSET_ID).unwrap();
    books
        .with_book_mut(ASSET_ID, |book| {
            book.set_tick_size_ticks(1);
            Ok(())
        })
        .unwrap();

    let template = build_ws_book_template(16);
    let timestamp_range = TimestampRange::find(&template);
    let timestamp = AtomicU64::new(START_TIMESTAMP + 1);
    let mut processor = WsBookUpdateProcessor::new(template.len());

    c.bench_function("polyfill_hft_ws_apply_decide_sign_serialize_auth", |b| {
        b.iter_batched_ref(
            || {
                let mut message = template.clone();
                let next_timestamp = timestamp.fetch_add(1, Ordering::Relaxed);
                timestamp_range.write_fixed_width(&mut message, next_timestamp);
                message
            },
            |message| {
                let stats = processor.process_bytes(black_box(message), &books).unwrap();
                debug_assert_eq!(stats.book_messages, 1);

                let selected_price = books
                    .with_book_mut(ASSET_ID, |book| {
                        let best_ask = book.best_ask_fast().expect("seeded ask side");
                        Ok(price_decimal_from_ticks(best_ask.price))
                    })
                    .unwrap();

                let signed_order = prepared_order
                    .create_limit_order(
                        black_box(Side::BUY),
                        black_box(selected_price),
                        black_box(Decimal::new(10_025, 2)),
                        black_box(Some(1_900_000_000)),
                    )
                    .unwrap();
                let body = PostOrder::new(
                    black_box(signed_order),
                    black_box(api_creds.api_key.clone()),
                    black_box(post_options),
                );
                let body_bytes = serde_json::to_vec(black_box(&body)).unwrap();
                let headers = create_l2_headers_with_body_bytes(
                    &signer,
                    &prepared_api_creds,
                    "POST",
                    "/order",
                    Some(&body_bytes),
                )
                .unwrap();

                black_box((stats, body_bytes, headers))
            },
            BatchSize::SmallInput,
        )
    });
}

fn benchmark_polyfill_order_decision_to_payload(c: &mut Criterion) {
    let (signer, prepared_order, api_creds, prepared_api_creds, post_options) =
        build_polyfill_order_context();
    let selected_price = Decimal::new(7_501, 4);

    c.bench_function("polyfill_decide_sign_serialize_prepared", |b| {
        b.iter(|| {
            let signed_order = prepared_order
                .create_limit_order(
                    black_box(Side::BUY),
                    black_box(selected_price),
                    black_box(Decimal::new(10_025, 2)),
                    black_box(Some(1_900_000_000)),
                )
                .unwrap();
            let body = PostOrder::new(
                black_box(signed_order),
                black_box(api_creds.api_key.clone()),
                black_box(post_options),
            );
            let body_bytes = serde_json::to_vec(black_box(&body)).unwrap();
            black_box(body_bytes)
        })
    });

    c.bench_function("polyfill_decide_sign_serialize_auth_prepared", |b| {
        b.iter(|| {
            let signed_order = prepared_order
                .create_limit_order(
                    black_box(Side::BUY),
                    black_box(selected_price),
                    black_box(Decimal::new(10_025, 2)),
                    black_box(Some(1_900_000_000)),
                )
                .unwrap();
            let body = PostOrder::new(
                black_box(signed_order),
                black_box(api_creds.api_key.clone()),
                black_box(post_options),
            );
            let body_bytes = serde_json::to_vec(black_box(&body)).unwrap();
            let headers = create_l2_headers_with_body_bytes(
                &signer,
                &prepared_api_creds,
                "POST",
                "/order",
                Some(&body_bytes),
            )
            .unwrap();
            black_box((body_bytes, headers))
        })
    });
}

#[cfg(feature = "official-client-benchmark")]
fn benchmark_official_order_decision_to_payload(c: &mut Criterion) {
    use polymarket_client_sdk_v2::{
        auth::{Credentials, LocalSigner, Signer, Uuid},
        clob::{
            types::{OrderType as OfficialOrderType, Side as OfficialSide, TickSize},
            Client as OfficialClient, Config as OfficialConfig,
        },
        types::{dec, Decimal as OfficialDecimal, U256},
        POLYGON,
    };
    use std::str::FromStr as _;

    let mut server = mockito::Server::new();
    let _version_mock = server
        .mock("GET", "/version")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"version":2}"#)
        .create();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let signer = LocalSigner::from_str(PRIVATE_KEY)
        .unwrap()
        .with_chain_id(Some(POLYGON));
    let token_id = U256::from_str(ASSET_ID).unwrap();
    let client = runtime.block_on(async {
        let client = OfficialClient::new(&server.url(), OfficialConfig::default()).unwrap();
        client.set_tick_size(token_id, TickSize::TenThousandth);
        client.set_neg_risk(token_id, false);
        client.version().await.unwrap();
        client
            .authentication_builder(&signer)
            .credentials(Credentials::new(
                Uuid::nil(),
                "dGVzdF9zZWNyZXRfa2V5XzEyMzQ1".to_string(),
                "benchmark-passphrase".to_string(),
            ))
            .authenticate()
            .await
            .unwrap()
    });

    c.bench_function("rs_clob_client_v2_decide_sign_serialize_warmed", |b| {
        b.iter(|| {
            let body_bytes = runtime.block_on(async {
                let order = client
                    .limit_order()
                    .token_id(black_box(token_id))
                    .side(black_box(OfficialSide::Buy))
                    .price(black_box(dec!(0.7501)))
                    .size(black_box(OfficialDecimal::new(10_025, 2)))
                    .expiration(chrono::DateTime::from_timestamp(1_900_000_000, 0).unwrap())
                    .order_type(OfficialOrderType::GTD)
                    .build()
                    .await
                    .unwrap();
                let signed = client.sign(&signer, order).await.unwrap();
                serde_json::to_vec(black_box(&signed)).unwrap()
            });
            black_box(body_bytes)
        })
    });
}

#[cfg(not(feature = "official-client-benchmark"))]
criterion_group!(
    benches,
    benchmark_polyfill_hft_loop,
    benchmark_polyfill_order_decision_to_payload
);

#[cfg(feature = "official-client-benchmark")]
criterion_group!(
    benches,
    benchmark_polyfill_hft_loop,
    benchmark_polyfill_order_decision_to_payload,
    benchmark_official_order_decision_to_payload
);

criterion_main!(benches);
