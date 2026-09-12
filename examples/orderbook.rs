//! A public, read-only Polymarket order-book viewer.
//!
//! Run `cargo run --release --example orderbook -- --help` for options.
//! Uses public APIs and full-depth Decimal books; no credentials required.
//! This terminal viewer is not a zero-allocation benchmark.

use anyhow::{bail, ensure, Context, Result};
use futures_util::{stream, SinkExt, Stream, StreamExt};
use polyfill_rs::{types::OrderSummary, Side, StreamMessage};
use rust_decimal::Decimal;
use std::{
    collections::{BTreeMap, VecDeque},
    io::{self, IsTerminal, Write},
    time::{Duration, Instant},
};
use tokio::time::{interval, sleep, timeout, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[path = "orderbook/discovery.rs"]
mod discovery;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const LEVELS: usize = 5;
const HELP: &str = "polyfill-rs / live order book

Usage: cargo run --release --example orderbook [-- --token-id ID]

With no token ID, samples live books for 6s and picks the busiest visible ladder.
Public data only; no credentials required. Ctrl-C exits.
Shows five levels at 30 Hz (every 5s when redirected). Prices: USD/share; sizes: shares.
Update age is local receipt time, not network latency. Reconnects clear stale quotes.";

fn parse_token(args: &[String]) -> Result<Option<&str>> {
    match args {
        [] => Ok(None),
        [flag, id] if flag == "--token-id" && discovery::valid_token(id) => Ok(Some(id)),
        _ => {
            bail!("Expected --token-id followed by a decimal token ID (up to 78 digits), or --help")
        },
    }
}

struct Market {
    token_id: String,
    question: String,
    outcome: String,
}

#[derive(Default, Debug)]
struct Book {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    ready: bool,
    updates: u64,
    received_at: Option<Instant>,
}

impl Book {
    fn apply(&mut self, token_id: &str, event: StreamMessage) -> Result<bool> {
        match event {
            StreamMessage::Book(snapshot) if snapshot.asset_id == token_id => {
                let bids = levels(snapshot.bids)?;
                let asks = levels(snapshot.asks)?;
                // `book` always replaces BOTH sides, including omitted levels.
                self.bids = bids;
                self.asks = asks;
                self.ready = true;
                self.mark_update();
                Ok(true)
            },
            StreamMessage::PriceChange(update) => {
                let changes: Vec<_> = update
                    .price_changes
                    .into_iter()
                    .filter(|change| change.asset_id == token_id)
                    .collect();
                // Validate the entire batch before changing either side. A missing
                // size cannot be inferred from best_bid/best_ask; resubscribe instead.
                for change in &changes {
                    validate_level(
                        change.price,
                        change.size.context("Price change missing size")?,
                    )?;
                }
                if !self.ready || changes.is_empty() {
                    return Ok(false);
                }
                for change in changes {
                    let side = match change.side {
                        Side::BUY => &mut self.bids,
                        Side::SELL => &mut self.asks,
                    };
                    // Size is the new aggregate at this price, not an amount to add.
                    set_level(side, change.price, change.size.expect("validated size"));
                }
                // Preserve WS arrival order, including multiple events with the
                // same millisecond timestamp. The protocol supplies no sequence ID.
                self.mark_update();
                Ok(true)
            },
            _ => Ok(false),
        }
    }

    fn mark_update(&mut self) {
        self.updates += 1;
        self.received_at = Some(Instant::now());
    }

    fn rows(&self, depth: usize) -> impl Iterator<Item = [Option<(Decimal, Decimal)>; 2]> + '_ {
        let mut bids = self.bids.iter().rev();
        let mut asks = self.asks.iter();
        (0..depth).map(move |_| {
            [bids.next(), asks.next()].map(|level| level.map(|(&price, &size)| (price, size)))
        })
    }

    fn best_prices(&self) -> (Option<Decimal>, Option<Decimal>) {
        (
            self.bids.last_key_value().map(|(price, _)| *price),
            self.asks.first_key_value().map(|(price, _)| *price),
        )
    }

    fn spread_mid(&self) -> (Option<Decimal>, Option<Decimal>) {
        match self.best_prices() {
            (Some(bid), Some(ask)) => (Some(ask - bid), Some((bid + ask) / Decimal::TWO)),
            _ => (None, None),
        }
    }
}

fn validate_level(price: Decimal, size: Decimal) -> Result<()> {
    ensure!(
        (Decimal::ZERO..=Decimal::ONE).contains(&price) && size >= Decimal::ZERO,
        "Invalid book level: price must be 0..1 and size non-negative"
    );
    Ok(())
}

fn set_level(side: &mut BTreeMap<Decimal, Decimal>, price: Decimal, size: Decimal) {
    if size.is_zero() {
        side.remove(&price);
    } else {
        side.insert(price, size);
    }
}

fn levels(summaries: Vec<OrderSummary>) -> Result<BTreeMap<Decimal, Decimal>> {
    let mut side = BTreeMap::new();
    for level in summaries {
        validate_level(level.price, level.size)?;
        set_level(&mut side, level.price, level.size);
    }
    Ok(side)
}

fn decode_events(text: &str) -> Result<Vec<StreamMessage>> {
    // The general compatibility parser deliberately skips invalid batch members.
    // A local order book must fail closed on an undecodable update and resubscribe.
    let value: serde_json::Value = serde_json::from_str(text).context("Invalid WebSocket JSON")?;
    let values = match value {
        serde_json::Value::Array(values) => values,
        value => vec![value],
    };
    values
        .into_iter()
        .map(|value| {
            // Compatibility types accept null collections. Here an absent book
            // side or delta batch is ambiguous, so request a fresh snapshot.
            let required_arrays: &[&str] = match value.get("event_type").and_then(|v| v.as_str()) {
                Some("book") => &["bids", "asks"],
                Some("price_change") => &["price_changes"],
                _ => &[],
            };
            for field in required_arrays {
                ensure!(
                    value.get(*field).is_some_and(|v| v.is_array()),
                    "Market event is missing an array for {field}; resynchronizing"
                );
            }
            serde_json::from_value(value).context("Could not decode market event; resynchronizing")
        })
        .collect()
}

struct Display {
    interactive: bool,
    color: bool,
    started: Instant,
    history: DisplayHistory,
}

const HIGHLIGHT_DURATION: Duration = Duration::from_millis(150);
const UPDATE_RATE_WINDOW: Duration = Duration::from_secs(5);

#[derive(Default)]
struct HighlightRow {
    level: Option<(Decimal, Decimal)>,
    price_changed: Option<Instant>,
    size_changed: Option<Instant>,
}

impl HighlightRow {
    fn observe(&mut self, level: Option<(Decimal, Decimal)>, initialized: bool, now: Instant) {
        if initialized {
            if self.level.map(|(price, _)| price) != level.map(|(price, _)| price) {
                self.price_changed = Some(now);
            }
            if self.level.map(|(_, size)| size) != level.map(|(_, size)| size) {
                self.size_changed = Some(now);
            }
        }
        self.level = level;
    }

    fn changed(&self, now: Instant) -> bool {
        recent(self.price_changed, now) || recent(self.size_changed, now)
    }

    fn cells(&self, color: Option<u8>, now: Instant) -> (String, String) {
        let cell = |value, changed, width| {
            let value = format!("{:>width$}", number(value));
            match color {
                Some(color) if recent(changed, now) => {
                    format!("\x1b[30;48;5;{color}m{value}\x1b[0m")
                },
                Some(color) => format!("\x1b[38;5;{color}m{value}\x1b[0m"),
                None => value,
            }
        };
        (
            cell(self.level.map(|(p, _)| p), self.price_changed, 8),
            cell(self.level.map(|(_, s)| s), self.size_changed, 12),
        )
    }
}

fn recent(changed: Option<Instant>, now: Instant) -> bool {
    changed.is_some_and(|at| now.saturating_duration_since(at) < HIGHLIGHT_DURATION)
}

#[derive(Default)]
struct DisplayHistory {
    rows: Vec<[HighlightRow; 2]>,
    samples: VecDeque<(Instant, u64)>,
}

impl DisplayHistory {
    fn observe(&mut self, book: &Book, levels: usize, now: Instant) -> f64 {
        if !book.ready
            || self
                .samples
                .back()
                .is_some_and(|(_, count)| book.updates < *count)
        {
            *self = Self::default();
        }
        self.rows.resize_with(levels, Default::default);
        for (row, levels) in self.rows.iter_mut().zip(book.rows(levels)) {
            for (cell, level) in row.iter_mut().zip(levels) {
                cell.observe(level, !self.samples.is_empty(), now);
            }
        }
        if !book.ready {
            return 0.0;
        }

        // Count received events, not redraws; retain a sample on the 5s boundary.
        self.samples.push_back((now, book.updates));
        while self
            .samples
            .get(1)
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) >= UPDATE_RATE_WINDOW)
        {
            self.samples.pop_front();
        }
        let (start, count) = self.samples.front().expect("sample was just recorded");
        let elapsed = now.saturating_duration_since(*start).as_secs_f64();
        if elapsed > 0.0 {
            (book.updates - count) as f64 / elapsed
        } else {
            0.0
        }
    }
}

impl Display {
    fn new() -> Result<Self> {
        let interactive =
            io::stdout().is_terminal() && std::env::var("TERM").is_ok_and(|term| term != "dumb");
        let display = Self {
            interactive,
            color: interactive && std::env::var_os("NO_COLOR").is_none(),
            started: Instant::now(),
            history: DisplayHistory::default(),
        };
        if interactive {
            print!("\x1b[?25l");
            io::stdout().flush()?;
        }
        Ok(display)
    }

    fn message(&self, message: &str) -> Result<()> {
        let mut out = io::stdout().lock();
        writeln!(out, "{}", clean(message, 160))?;
        out.flush()?;
        Ok(())
    }

    fn render(&mut self, market: &Market, book: &Book, status: &str, detail: &str) -> Result<()> {
        let frame = self.frame(market, book, status, detail, Instant::now());
        let mut out = io::stdout().lock();
        out.write_all(frame.as_bytes())?;
        out.flush()?;
        Ok(())
    }

    fn frame(
        &mut self,
        market: &Market,
        book: &Book,
        status: &str,
        detail: &str,
        now: Instant,
    ) -> String {
        use std::fmt::Write;
        let rate = self.history.observe(book, LEVELS, now);
        let (bid, ask) = book.best_prices();
        let (spread, mid) = book.spread_mid();
        let [bid_color, ask_color, accent, reset] = if self.color {
            [
                "\x1b[38;5;121m",
                "\x1b[38;5;210m",
                "\x1b[1;38;5;117m",
                "\x1b[0m",
            ]
        } else {
            [""; 4]
        };
        let clear = if self.interactive {
            "\x1b[H\x1b[2J"
        } else {
            ""
        };
        let rule = "─".repeat(76);
        let mut frame = format!(
            "{clear}{accent}polyfill-rs / live order book{reset}       PUBLIC DATA / NO KEYS\n\
             {rule}\n{}\nOutcome: {}\nToken: {}\n\n\
             {accent}{status:<12}{reset} {:<43}  {:>5}s elapsed\n\
             {bid_color}BID {:>8}{reset}  {ask_color}ASK {:>8}{reset}  SPREAD {:>8}  MID {:>8}\n\n\
             {bid_color}     BID SIZE       BID PRICE{reset}  │  {ask_color}ASK PRICE       ASK SIZE{reset}\n\
             ───────────────────────────────┼────────────────────────────\n",
            clean(&market.question, 76), clean(&market.outcome, 65), short_token(&market.token_id),
            clean(detail, 43), now.duration_since(self.started).as_secs(),
            number(bid), number(ask), number(spread), number(mid),
        );
        for [bid, ask] in &self.history.rows {
            let (bid_price, bid_size) = bid.cells(self.color.then_some(121), now);
            let (ask_price, ask_size) = ask.cells(self.color.then_some(210), now);
            let marker = |row: &HighlightRow| {
                if self.interactive && row.changed(now) {
                    '*'
                } else {
                    ' '
                }
            };
            writeln!(
                frame,
                "{}{bid_size}        {bid_price}  │ {}{ask_price}   {ask_size}",
                marker(bid),
                marker(ask)
            )
            .unwrap();
        }
        let age = book
            .received_at
            .map(|at| {
                format!(
                    "{:.1}s ago",
                    now.saturating_duration_since(at).as_secs_f64()
                )
            })
            .unwrap_or_else(|| "awaiting snapshot".to_owned());
        write!(
            frame,
            "\nUpdates: {:<8}  Updates/s (5s avg): {rate:>6.1}\n\
             Last book update: {age}\n\
             USD/share | Size: shares | * recent change | Ctrl-C to exit\n{rule}\n",
            book.updates,
        )
        .unwrap();
        frame
    }
}

impl Drop for Display {
    fn drop(&mut self) {
        if self.interactive {
            let _ = write!(io::stdout(), "\x1b[0m\x1b[?25h");
            let _ = io::stdout().flush();
        }
    }
}

fn clean(text: &str, width: usize) -> String {
    // Public market titles must not inject terminal controls (including escapes).
    let text: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if text.chars().count() <= width {
        text
    } else {
        format!(
            "{}…",
            text.chars()
                .take(width.saturating_sub(1))
                .collect::<String>()
        )
    }
}

fn short_token(token: &str) -> String {
    if token.len() > 60 {
        format!("{}…{}", &token[..30], &token[token.len() - 20..])
    } else {
        token.to_owned()
    }
}

fn number(value: Option<Decimal>) -> String {
    value
        .map(|v| v.normalize().to_string())
        .unwrap_or_else(|| "--".to_owned())
}

// The stream owns each pending read/send, so a redraw cannot cancel a heartbeat.
async fn subscribe(
    url: &str,
    tokens: &[String],
) -> Result<impl Stream<Item = Result<Vec<StreamMessage>>>> {
    let (mut socket, _) = timeout(Duration::from_secs(10), connect_async(url))
        .await
        .context("WebSocket connection timed out")??;
    let subscription = serde_json::json!({
        "type": "market", "assets_ids": tokens,
        "initial_dump": true, "custom_feature_enabled": true,
    });
    timeout(
        Duration::from_secs(5),
        socket.send(Message::Text(subscription.to_string())),
    )
    .await
    .context("Subscription send timed out")??;
    let mut heartbeat = interval(Duration::from_secs(10));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    Ok(stream::try_unfold(
        (socket, heartbeat, Instant::now()),
        |(mut socket, mut heartbeat, mut last_pong)| async move {
            loop {
                let outgoing = tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until((last_pong + Duration::from_secs(25)).into()) =>
                        bail!("No heartbeat PONG for 25s; connection may be stale"),
                    // Polymarket requires literal text PING, not just WS control frames.
                    _ = heartbeat.tick() => Message::Text("PING".to_owned()),
                    incoming = socket.next() => match incoming.context("Market stream ended")?? {
                        Message::Text(text) if text == "PONG" => { last_pong = Instant::now(); continue; },
                        Message::Text(text) => return Ok(Some((decode_events(&text)?, (socket, heartbeat, last_pong)))),
                        Message::Ping(payload) => Message::Pong(payload),
                        Message::Close(_) => bail!("Server closed the market stream"),
                        Message::Binary(_) => bail!("Unexpected binary market message; cannot maintain book state"),
                        _ => continue,
                    },
                };
                timeout(Duration::from_secs(5), socket.send(outgoing))
                    .await
                    .context("Heartbeat send timed out")??;
            }
        },
    ))
}

async fn session(market: &Market, display: &mut Display) -> Result<()> {
    // A fresh book per connection prevents deltas after reconnect from updating
    // stale depth. Deltas arriving before the initial dump are ignored.
    let mut book = Book::default();
    display.render(market, &book, "CONNECTING", "Opening public market stream")?;
    let mut events = Box::pin(subscribe(WS_URL, std::slice::from_ref(&market.token_id)).await?);
    display.render(
        market,
        &book,
        "SYNCING",
        "Waiting for initial book snapshot",
    )?;

    let connected = Instant::now();
    let mut refresh = interval(Duration::from_nanos(1_000_000_000 / 30));
    refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_render = Instant::now();

    loop {
        tokio::select! {
            incoming = events.next() => {
                let was_ready = book.ready;
                for event in incoming.context("Market stream ended")?? {
                    if let StreamMessage::MarketResolved(ref resolved) = event {
                        if resolved.asset_ids.contains(&market.token_id) {
                            display.render(market, &Book::default(), "RESOLVED", "Restart to discover another market")?;
                            return Ok(());
                        }
                    }
                    book.apply(&market.token_id, event)?;
                }
                if book.ready && !was_ready {
                    render_live(display, market, &book)?;
                    last_render = Instant::now();
                }
            },
            _ = refresh.tick() => {
                ensure!(book.ready || connected.elapsed() < Duration::from_secs(15), "No initial snapshot within 15s; verify --token-id is an active outcome token");
                if display.interactive || last_render.elapsed() >= Duration::from_secs(5) {
                    render_live(display, market, &book)?;
                    last_render = Instant::now();
                }
            },
        }
    }
}

fn render_live(display: &mut Display, market: &Market, book: &Book) -> Result<()> {
    let (bid, ask) = book.best_prices();
    let (status, detail) = if !book.ready {
        ("SYNCING", "Waiting for initial book snapshot")
    } else if matches!((bid, ask), (Some(bid), Some(ask)) if bid > ask) {
        ("CROSSED", "Received bid exceeds ask; inspect feed")
    } else if book
        .received_at
        .is_some_and(|at| at.elapsed() > Duration::from_secs(15))
    {
        ("QUIET", "Heartbeat OK; no recent book changes")
    } else {
        ("LIVE", "Connected / snapshots + price changes")
    };
    display.render(market, book, status, detail)
}

async fn run(token_id: Option<&str>, display: &mut Display) -> Result<()> {
    display.message("Discovering a public market…")?;
    let market = discovery::discover(token_id, display).await?;
    let mut failures = 0u32;
    loop {
        let connected_at = Instant::now();
        let error = match session(&market, display).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        if connected_at.elapsed() > Duration::from_secs(60) {
            failures = 0;
        }
        failures += 1;
        // Never present the previous connection's depth as a live quote.
        display.render(
            &market,
            &Book::default(),
            "DISCONNECTED",
            "Quotes cleared; waiting to reconnect",
        )?;
        ensure!(failures <= 5, "Stream failed after 5 retries: {error:#}. Check network access or try another --token-id.");
        let delay = Duration::from_secs(1 << (failures - 1));
        display.message(&format!(
            "{error:#}. Reconnecting in {}s ({failures}/5)…",
            delay.as_secs()
        ))?;
        sleep(delay).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if matches!(args.as_slice(), [flag] if flag == "--help" || flag == "-h") {
        println!("{HELP}");
        return Ok(());
    }
    let token_id = parse_token(&args)?;
    let mut display = Display::new()?;
    tokio::select! {
        result = run(token_id, &mut display) => result?,
        result = tokio::signal::ctrl_c() => result.context("Could not listen for Ctrl-C")?,
    }
    display.message("Stopped. Disconnected.")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    pub(super) fn snapshot(bids: &[(&str, &str)], asks: &[(&str, &str)]) -> StreamMessage {
        let levels = |side: &[(&str, &str)]| {
            side.iter()
                .map(|(price, size)| json!({"price":price,"size":size}))
                .collect::<Vec<_>>()
        };
        serde_json::from_value(json!({
            "event_type":"book", "asset_id":"123", "market":"market",
            "timestamp":"1000", "bids":levels(bids), "asks":levels(asks), "hash":"snapshot"
        }))
        .unwrap()
    }

    pub(super) fn changes(entries: serde_json::Value) -> StreamMessage {
        serde_json::from_value(json!({
            "event_type":"price_change", "market":"market", "timestamp":"1000",
            "price_changes":entries
        }))
        .unwrap()
    }

    fn book(bids: &[(&str, &str)], asks: &[(&str, &str)]) -> Book {
        let mut book = Book::default();
        book.apply("123", snapshot(bids, asks)).unwrap();
        book
    }

    pub(super) fn delta(price: &str, size: Option<&str>) -> StreamMessage {
        changes(json!([{"asset_id":"123","price":price,"size":size,"side":"BUY"}]))
    }

    #[test]
    fn snapshots_replace_both_sides_and_sort_prices() {
        let mut book = book(
            &[("0.1", "20"), ("0.45", "30")],
            &[("0.8", "40"), ("0.55", "50")],
        );
        assert_eq!(book.best_prices(), (Some(dec!(0.45)), Some(dec!(0.55))));
        assert_eq!(book.spread_mid(), (Some(dec!(0.1)), Some(dec!(0.5))));
        // Same-timestamp snapshots are distinct states in arrival order.
        book.apply("123", snapshot(&[], &[("0.6", "5")])).unwrap();
        assert_eq!(book.best_prices(), (None, Some(dec!(0.6))));
        assert_eq!(book.spread_mid(), (None, None));
    }

    #[test]
    fn deltas_set_absolute_sizes_keep_precision_and_reveal_deeper_levels() {
        let mut book = book(&[("0.4", "10"), ("0.45", "30")], &[("0.55", "50")]);
        book.apply(
            "123",
            changes(json!([
                {"asset_id":"123","price":"0.4","size":"1.123456","side":"BUY"},
                {"asset_id":"123","price":"0.45","size":"0","side":"BUY"},
                {"asset_id":"999","price":"0.99","size":"900","side":"BUY"},
                {"asset_id":"123","price":"0.55","size":"22","side":"SELL"}
            ])),
        )
        .unwrap();
        assert_eq!(book.best_prices(), (Some(dec!(0.4)), Some(dec!(0.55))));
        assert_eq!(book.bids[&dec!(0.4)], dec!(1.123456));
        assert_eq!(book.asks[&dec!(0.55)], dec!(22));
        assert_eq!(book.spread_mid(), (Some(dec!(0.15)), Some(dec!(0.475))));
    }

    #[test]
    fn reconnect_waits_for_snapshot_and_ignores_other_tokens() {
        let mut book = Book::default();
        let delta = || delta("0.4", Some("10"));
        assert!(!book.apply("123", delta()).unwrap());
        assert!(!book.ready);
        assert!(!book.apply("999", snapshot(&[], &[])).unwrap());
        book.apply("123", snapshot(&[], &[])).unwrap();
        assert!(book.apply("123", delta()).unwrap());
        book = Book::default(); // Every connection starts with this state.
        assert!(!book.apply("123", delta()).unwrap());
        assert!(book.bids.is_empty());
    }

    #[test]
    fn invalid_batch_never_silently_loses_an_update() {
        assert!(decode_events(
            r#"[{"event_type":"future_event"},{"event_type":"book","asset_id":"123"}]"#
        )
        .is_err());
        assert!(matches!(
            decode_events(r#"{"event_type":"future_event"}"#).unwrap()[0],
            StreamMessage::Unknown
        ));
        let mut book = book(&[], &[]);
        assert!(book
            .apply(
                "123",
                changes(json!([
                    {"asset_id":"123","price":"0.4","size":"10","side":"BUY"},
                    {"asset_id":"123","price":"0.5","side":"BUY"}
                ]))
            )
            .is_err());
        assert!(book.bids.is_empty());
    }

    #[test]
    fn null_collections_are_rejected_and_invalid_snapshots_preserve_state() {
        assert!(decode_events(r#"{"event_type":"book","asset_id":"123","market":"market","timestamp":"1000","bids":null,"asks":[]}"#).is_err());
        assert!(decode_events(r#"{"event_type":"price_change","market":"market","timestamp":"1000","price_changes":null}"#).is_err());
        let mut book = book(&[("0.4", "10")], &[]);
        assert!(book.apply("123", snapshot(&[], &[("1.1", "10")])).is_err());
        assert_eq!(book.best_prices(), (Some(dec!(0.4)), None));
        assert_eq!(book.updates, 1);
    }

    #[test]
    fn cli_rejects_invalid_tokens_and_terminal_controls_are_removed() {
        assert_eq!(parse_token(&[]).unwrap(), None);
        assert_eq!(
            parse_token(&["--token-id".into(), "123".into()]).unwrap(),
            Some("123")
        );
        for args in [
            vec!["--token-id"],
            vec!["--token-id", ""],
            vec!["--token-id", "abc"],
            vec!["--unknown"],
        ] {
            assert!(parse_token(&args.into_iter().map(str::to_owned).collect::<Vec<_>>()).is_err());
        }
        assert_eq!(clean("hello\x1b\nworld", 30), "hello  world");
        assert_eq!(clean("abcdef", 4), "abc…");
    }

    #[test]
    fn changed_cells_flash_briefly_without_flashing_the_initial_snapshot() {
        let mut book = book(&[("0.4", "10")], &[]);
        let mut history = DisplayHistory::default();
        let start = Instant::now();
        history.observe(&book, 5, start);
        assert!(!history.rows[0][0].changed(start));
        book.apply("123", delta("0.4", Some("20"))).unwrap();
        let changed = start + Duration::from_millis(100);
        history.observe(&book, 5, changed);
        assert!(!recent(history.rows[0][0].price_changed, changed));
        assert!(recent(history.rows[0][0].size_changed, changed));
        let row = &history.rows[0][0];
        assert_eq!(
            row.cells(None, changed),
            ("     0.4".into(), "          20".into())
        );
        let (price, size) = row.cells(Some(121), changed);
        assert_eq!(price, "\x1b[38;5;121m     0.4\x1b[0m");
        assert_eq!(size, "\x1b[30;48;5;121m          20\x1b[0m");
        let later = changed + HIGHLIGHT_DURATION;
        history.observe(&book, 5, later);
        assert!(!history.rows[0][0].changed(later));
    }

    #[tokio::test]
    async fn shared_feed_handles_heartbeats_cancelled_reads_and_malformed_batches() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let (release, released) = tokio::sync::oneshot::channel();
        let server = async {
            let (tcp, _) = listener.accept().await?;
            let mut socket = tokio_tungstenite::accept_async(tcp).await?;
            let subscription = socket.next().await.context("No subscription")??;
            let subscription: serde_json::Value = serde_json::from_str(subscription.to_text()?)?;
            assert_eq!(subscription["assets_ids"], json!(["123"]));
            assert_eq!(subscription["initial_dump"], true);
            assert_eq!(socket.next().await.unwrap()?, Message::Text("PING".into()));
            socket.send(Message::Text("PONG".into())).await?;
            socket.send(Message::Ping(vec![1, 2])).await?;
            assert_eq!(socket.next().await.unwrap()?, Message::Pong(vec![1, 2]));
            released.await?;
            socket
                .send(Message::Text(r#"[{"event_type":"future_event"}]"#.into()))
                .await?;
            socket
                .send(Message::Text(
                    r#"[{"event_type":"future_event"},{"event_type":"book"}]"#.into(),
                ))
                .await?;
            Ok::<_, anyhow::Error>(())
        };
        let client = async {
            let mut events = Box::pin(subscribe(&url, &["123".into()]).await?);
            assert!(timeout(Duration::from_millis(5), events.next())
                .await
                .is_err());
            release.send(()).unwrap();
            assert!(matches!(
                events.next().await.unwrap()?[0],
                StreamMessage::Unknown
            ));
            assert!(events.next().await.unwrap().is_err());
            assert!(events.next().await.is_none());
            Ok::<_, anyhow::Error>(())
        };
        timeout(Duration::from_secs(3), async {
            tokio::try_join!(server, client)
        })
        .await??;
        Ok(())
    }

    #[test]
    fn update_rate_uses_real_counts_decays_when_quiet_and_resets_on_disconnect() {
        let mut book = Book {
            ready: true,
            updates: 1,
            ..Book::default()
        };
        let mut history = DisplayHistory::default();
        let start = Instant::now();
        for (seconds, updates, expected) in [
            (0, 1, 0.0),
            (1, 11, 10.0),
            (2, 21, 10.0),
            (6, 21, 2.0),
            (7, 21, 0.0),
        ] {
            book.updates = updates;
            assert_eq!(
                history.observe(&book, 5, start + Duration::from_secs(seconds)),
                expected
            );
        }
        assert_eq!(
            history.observe(&Book::default(), 5, start + Duration::from_secs(8)),
            0.0
        );
        assert!(history.samples.is_empty());
        book.updates = 1;
        assert_eq!(
            history.observe(&book, 5, start + Duration::from_secs(9)),
            0.0
        );
    }
}
