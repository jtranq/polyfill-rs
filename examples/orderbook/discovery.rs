//! Select a public market by observed changes to the displayed order-book depth.

use super::{subscribe, Book, Display, Market, LEVELS, WS_URL};
use anyhow::{ensure, Context, Result};
use futures_util::StreamExt;
use polyfill_rs::StreamMessage;
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};
use tokio::time::timeout;

const CANDIDATE_LIMIT: usize = 96;
const SAMPLE_WINDOW: Duration = Duration::from_secs(6);
const GAMMA_URL: &str = "https://gamma-api.polymarket.com/markets/keyset";

pub(super) async fn discover(token_id: Option<&str>, display: &Display) -> Result<Market> {
    if let Some(id) = token_id {
        return Ok(Market {
            token_id: id.to_owned(),
            question: "Selected outcome token".to_owned(),
            outcome: "Custom token".to_owned(),
        });
    }

    let markets = gamma_candidates()
        .await
        .context("Market discovery failed; retry or use --token-id ID")?;
    ensure!(
        !markets.is_empty(),
        "No open market candidates found; try --token-id ID from an active market"
    );
    display.message(&format!(
        "Watching {} markets for {}s; measuring changes to the top {} levels…",
        markets.len(),
        SAMPLE_WINDOW.as_secs(),
        LEVELS,
    ))?;
    let mut candidates: BTreeMap<_, _> = markets
        .into_iter()
        .map(|market| (market.token_id.clone(), Candidate::new(market)))
        .collect();
    sample(&mut candidates, LEVELS).await?;
    let selected = candidates
        .into_values()
        .filter(Candidate::eligible)
        .max_by_key(Candidate::score)
        .context(
            "No valid two-sided book received during discovery; try again or supply --token-id ID",
        )?;
    display.message(&format!(
        "Selected {}: {} displayed depth changes in {}s (activity can change)",
        selected.market.question,
        selected.visible_changes,
        SAMPLE_WINDOW.as_secs(),
    ))?;
    Ok(selected.market)
}

pub(super) fn valid_token(id: &str) -> bool {
    !id.is_empty() && id.len() <= 78 && id.bytes().all(|c| c.is_ascii_digit())
}

fn string_array(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::String(encoded) => serde_json::from_str(encoded).ok(),
        Value::Array(values) => values
            .iter()
            .map(|value| value.as_str().map(str::to_owned))
            .collect(),
        _ => None,
    }
}

fn gamma_market(value: &Value) -> Option<Market> {
    for field in ["active", "enableOrderBook", "acceptingOrders"] {
        if !value.get(field)?.as_bool()? {
            return None;
        }
    }
    if value.get("closed")?.as_bool()?
        || value.get("archived").and_then(Value::as_bool) == Some(true)
    {
        return None;
    }
    let ids = string_array(value.get("clobTokenIds")?)?;
    let outcomes = string_array(value.get("outcomes")?)?;
    if ids.len() != outcomes.len() {
        return None;
    }
    let token_id = ids.first()?.clone();
    let outcome = outcomes.first()?.clone();
    if !valid_token(&token_id) || outcome.is_empty() {
        return None;
    }
    Some(Market {
        token_id,
        question: value.get("question")?.as_str()?.to_owned(),
        outcome,
    })
}

async fn gamma_candidates() -> Result<Vec<Market>> {
    let limit = CANDIDATE_LIMIT.to_string();
    let client = reqwest::Client::builder()
        .user_agent("polyfill-rs-orderbook-example/0.4.3")
        .timeout(Duration::from_secs(8))
        .build()?;
    let response: Value = client
        .get(GAMMA_URL)
        .query(&[
            ("closed", "false"),
            ("limit", limit.as_str()),
            ("order", "volume24hr"),
            ("ascending", "false"),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let markets = response
        .get("markets")
        .and_then(Value::as_array)
        .context("Unexpected public market-list response")?;
    let mut candidates = BTreeMap::new();
    for market in markets.iter().filter_map(gamma_market) {
        candidates.entry(market.token_id.clone()).or_insert(market);
        if candidates.len() == CANDIDATE_LIMIT {
            break;
        }
    }
    Ok(candidates.into_values().collect())
}

struct Candidate {
    market: Market,
    book: Book,
    visible_changes: u64,
    depth_changes: u64,
    invalid: bool,
}

impl Candidate {
    fn new(market: Market) -> Self {
        Self {
            market,
            book: Book::default(),
            visible_changes: 0,
            depth_changes: 0,
            invalid: false,
        }
    }

    fn apply(&mut self, event: StreamMessage, levels: usize) {
        if self.invalid {
            return;
        }
        let was_ready = self.book.ready;
        let before: Vec<_> = self.book.rows(levels).collect();
        let old_bids = self.book.bids.clone();
        let old_asks = self.book.asks.clone();
        match self.book.apply(&self.market.token_id, event) {
            Ok(true) if was_ready => {
                if old_bids != self.book.bids || old_asks != self.book.asks {
                    self.depth_changes += 1;
                    if !before.into_iter().eq(self.book.rows(levels)) {
                        self.visible_changes += 1;
                    }
                }
            },
            Err(_) => self.invalid = true,
            _ => {},
        }
    }

    fn eligible(&self) -> bool {
        !self.invalid
            && self.book.ready
            && matches!(self.book.best_prices(), (Some(bid), Some(ask)) if bid <= ask)
    }

    fn score(&self) -> (u64, u64) {
        (self.visible_changes, self.depth_changes)
    }
}

fn apply_event(candidates: &mut BTreeMap<String, Candidate>, event: StreamMessage, levels: usize) {
    match &event {
        StreamMessage::Book(snapshot) => {
            if let Some(candidate) = candidates.get_mut(&snapshot.asset_id) {
                candidate.apply(event, levels);
            }
        },
        StreamMessage::PriceChange(update) => {
            let mut ids: Vec<_> = update
                .price_changes
                .iter()
                .map(|change| change.asset_id.clone())
                .collect();
            ids.sort_unstable();
            ids.dedup();
            for id in ids {
                if let Some(candidate) = candidates.get_mut(&id) {
                    candidate.apply(event.clone(), levels);
                }
            }
        },
        StreamMessage::MarketResolved(resolved) => {
            for id in &resolved.asset_ids {
                if let Some(candidate) = candidates.get_mut(id) {
                    candidate.invalid = true;
                }
            }
        },
        _ => {},
    }
}

async fn sample(candidates: &mut BTreeMap<String, Candidate>, levels: usize) -> Result<()> {
    let tokens: Vec<_> = candidates.keys().cloned().collect();
    let mut events = Box::pin(subscribe(WS_URL, &tokens).await?);
    // Discovery state is discarded: the viewer starts from its own fresh snapshot.
    let scan = async {
        while let Some(batch) = events.next().await {
            for event in batch? {
                apply_event(candidates, event, levels);
            }
        }
        anyhow::bail!("Market stream ended during discovery")
    };
    match timeout(SAMPLE_WINDOW, scan).await {
        Err(_) => Ok(()),
        Ok(result) => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{changes, delta, snapshot as book_snapshot};
    use rust_decimal::Decimal;
    use serde_json::json;

    fn candidate() -> Candidate {
        Candidate::new(Market {
            token_id: "123".to_owned(),
            question: "Test market".to_owned(),
            outcome: "Yes".to_owned(),
        })
    }

    fn snapshot() -> StreamMessage {
        book_snapshot(&[("0.4", "10"), ("0.3", "20")], &[("0.6", "30")])
    }

    #[test]
    fn gamma_filters_closed_and_invalid_records_and_accepts_both_array_encodings() {
        let valid = json!({
            "active":true,"closed":false,"enableOrderBook":true,"acceptingOrders":true,
            "question":"Test", "clobTokenIds":"[\"123\",\"456\"]", "outcomes":["Yes","No"]
        });
        assert_eq!(gamma_market(&valid).unwrap().token_id, "123");
        let mut changed = valid.clone();
        changed["clobTokenIds"] = json!(["123", "456"]);
        changed["outcomes"] = json!("[\"Yes\",\"No\"]");
        assert!(gamma_market(&changed).is_some());
        for (field, value) in [
            ("closed", json!(true)),
            ("active", json!(false)),
            ("acceptingOrders", Value::Null),
            ("enableOrderBook", json!(false)),
            ("archived", json!(true)),
            ("clobTokenIds", json!(["abc", "456"])),
            ("outcomes", json!(["Yes"])),
        ] {
            let mut changed = valid.clone();
            changed[field] = value;
            assert!(gamma_market(&changed).is_none(), "accepted invalid {field}");
        }
    }

    #[test]
    fn initial_snapshots_noops_and_pre_snapshot_deltas_do_not_count() {
        let mut candidate = candidate();
        candidate.apply(delta("0.4", Some("11")), 1);
        assert!(!candidate.eligible());
        candidate.apply(snapshot(), 1);
        candidate.apply(snapshot(), 1);
        candidate.apply(delta("0.4", Some("10.000")), 1);
        assert!(candidate.eligible());
        assert_eq!(candidate.score(), (0, 0));
        // A deeper update is only the tiebreaker for a one-level display.
        candidate.apply(delta("0.3", Some("21")), 1);
        assert_eq!(candidate.score(), (0, 1));
        candidate.apply(delta("0.4", Some("0")), 1);
        assert_eq!(candidate.score(), (1, 2));
    }

    #[test]
    fn visible_changes_outrank_deep_churn_and_respect_display_depth() {
        let mut deep = candidate();
        deep.apply(snapshot(), 1);
        for size in ["21", "22", "23"] {
            deep.apply(delta("0.3", Some(size)), 1);
        }
        let mut visible = candidate();
        visible.apply(snapshot(), 1);
        visible.apply(delta("0.4", Some("11")), 1);
        assert!(visible.score() > deep.score());
        let mut two_levels = candidate();
        two_levels.apply(snapshot(), 2);
        two_levels.apply(delta("0.3", Some("21")), 2);
        assert_eq!(two_levels.score(), (1, 1));
    }

    #[test]
    fn multiasset_batches_route_once_per_token_and_ignore_other_assets() {
        let mut second = candidate();
        second.market.token_id = "456".to_owned();
        let mut second_snapshot = snapshot();
        if let StreamMessage::Book(book) = &mut second_snapshot {
            book.asset_id = "456".to_owned();
        }
        let mut candidates =
            BTreeMap::from([("123".to_owned(), candidate()), ("456".to_owned(), second)]);
        apply_event(&mut candidates, snapshot(), 1);
        apply_event(&mut candidates, second_snapshot, 1);
        let update = changes(json!([
                {"asset_id":"123","price":"0.4","size":"11","side":"BUY"},
                {"asset_id":"456","price":"0.4","size":"20","side":"BUY"},
                {"asset_id":"123","price":"0.4","size":"12","side":"BUY"},
                {"asset_id":"999","price":"0.99","size":"30","side":"BUY"}
        ]));
        apply_event(&mut candidates, update, 1);
        assert_eq!(candidates["123"].score(), (1, 1));
        assert_eq!(candidates["456"].score(), (1, 1));
        assert_eq!(
            candidates["123"].book.bids[&Decimal::new(4, 1)],
            Decimal::from(12)
        );
        assert_eq!(
            candidates["456"].book.bids[&Decimal::new(4, 1)],
            Decimal::from(20)
        );
        assert_eq!(candidates.len(), 2);
    }

    #[test]
    fn malformed_crossed_and_resolved_candidates_cannot_win() {
        let mut pre_snapshot = candidate();
        pre_snapshot.apply(delta("0.4", None), 1);
        pre_snapshot.apply(snapshot(), 1);
        assert!(!pre_snapshot.eligible());
        let mut malformed = candidate();
        malformed.apply(snapshot(), 1);
        malformed.apply(delta("0.4", None), 1);
        malformed.apply(snapshot(), 1);
        assert!(!malformed.eligible());
        let mut crossed = candidate();
        crossed.apply(snapshot(), 1);
        crossed.apply(delta("0.7", Some("10")), 1);
        assert!(!crossed.eligible());
        let mut missing_side = candidate();
        missing_side.apply(snapshot(), 1);
        missing_side.book.asks.clear();
        assert!(!missing_side.eligible());
        let resolved: StreamMessage = serde_json::from_value(json!({
            "event_type":"market_resolved", "id":"m", "question":"Test", "market":"m",
            "slug":"test", "assets_ids":["123"], "winning_asset_id":"123", "winning_outcome":"Yes", "timestamp":"1"
        }))
        .unwrap();
        let mut candidates = BTreeMap::from([("123".to_owned(), candidate())]);
        apply_event(&mut candidates, snapshot(), 1);
        apply_event(&mut candidates, resolved, 1);
        assert!(!candidates["123"].eligible());
    }
}
