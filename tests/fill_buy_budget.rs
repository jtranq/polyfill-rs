use polyfill_rs::{
    fill::FillStatus, types::MarketOrderRequest, FillEngine, OrderBookImpl, OrderDelta, Side,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn book(side: Side, levels: &[(Decimal, Decimal)]) -> OrderBookImpl {
    let mut book = OrderBookImpl::new("token".into(), 100);
    for (index, &(price, size)) in levels.iter().enumerate() {
        book.apply_delta(OrderDelta {
            token_id: "token".into(),
            timestamp: chrono::Utc::now(),
            side,
            price,
            size,
            sequence: index as u64 + 1,
        })
        .unwrap();
    }
    book
}

#[test]
fn buy_amount_is_collateral_before_fees() {
    let book = book(Side::SELL, &[(dec!(0.50), dec!(300))]);
    let mut engine = FillEngine::new(dec!(1), dec!(100), 100);
    let order = MarketOrderRequest {
        token_id: "token".into(),
        side: Side::BUY,
        amount: dec!(100),
        slippage_tolerance: None,
        client_id: Some("buy".into()),
    };
    let result = engine.execute_market_order(&order, &book).unwrap();
    assert_eq!(result.status, FillStatus::Filled);
    assert_eq!(result.total_size, dec!(200));
    assert_eq!(result.total_cost, dec!(100));
    assert_eq!(result.average_price, dec!(0.50));
    assert_eq!(result.fees, dec!(1));
    assert_eq!(engine.get_fills("buy").unwrap()[0].size, dec!(200));
}

#[test]
fn market_buy_sweeps_budget_and_reports_partial_liquidity() {
    let book = book(
        Side::SELL,
        &[(dec!(0.25), dec!(100)), (dec!(0.50), dec!(200))],
    );
    for (amount, size, cost, status) in [
        (dec!(25), dec!(100), dec!(25), FillStatus::Filled),
        (dec!(75), dec!(200), dec!(75), FillStatus::Filled),
        (dec!(125), dec!(300), dec!(125), FillStatus::Filled),
        (dec!(150), dec!(300), dec!(125), FillStatus::Partial),
    ] {
        let mut engine = FillEngine::new(dec!(1), dec!(100), 0);
        let order = MarketOrderRequest {
            token_id: "token".into(),
            side: Side::BUY,
            amount,
            slippage_tolerance: None,
            client_id: None,
        };
        let result = engine.execute_market_order(&order, &book).unwrap();
        assert_eq!(result.status, status, "amount={amount}");
        assert_eq!(result.total_size, size);
        assert_eq!(result.total_cost, cost);
        assert_eq!(
            result
                .fills
                .iter()
                .map(|fill| fill.price * fill.size)
                .sum::<Decimal>(),
            cost
        );
    }
}

#[test]
fn market_sell_amount_remains_a_share_quantity() {
    let book = book(Side::BUY, &[(dec!(0.50), dec!(300))]);
    let mut engine = FillEngine::new(dec!(1), dec!(100), 100);
    let order = MarketOrderRequest {
        token_id: "token".into(),
        side: Side::SELL,
        amount: dec!(100),
        slippage_tolerance: None,
        client_id: None,
    };
    let result = engine.execute_market_order(&order, &book).unwrap();
    assert_eq!(result.status, FillStatus::Filled);
    assert_eq!(result.total_size, dec!(100));
    assert_eq!(result.total_cost, dec!(50));
    assert_eq!(result.fees, dec!(0.50));
}

#[test]
fn fractional_shares_do_not_leave_a_false_unfilled_budget() {
    let book = book(Side::SELL, &[(dec!(0.30), dec!(100))]);
    let mut engine = FillEngine::new(dec!(1), dec!(100), 0);
    let order = MarketOrderRequest {
        token_id: "token".into(),
        side: Side::BUY,
        amount: dec!(1),
        slippage_tolerance: None,
        client_id: None,
    };
    let result = engine.execute_market_order(&order, &book).unwrap();
    assert_eq!(result.status, FillStatus::Filled);
    assert_eq!(result.total_cost, dec!(1));
    assert!((result.total_size - dec!(1) / dec!(0.30)).abs() < dec!(0.00000001));
}
