use polyfill_rs::{
    fill::FillStatus, FillEngine, OrderBookImpl, OrderDelta, OrderRequest, OrderType, Side,
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

fn order(side: Side, price: Decimal, size: Decimal, order_type: OrderType) -> OrderRequest {
    OrderRequest {
        token_id: "token".into(),
        side,
        price,
        size,
        order_type,
        expiration: None,
        client_id: Some("limit".into()),
    }
}

#[test]
fn crossing_limit_does_not_invent_liquidity_or_charge_the_limit_price() {
    let book = book(Side::SELL, &[(dec!(0.50), dec!(1))]);
    let mut engine = FillEngine::new(dec!(1), dec!(2), 100);
    let result = engine
        .execute_limit_order(
            &order(Side::BUY, dec!(0.60), dec!(100), OrderType::GTC),
            &book,
        )
        .unwrap();
    assert_eq!(result.status, FillStatus::Partial);
    assert_eq!(result.total_size, dec!(1));
    assert_eq!(result.average_price, dec!(0.50));
    assert_eq!(result.total_cost, dec!(0.50));
    assert_eq!(result.fees, dec!(0.005));
    assert_eq!(engine.get_fills("limit").unwrap()[0].size, dec!(1));
}

#[test]
fn limit_sweep_respects_prices_sizes_and_partial_fills_on_both_sides() {
    for (side, opposing, prices) in [
        (Side::BUY, Side::SELL, [dec!(0.50), dec!(0.55), dec!(0.65)]),
        (Side::SELL, Side::BUY, [dec!(0.50), dec!(0.45), dec!(0.35)]),
    ] {
        let book = book(
            opposing,
            &[
                (prices[0], dec!(1)),
                (prices[1], dec!(2)),
                (prices[2], dec!(100)),
            ],
        );
        for (size, filled, status) in [
            (dec!(1), dec!(1), FillStatus::Filled),
            (dec!(2), dec!(2), FillStatus::Filled),
            (dec!(3), dec!(3), FillStatus::Filled),
            (dec!(4), dec!(3), FillStatus::Partial),
        ] {
            let mut engine = FillEngine::new(dec!(1), dec!(2), 100);
            let result = engine
                .execute_limit_order(&order(side, prices[1], size, OrderType::GTC), &book)
                .unwrap();
            assert_eq!(result.status, status);
            assert_eq!(result.total_size, filled);
            let cost = prices[0] + (filled - Decimal::ONE) * prices[1];
            assert_eq!(result.total_cost, cost);
            assert_eq!(result.average_price, cost / filled);
            assert_eq!(result.fees, cost / dec!(100));
            assert_eq!(result.fills[0].price, prices[0]);
            assert_eq!(
                result.fills.iter().map(|fill| fill.size).sum::<Decimal>(),
                filled
            );
            assert_eq!(
                result
                    .fills
                    .iter()
                    .map(|fill| fill.price * fill.size)
                    .sum::<Decimal>(),
                cost
            );
            assert!(result.fills.iter().all(|fill| match side {
                Side::BUY => fill.price <= prices[1],
                Side::SELL => fill.price >= prices[1],
            }));
        }
    }
}

#[test]
fn insufficient_fok_liquidity_does_not_record_partial_fills() {
    let book = book(Side::SELL, &[(dec!(0.50), dec!(1))]);
    for (kind, status, filled) in [
        (OrderType::FOK, FillStatus::Unfilled, Decimal::ZERO),
        (OrderType::FAK, FillStatus::Partial, Decimal::ONE),
    ] {
        let mut engine = FillEngine::new(dec!(1), dec!(2), 100);
        let result = engine
            .execute_limit_order(&order(Side::BUY, dec!(0.60), dec!(2), kind), &book)
            .unwrap();
        assert_eq!(result.status, status);
        assert_eq!(result.total_size, filled);
        assert_eq!(
            engine.get_all_fills().len(),
            usize::from(kind == OrderType::FAK)
        );
        if kind == OrderType::FOK {
            assert_eq!(result.total_cost, Decimal::ZERO);
            assert_eq!(result.fees, Decimal::ZERO);
        }
    }
}

#[test]
fn empty_and_non_crossing_books_are_unfilled() {
    for side in [Side::BUY, Side::SELL] {
        for levels in [vec![], vec![(dec!(0.50), dec!(10))]] {
            let opposing = if side == Side::BUY {
                Side::SELL
            } else {
                Side::BUY
            };
            let book = book(opposing, &levels);
            let price = if side == Side::BUY {
                dec!(0.49)
            } else {
                dec!(0.51)
            };
            let mut engine = FillEngine::new(dec!(1), dec!(2), 100);
            let result = engine
                .execute_limit_order(&order(side, price, dec!(10), OrderType::GTC), &book)
                .unwrap();
            assert_eq!(result.status, FillStatus::Unfilled);
            assert_eq!(result.total_size, Decimal::ZERO);
            assert_eq!(result.total_cost, Decimal::ZERO);
            assert_eq!(result.fees, Decimal::ZERO);
            assert!(result.fills.is_empty());
            assert!(engine.get_all_fills().is_empty());
        }
    }
}
