use polyfill_rs::{
    fill::FillStatus, types::MarketOrderRequest, FillEngine, OrderBookImpl, OrderDelta, Side,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn simulate(
    side: Side,
    second_price: Decimal,
    maximum: Decimal,
    tolerance: Option<Decimal>,
) -> (FillStatus, usize) {
    let mut book = OrderBookImpl::new("token".into(), 100);
    let opposing = if side == Side::BUY {
        Side::SELL
    } else {
        Side::BUY
    };
    for (index, price) in [dec!(0.50), second_price].into_iter().enumerate() {
        book.apply_delta(OrderDelta {
            token_id: "token".into(),
            timestamp: chrono::Utc::now(),
            side: opposing,
            price,
            size: dec!(10),
            sequence: index as u64 + 1,
        })
        .unwrap();
    }
    let mut engine = FillEngine::new(dec!(1), maximum, 100);
    let order = MarketOrderRequest {
        token_id: "token".into(),
        side,
        amount: dec!(20),
        slippage_tolerance: tolerance,
        client_id: Some("slippage".into()),
    };
    let result = engine.execute_market_order(&order, &book).unwrap();
    if result.status == FillStatus::Rejected {
        assert!(result.fills.is_empty());
        assert_eq!(result.total_size, Decimal::ZERO);
        assert_eq!(result.total_cost, Decimal::ZERO);
        assert_eq!(result.fees, Decimal::ZERO);
    }
    (result.status, engine.get_all_fills().len())
}

#[test]
fn twenty_percent_slippage_exceeds_two_percent_limit() {
    for (side, price) in [(Side::BUY, dec!(0.70)), (Side::SELL, dec!(0.30))] {
        assert_eq!(
            simulate(side, price, dec!(2), None),
            (FillStatus::Rejected, 0)
        );
    }
}

#[test]
fn request_tolerance_can_tighten_but_not_relax_engine_maximum() {
    for (side, price) in [(Side::BUY, dec!(0.70)), (Side::SELL, dec!(0.30))] {
        assert_eq!(
            simulate(side, price, dec!(25), Some(dec!(2))),
            (FillStatus::Rejected, 0)
        );
        assert_eq!(
            simulate(side, price, dec!(2), Some(dec!(25))),
            (FillStatus::Rejected, 0)
        );
        assert_ne!(
            simulate(side, price, dec!(20), Some(dec!(20))).0,
            FillStatus::Rejected
        );
    }
}

#[test]
fn percentage_boundary_is_inclusive() {
    for (side, price, too_far) in [
        (Side::BUY, dec!(0.52), dec!(0.5201)),
        (Side::SELL, dec!(0.48), dec!(0.4799)),
    ] {
        assert_ne!(simulate(side, price, dec!(2), None).0, FillStatus::Rejected);
        assert_eq!(
            simulate(side, too_far, dec!(2), None),
            (FillStatus::Rejected, 0)
        );
        assert_eq!(
            simulate(side, price, dec!(2), Some(Decimal::ZERO)),
            (FillStatus::Rejected, 0)
        );
    }
}

#[test]
fn negative_slippage_settings_are_invalid() {
    let book = OrderBookImpl::new("token".into(), 100);
    for (maximum, tolerance) in [(dec!(-1), None), (dec!(2), Some(dec!(-1)))] {
        let mut engine = FillEngine::new(dec!(1), maximum, 0);
        let order = MarketOrderRequest {
            token_id: "token".into(),
            side: Side::BUY,
            amount: dec!(20),
            slippage_tolerance: tolerance,
            client_id: None,
        };
        assert!(engine.execute_market_order(&order, &book).is_err());
    }
}
