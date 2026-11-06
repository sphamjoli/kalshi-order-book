//! Book and unit behaviour through the public domain API.

use engineering_challenge::Side;
use engineering_challenge::domain::{Book, BookError, Level, Price, Quantity, QuantityDelta};

fn price(raw: i64) -> Price {
    Price::try_new(raw).unwrap()
}

fn level(raw_price: i64, raw_quantity: i64) -> Level {
    Level {
        price: price(raw_price),
        quantity: Quantity::try_new(raw_quantity).unwrap(),
    }
}

fn book() -> Book {
    Book::from_levels([level(40, 12)], [level(45, 7)]).unwrap()
}

#[test]
fn signed_changes_report_absolute_quantities_and_remove_at_zero() {
    let mut book = book();
    let first = book
        .apply(Side::Sell, price(45), QuantityDelta::new(-2))
        .unwrap();
    assert_eq!(first.quantity.raw(), 5);
    let second = book
        .apply(Side::Sell, price(45), QuantityDelta::new(-5))
        .unwrap();
    assert_eq!(second.quantity, Quantity::ZERO);
    assert_eq!(book.asks().count(), 0);
}

#[test]
fn positive_change_creates_a_missing_level() {
    let mut book = book();
    book.apply(Side::Buy, price(38), QuantityDelta::new(3))
        .unwrap();
    assert_eq!(
        book.bids().collect::<Vec<_>>(),
        [level(40, 12), level(38, 3)]
    );
}

#[test]
fn negative_result_is_rejected_and_leaves_the_book_unchanged() {
    let mut changed = book();
    let error = changed
        .apply(Side::Sell, price(45), QuantityDelta::new(-9))
        .unwrap_err();
    assert_eq!(error, BookError::NegativeQuantity(-2));
    assert_eq!(changed, book());

    let error = changed
        .apply(Side::Buy, price(38), QuantityDelta::new(-1))
        .unwrap_err();
    assert_eq!(error, BookError::NegativeQuantity(-1), "a missing level");
    assert_eq!(changed, book());
}

#[test]
fn overflow_is_rejected_and_leaves_the_book_unchanged() {
    let mut changed = book();
    let error = changed
        .apply(Side::Sell, price(45), QuantityDelta::new(i64::MAX))
        .unwrap_err();
    assert_eq!(error, BookError::QuantityOverflow);
    assert_eq!(changed, book());
}

#[test]
fn levels_are_returned_best_first_from_an_unordered_snapshot() {
    let book = Book::from_levels(
        [level(38, 1), level(40, 1), level(39, 1)],
        [level(47, 1), level(45, 1), level(46, 1)],
    )
    .unwrap();
    let prices = |levels: Vec<Level>| levels.iter().map(|l| l.price.raw()).collect::<Vec<_>>();
    assert_eq!(prices(book.bids().collect()), [40, 39, 38]);
    assert_eq!(prices(book.asks().collect()), [45, 46, 47]);
}

#[test]
fn zero_change_at_a_missing_level_reports_it_absent() {
    let mut changed = book();
    let change = changed
        .apply(Side::Buy, price(38), QuantityDelta::new(0))
        .unwrap();
    assert_eq!(change.quantity, Quantity::ZERO);
    assert_eq!(changed, book());
}

#[test]
fn snapshot_treats_zero_levels_as_absent() {
    let book = Book::from_levels([level(40, 0)], []).unwrap();
    assert_eq!(book.bids().count(), 0);
}

#[test]
fn crossing_is_detected_at_and_beyond_equal_prices() {
    let crossed = |bid, ask| {
        Book::from_levels([level(bid, 1)], [level(ask, 1)])
            .unwrap()
            .is_crossed()
    };
    assert!(!crossed(44, 45));
    assert!(crossed(45, 45));
    assert!(crossed(46, 45));

    let book =
        Book::from_levels([level(10, 1), level(46, 1)], [level(90, 1), level(45, 1)]).unwrap();
    assert!(book.is_crossed(), "the best level of each side decides");
}

#[test]
fn a_one_sided_book_is_not_crossed() {
    assert!(!Book::from_levels([level(40, 1)], []).unwrap().is_crossed());
    assert!(!Book::from_levels([], [level(40, 1)]).unwrap().is_crossed());
}

#[test]
fn a_crossing_change_is_applied_and_can_clear() {
    let mut book = book();
    book.apply(Side::Sell, price(39), QuantityDelta::new(1))
        .unwrap();
    assert!(book.is_crossed());
    book.apply(Side::Sell, price(39), QuantityDelta::new(-1))
        .unwrap();
    assert!(!book.is_crossed());
}

#[test]
fn a_duplicate_level_is_rejected_even_at_zero() {
    for levels in [
        [level(40, 1), level(40, 2)],
        [level(40, 0), level(40, 2)],
        [level(40, 2), level(40, 0)],
    ] {
        let error = Book::from_levels(levels, []).unwrap_err();
        assert_eq!(error, BookError::DuplicateLevel(40));
    }
}

#[test]
fn units_start_at_zero() {
    assert_eq!(Price::try_new(0).unwrap().raw(), 0);
    assert_eq!(Quantity::try_new(0), Ok(Quantity::ZERO));
    assert_eq!(Price::try_new(-1), Err(BookError::NegativePrice(-1)));
    assert_eq!(Quantity::try_new(-1), Err(BookError::NegativeQuantity(-1)));
}
