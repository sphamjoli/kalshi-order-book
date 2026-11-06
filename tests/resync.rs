//! The decision made about markets waiting for snapshots.

use engineering_challenge::resync::{ResyncDecision, ResyncWatch};

fn waiting(symbols: &[&str]) -> Vec<String> {
    symbols.iter().map(|symbol| (*symbol).to_owned()).collect()
}

#[test]
fn nothing_waiting_is_idle() {
    assert_eq!(
        ResyncWatch::default().review(&[], true),
        ResyncDecision::Idle
    );
}

#[test]
fn a_stall_with_no_live_books_rebuilds() {
    let mut watch = ResyncWatch::default();
    let stuck = waiting(&["M"]);
    assert_eq!(watch.review(&stuck, false), ResyncDecision::Retry);
    assert_eq!(watch.review(&stuck, false), ResyncDecision::Retry);
    assert_eq!(watch.review(&stuck, false), ResyncDecision::Rebuild);
}

#[test]
fn a_stall_beside_live_books_only_retries() {
    let mut watch = ResyncWatch::default();
    let stuck = waiting(&["M"]);
    watch.review(&stuck, true);
    watch.review(&stuck, true);
    assert_eq!(watch.review(&stuck, true), ResyncDecision::RetryOverdue);
}

#[test]
fn a_market_leaving_the_set_is_progress() {
    let mut watch = ResyncWatch::default();
    watch.review(&waiting(&["M", "N"]), false);
    watch.review(&waiting(&["M", "N"]), false);
    assert_eq!(watch.review(&waiting(&["M"]), false), ResyncDecision::Retry);
}

#[test]
fn added_markets_are_not_progress() {
    let mut watch = ResyncWatch::default();
    watch.review(&waiting(&["M"]), false);
    watch.review(&waiting(&["M", "N"]), false);
    assert_eq!(
        watch.review(&waiting(&["M", "N", "O"]), false),
        ResyncDecision::Rebuild
    );
}
