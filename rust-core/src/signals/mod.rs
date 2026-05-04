pub mod intramarket;
pub mod oracle_arb;
pub mod regime;

/// Direction of a trade signal — which side of a binary market to buy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Buy the "Up" (YES) token — bet that BTC closes higher than the strike
    Up,
    /// Buy the "Down" (NO) token — bet that BTC closes equal-or-lower than the strike
    Down,
}
