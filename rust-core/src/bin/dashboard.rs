// Sprint 8 / S8.2 — Operational dashboard binary.
//
// Run alongside the bot to monitor live state. Reads the JSONL ledgers
// every 2 seconds and prints a refreshing summary using ANSI escape
// codes (no curses / ratatui dep).
//
// Usage:
//   cargo run --release --bin dashboard
//   cargo run --release --bin dashboard -- --db data/db --interval-secs 2
//
// The bot writes to the same data/db directory; the dashboard reads from
// it. They never share an in-memory state — the JSONL file IS the
// communication channel.

use std::path::PathBuf;
use std::time::Duration;

use polymarket_bot::storage::dashboard::{from_path, DashboardSnapshot};
use polymarket_bot::storage::Position;

const ANSI_CLEAR: &str = "\x1b[2J\x1b[H";  // clear screen + cursor home
const ANSI_BOLD:  &str = "\x1b[1m";
const ANSI_RESET: &str = "\x1b[0m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_RED:   &str = "\x1b[31m";
const ANSI_DIM:   &str = "\x1b[2m";

fn parse_args() -> (PathBuf, u64) {
    let mut db = PathBuf::from("data/db");
    let mut interval = 2u64;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => {
                if let Some(v) = args.next() { db = PathBuf::from(v); }
            }
            "--interval-secs" => {
                if let Some(v) = args.next() {
                    if let Ok(n) = v.parse() { interval = n; }
                }
            }
            "--help" | "-h" => {
                eprintln!("Usage: dashboard [--db PATH] [--interval-secs N]");
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(2);
            }
        }
    }
    (db, interval)
}

fn pnl_color(pnl: f64) -> &'static str {
    if pnl > 0.0 { ANSI_GREEN }
    else if pnl < 0.0 { ANSI_RED }
    else { ANSI_RESET }
}

fn render(snap: &DashboardSnapshot) -> String {
    let mut buf = String::new();
    buf.push_str(ANSI_CLEAR);
    buf.push_str(&format!(
        "{bold}Constantine — Polymarket Bot Dashboard{reset}\n",
        bold = ANSI_BOLD, reset = ANSI_RESET,
    ));
    buf.push_str(&format!(
        "{dim}t={} ms{reset}\n\n",
        snap.now_ms, dim = ANSI_DIM, reset = ANSI_RESET,
    ));

    // Position counts
    buf.push_str(&format!("{bold}POSITIONS{reset}\n", bold = ANSI_BOLD, reset = ANSI_RESET));
    buf.push_str(&format!(
        "  total      = {:>4}    open  = {:>4}\n",
        snap.n_total(), snap.n_open(),
    ));
    buf.push_str(&format!(
        "  submitted  = {:>4}    filled = {:>4}\n",
        snap.n_submitted, snap.n_filled,
    ));
    buf.push_str(&format!(
        "  settled    = {:>4}    failed = {:>4}\n\n",
        snap.n_settled, snap.n_failed,
    ));

    // P&L
    buf.push_str(&format!("{bold}REALISED P&L{reset}\n", bold = ANSI_BOLD, reset = ANSI_RESET));
    buf.push_str(&format!(
        "  pnl        = {color}${:>+9.2}{reset}\n",
        snap.realised_pnl_usd, color = pnl_color(snap.realised_pnl_usd), reset = ANSI_RESET,
    ));
    buf.push_str(&format!(
        "  wins       = {:>4}    losses = {:>4}    win rate = {:.1}%\n",
        snap.n_wins, snap.n_losses, snap.win_rate * 100.0,
    ));
    buf.push_str(&format!(
        "  exposure   = ${:>9.2}\n\n",
        snap.open_exposure_usd,
    ));

    // Top positions
    buf.push_str(&format!("{bold}TOP POSITIONS BY |P&L|{reset}\n", bold = ANSI_BOLD, reset = ANSI_RESET));
    if snap.top_positions.is_empty() {
        buf.push_str(&format!("  {dim}(none yet){reset}\n", dim = ANSI_DIM, reset = ANSI_RESET));
    } else {
        for p in &snap.top_positions {
            buf.push_str(&render_position_line(p));
        }
    }

    buf.push_str(&format!(
        "\n{dim}refresh ~2s — Ctrl+C to exit{reset}\n",
        dim = ANSI_DIM, reset = ANSI_RESET,
    ));
    buf
}

fn render_position_line(p: &Position) -> String {
    let pnl = p.pnl_dollars.unwrap_or(0.0);
    let color = pnl_color(pnl);
    format!(
        "  {:<14} {:<5} ${:>5.2} @ {:.2}  →  {color}${:>+7.2}{reset}\n",
        truncate(&p.order_id, 14),
        p.side,
        p.bet_dollars,
        p.price,
        pnl,
        color = color, reset = ANSI_RESET,
    )
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n { s.to_string() } else { format!("{}…", &s[..n.saturating_sub(1)]) }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let (db, interval_secs) = parse_args();
    eprintln!("dashboard: reading from {} every {}s", db.display(), interval_secs);

    let mut tick = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tick.tick().await;
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        match from_path(&db, now_ms) {
            Ok(snap) => print!("{}", render(&snap)),
            Err(e) => eprintln!("dashboard: error reading {}: {e}", db.display()),
        }
    }
}
