//! `discover` — find markets by ticker/title substring (e.g. the IPL ticker).
//!
//! Lists open markets whose ticker or title contains `--query`, ranked by
//! volume, so the operator can pick the ticker to capture. Public/no-auth.

use anyhow::Context;
use tracing::instrument;

use crate::args::Args;

/// `discover [--query SUBSTRING] [--series TICKER] [--status S] [--pages N] [--limit N]`
///
/// At least one of `--query` / `--series` is required. `--series` narrows
/// server-side (recommended — the open-markets list is dominated by combo
/// markets), e.g. `discover --series KXIPLGAME`. `--status` filters (`open`,
/// `finalized`, `settled`, …); omit it to see **all** statuses (needed to find
/// concluded markets — e.g. past IPL games are `finalized`, not `open`).
#[instrument(skip(args))]
pub async fn run_discover(args: &Args) -> anyhow::Result<()> {
    let query = args.get("query").unwrap_or("");
    let series = args.get("series");
    let status = args.get("status");
    if query.is_empty() && series.is_none() {
        anyhow::bail!("discover requires --query SUBSTRING and/or --series TICKER");
    }
    let max_pages: u32 = args
        .get_or("pages", "5")
        .parse()
        .context("--pages must be an integer")?;
    let limit: usize = args
        .get_or("limit", "40")
        .parse()
        .context("--limit must be an integer")?;

    let client = reqwest::Client::builder()
        .user_agent(concat!("kdp-cli/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building http client")?;

    let markets = kdp_kalshi::rest::discover_markets(&client, query, series, status, max_pages)
        .await
        .context("discovering markets")?;

    eprintln!(
        "\n{} market(s) matching query={:?} series={:?} status={:?}:",
        markets.len(),
        query,
        series,
        status
    );
    eprintln!("{:>12}  {:<34}  {}", "volume", "ticker", "title");
    for m in markets.iter().take(limit) {
        eprintln!(
            "{:>12.0}  {:<34}  {}",
            m.volume(),
            m.ticker.as_str(),
            m.title
        );
    }
    if markets.len() > limit {
        eprintln!("... ({} more; raise --limit)", markets.len() - limit);
    }
    eprintln!();
    Ok(())
}
