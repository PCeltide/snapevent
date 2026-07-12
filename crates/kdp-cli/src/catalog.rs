//! `catalog` — browse what's on Kalshi (category -> series -> live markets),
//! ranked by traded volume; `--series` emits a ready-to-run capture command.
//! Public/no-auth.

use anyhow::Context;
use kdp_kalshi::rest::{get_series, get_series_list, list_markets_page, Market, Series};
use tracing::instrument;

use crate::args::Args;

/// Group `series` by `category`, summing count + `volume()`. Sorted volume
/// desc, then category name asc on ties (deterministic).
fn category_rollup(series: &[Series]) -> Vec<(String, usize, f64)> {
    let mut by_category: std::collections::HashMap<String, (usize, f64)> =
        std::collections::HashMap::new();
    for s in series {
        let entry = by_category.entry(s.category.clone()).or_insert((0, 0.0));
        entry.0 += 1;
        entry.1 += s.volume();
    }
    let mut rows: Vec<(String, usize, f64)> = by_category
        .into_iter()
        .map(|(category, (count, volume))| (category, count, volume))
        .collect();
    // total_cmp everywhere volumes are sorted: parse::<f64> accepts "NaN",
    // and a partial_cmp-to-Equal comparator is then non-total, which sort_by
    // panics on when detected in current Rust.
    rows.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
    rows
}

/// Rank `series` by `volume()` desc, ticker asc on ties.
fn rank_series(series: &[Series]) -> Vec<&Series> {
    let mut ranked: Vec<&Series> = series.iter().collect();
    ranked.sort_by(|a, b| {
        b.volume()
            .total_cmp(&a.volume())
            .then_with(|| a.ticker.cmp(&b.ticker))
    });
    ranked
}

/// Top `n` `markets` by `volume_24h()` desc, ticker asc on ties.
fn top_by_24h(markets: &[Market], n: usize) -> Vec<&Market> {
    let mut ranked: Vec<&Market> = markets.iter().collect();
    ranked.sort_by(|a, b| {
        b.volume_24h()
            .total_cmp(&a.volume_24h())
            .then_with(|| a.ticker.cmp(&b.ticker))
    });
    ranked.truncate(n);
    ranked
}

/// The `--name` value for `capture-universe`: `series_ticker` lowercased with
/// every run of non-alphanumeric characters collapsed to a single `-`, and
/// leading/trailing `-` trimmed (e.g. `"KDPTEST-A.B12"` -> `"kdptest-a-b12"`).
fn universe_name(series_ticker: &str) -> String {
    let mut out = String::new();
    let mut last_was_dash = false;
    for c in series_ticker.chars() {
        if c.is_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            last_was_dash = false;
        } else if !last_was_dash {
            out.push('-');
            last_was_dash = true;
        }
    }
    out.trim_matches('-').to_string()
}

/// The ready-to-paste `capture-universe` suggestion footer for `series_ticker`.
fn suggestion(series_ticker: &str) -> String {
    let name = universe_name(series_ticker);
    format!(
        "\nTo capture this series continuously:\n\n  kdp-cli capture-universe --series {series_ticker} --name {name}\n\n\
         (add --max-units / --min-volume to bound breadth; see README. One-off or\n \
         long-lived series may need capture-scheduled or a --max-hours backstop.)"
    )
}

/// `catalog [--category NAME | --series TICKER] [--limit N]`
///
/// No selector: category rollup, ranked by summed series volume. `--category`:
/// series in that category, ranked by volume (`--limit` rows, default 25).
/// `--series`: drill-down into a single series' live markets (`--limit` top
/// movers, default 5) plus a ready-to-run `capture-universe` suggestion.
/// `--category` and `--series` are mutually exclusive.
#[instrument(skip(args))]
pub async fn run_catalog(args: &Args) -> anyhow::Result<()> {
    let category = args.get("category");
    let series_ticker = args.get("series");
    if category.is_some() && series_ticker.is_some() {
        anyhow::bail!("--category and --series are mutually exclusive");
    }
    // Per-view defaults: 25 ranked series, 5 top drill-down movers.
    let parse_limit = |default: &'static str| -> anyhow::Result<usize> {
        args.get_or("limit", default)
            .parse()
            .context("--limit must be an integer")
    };

    let client = reqwest::Client::builder()
        .user_agent(concat!("kdp-cli/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building http client")?;

    if let Some(ticker) = series_ticker {
        return run_series_drilldown(&client, ticker, parse_limit("5")?).await;
    }
    let limit = parse_limit("25")?;

    let series = get_series_list(&client, category)
        .await
        .context("listing kalshi series")?;

    match category {
        None => print_category_rollup(&series),
        Some(name) => print_category_series(&series, name, limit),
    }
    Ok(())
}

fn print_category_rollup(series: &[Series]) {
    let rollup = category_rollup(series);
    if rollup.is_empty() {
        eprintln!("no series returned by the venue.");
        return;
    }
    eprintln!();
    eprintln!("{:>7}  {:>14}  category", "series", "volume");
    for (category, count, volume) in &rollup {
        eprintln!("{count:>7}  {volume:>14.0}  {category}");
    }
    eprintln!();
}

fn print_category_series(series: &[Series], name: &str, limit: usize) {
    let ranked = rank_series(series);
    if ranked.is_empty() {
        eprintln!(
            "no series matched category {name:?} (case-sensitive; run 'kdp-cli catalog' to list categories)"
        );
        return;
    }
    eprintln!();
    eprintln!(
        "{:>14}  {:<10}  {:<24}  title",
        "volume", "frequency", "ticker"
    );
    for s in ranked.iter().take(limit) {
        eprintln!(
            "{:>14.0}  {:<10}  {:<24}  {}",
            s.volume(),
            s.frequency,
            s.ticker,
            s.title
        );
    }
    if ranked.len() > limit {
        eprintln!("... ({} more; raise --limit)", ranked.len() - limit);
    }
    eprintln!();
}

/// Bound on the drill-down's open-market pagination: 10 pages x 1000 =
/// 10k markets, far beyond any live series today (mirrors the universe
/// sweep's bound). Hitting it prints an explicit truncation line.
const DRILLDOWN_MAX_PAGES: usize = 10;

async fn run_series_drilldown(
    client: &reqwest::Client,
    ticker: &str,
    limit: usize,
) -> anyhow::Result<()> {
    // GET /series/{ticker} is authoritative for existence (a 404 is "no such
    // series"), unlike scanning the full list -- and it's one small request.
    let found = get_series(client, ticker).await.with_context(|| {
        format!("fetching series {ticker:?} (does it exist? run 'kdp-cli catalog' to browse)")
    })?;

    eprintln!();
    eprintln!("{}", found.title);
    eprintln!(
        "category: {}  frequency: {}",
        found.category, found.frequency
    );
    if let Some(tags) = &found.tags {
        if !tags.is_empty() {
            eprintln!("tags: {}", tags.join(", "));
        }
    }

    // Walk every page of open markets so the counts/sums below are totals,
    // never a silently-truncated first page.
    let mut markets = Vec::new();
    let mut cursor: Option<String> = None;
    let mut truncated = true;
    for _ in 0..DRILLDOWN_MAX_PAGES {
        let (page, next) = list_markets_page(
            client,
            1000,
            Some("open"),
            Some(ticker),
            None,
            None,
            cursor.as_deref(),
        )
        .await
        .context("listing open markets for series")?;
        markets.extend(page);
        match next {
            Some(c) => cursor = Some(c),
            None => {
                truncated = false;
                break;
            }
        }
    }
    if truncated {
        eprintln!(
            "(more than {} open markets; counts and volumes below cover the first {} only)",
            markets.len(),
            markets.len()
        );
    }

    if markets.is_empty() {
        eprintln!("no open markets right now (capture-universe re-discovers as they list)");
    } else {
        let sum_volume: f64 = markets.iter().map(Market::volume).sum();
        let sum_24h: f64 = markets.iter().map(Market::volume_24h).sum();
        eprintln!(
            "{} open market(s); lifetime volume {:.0}; 24h volume {:.0}",
            markets.len(),
            sum_volume,
            sum_24h
        );
        // deviation: design's `yes_sub_title` isn't on our Market struct; using `title`.
        eprintln!("{:>12}  {:<34}  title", "24h vol", "ticker");
        for m in top_by_24h(&markets, limit) {
            eprintln!("{:>12.0}  {:<34}  {}", m.volume_24h(), m.ticker, m.title);
        }
    }
    eprintln!("{}", suggestion(ticker));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_series(ticker: &str, category: &str, volume_fp: Option<&str>) -> Series {
        Series {
            ticker: ticker.to_string(),
            title: format!("{ticker} title"),
            category: category.to_string(),
            frequency: "hourly".to_string(),
            tags: None,
            volume_fp: volume_fp.map(str::to_string),
        }
    }

    fn mk_market(ticker: &str, volume_24h_fp: Option<&str>) -> Market {
        Market {
            ticker: kdp_core::Ticker(ticker.to_string()),
            title: format!("{ticker} title"),
            status: Some("open".into()),
            volume_fp: None,
            close_time: None,
            event_ticker: None,
            open_time: None,
            settlement_ts: None,
            occurrence_datetime: None,
            result: None,
            floor_strike: None,
            yes_bid_dollars: None,
            yes_ask_dollars: None,
            last_price_dollars: None,
            volume_24h_fp: volume_24h_fp.map(str::to_string),
        }
    }

    #[test]
    fn category_rollup_groups_sums_and_orders() {
        let series = vec![
            mk_series("A", "Crypto", Some("100.00")),
            mk_series("B", "Crypto", Some("50.00")),
            mk_series("C", "Sports", Some("150.00")),
            mk_series("D", "Weather", Some("150.00")),
        ];
        let rollup = category_rollup(&series);
        // Crypto: 2 series, 150.0 volume; Sports: 1, 150.0; Weather: 1, 150.0.
        // Sports/Weather tie on volume -> name asc puts Sports before Weather;
        // Crypto ties too -> "Crypto" < "Sports" alphabetically.
        assert_eq!(
            rollup,
            vec![
                ("Crypto".to_string(), 2, 150.0),
                ("Sports".to_string(), 1, 150.0),
                ("Weather".to_string(), 1, 150.0),
            ]
        );
    }

    #[test]
    fn rank_series_orders_by_volume_desc_ticker_asc() {
        let series = vec![
            mk_series("ZEBRA", "X", Some("10.00")),
            mk_series("APPLE", "X", Some("10.00")),
            mk_series("HIGH", "X", Some("999.00")),
            mk_series("NONE", "X", None),
        ];
        let ranked = rank_series(&series);
        let tickers: Vec<&str> = ranked.iter().map(|s| s.ticker.as_str()).collect();
        assert_eq!(tickers, vec!["HIGH", "APPLE", "ZEBRA", "NONE"]);
    }

    #[test]
    fn top_by_24h_orders_desc_and_truncates() {
        let markets = vec![
            mk_market("A", Some("10.00")),
            mk_market("B", Some("30.00")),
            mk_market("C", Some("20.00")),
            mk_market("D", Some("30.00")),
        ];
        let top = top_by_24h(&markets, 2);
        let tickers: Vec<&str> = top.iter().map(|m| m.ticker.as_str()).collect();
        // B and D tie at 30.0 -> ticker asc puts B first; truncated to 2.
        assert_eq!(tickers, vec!["B", "D"]);
    }

    #[test]
    fn nan_volume_sorts_deterministically_without_panic() {
        // parse::<f64> accepts "NaN"; total_cmp keeps the comparator total
        // (partial_cmp-to-Equal would be non-total and sort_by panics on
        // detection). NaN sorts as the largest value in descending order.
        let series = vec![
            mk_series("B", "X", Some("10.00")),
            mk_series("A", "X", Some("NaN")),
            mk_series("C", "X", Some("5.00")),
        ];
        let ranked = rank_series(&series);
        let tickers: Vec<&str> = ranked.iter().map(|s| s.ticker.as_str()).collect();
        assert_eq!(tickers, vec!["A", "B", "C"]);

        let markets = vec![
            mk_market("B", Some("10.00")),
            mk_market("A", Some("NaN")),
            mk_market("C", Some("5.00")),
        ];
        let top = top_by_24h(&markets, 3);
        let tickers: Vec<&str> = top.iter().map(|m| m.ticker.as_str()).collect();
        assert_eq!(tickers, vec!["A", "B", "C"]);
    }

    #[test]
    fn universe_name_slugifies() {
        assert_eq!(universe_name("KXBTCD"), "kxbtcd");
        assert_eq!(universe_name("KDPTEST-A.B12"), "kdptest-a-b12");
        assert_eq!(universe_name("--X--"), "x");
    }

    #[test]
    fn suggestion_contains_exact_capture_universe_line() {
        let s = suggestion("KXBTCD");
        assert!(s.contains("kdp-cli capture-universe --series KXBTCD --name kxbtcd"));
    }

    #[tokio::test]
    async fn run_catalog_rejects_both_category_and_series() {
        let args = Args::parse(
            ["catalog", "--category", "Crypto", "--series", "KXBTCD"]
                .iter()
                .map(|s| s.to_string()),
        );
        let err = run_catalog(&args).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "--category and --series are mutually exclusive"
        );
    }
}
