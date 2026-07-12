//! `kdp-process` — offline processor: capture JSONL -> compressed columnar files.
//!
//! Reads a `kdp-cli capture` output tree and, per ticker, writes a set of
//! columnar tables (Parquet by default, Feather with `--format feather`) plus a
//! `manifest.json`. The lossless `book_events` table *replaces* the raw orderbook
//! JSONL; once the manifest reports `complete: true` and the outputs are
//! verified, the raw capture files can be dropped. See
//! `docs/dev-context/phase-3-processing.md`.
//!
//! Usage:
//! ```text
//! kdp-process --in <capture-dir> [--tickers A,B | --all] [--out <dir>] [--format parquet|feather]
//! ```

mod args;
mod inspect;
mod manifest;
mod process;
mod reader;

mod tables;
mod verify;
mod writer;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};

use args::Args;
use writer::Format;

fn main() -> ExitCode {
    init_tracing();
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // `{:#}` chains the anyhow context so the cause is visible.
            eprintln!("kdp-process: error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let args = Args::parse(std::env::args().skip(1));
    if args.has("help") || args.has("h") {
        print_usage();
        return Ok(());
    }

    // Inspect mode: peek at a single produced table instead of processing.
    if let Some(file) = args.get("head") {
        let path = PathBuf::from(file);
        let format = match args.get("format") {
            Some(f) => Format::parse(f)?,
            None => inspect::format_from_ext(&path)?,
        };
        let rows = args
            .get("rows")
            .and_then(|r| r.parse::<usize>().ok())
            .unwrap_or(5);
        return inspect::head(&path, rows, format);
    }

    let in_dir = PathBuf::from(
        args.get("in")
            .context("missing required --in <capture-dir> (try --help)")?,
    );
    if !in_dir.is_dir() {
        bail!("--in {} is not a directory", in_dir.display());
    }

    let format = Format::parse(args.get("format").unwrap_or("parquet"))?;
    let out_dir = match args.get("out") {
        Some(o) => PathBuf::from(o),
        None => default_out_dir(&in_dir),
    };

    // Ticker selection: explicit --tickers narrows; --all or absence = everything.
    let only: Option<Vec<String>> = match args.get("tickers") {
        Some(_) if args.has("all") => {
            bail!("pass either --tickers or --all, not both");
        }
        Some(list) => Some(
            list.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        ),
        None => None,
    };

    tracing::info!(
        in_dir = %in_dir.display(),
        out_dir = %out_dir.display(),
        ?format,
        "processing capture directory"
    );

    let outcomes = process::run(&in_dir, &out_dir, only.as_deref(), format)?;
    print_summary(&in_dir, &out_dir, format, &outcomes);
    Ok(())
}

/// Default output dir: a sibling of `<in>` named `<in>_processed`.
fn default_out_dir(in_dir: &Path) -> PathBuf {
    let name = in_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("capture");
    let parent = in_dir.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!("{name}_processed"))
}

fn print_summary(
    in_dir: &Path,
    out_dir: &Path,
    format: Format,
    outcomes: &[process::TickerOutcome],
) {
    if outcomes.is_empty() {
        println!(
            "kdp-process: no tickers processed (no capture files found under {})",
            in_dir.display()
        );
        return;
    }
    println!(
        "Processed {} ticker(s) -> {}\n",
        outcomes.len(),
        out_dir.display()
    );
    for o in outcomes {
        let safety = if o.complete {
            "safe to drop raw"
        } else {
            "REVIEW read_errors before dropping raw"
        };
        println!(
            "  {:<32} ob_msgs={:<7} book_events={:<8} trades={:<6} gaps={} raw={} read_errors={}  [{}]",
            o.ticker,
            o.counts.book_top,
            o.counts.book_events,
            o.counts.trades,
            o.counts.gaps,
            o.counts.raw,
            o.read_errors,
            safety,
        );
    }
    println!(
        "\nFormat: {} ({}). Each ticker also has a manifest.json with full provenance.",
        format.extension(),
        match format {
            Format::Parquet => "ZSTD-compressed, queryable, archival",
            Format::Feather => "Arrow IPC, zero-copy/fast reload",
        }
    );
}

fn print_usage() {
    println!(
        "kdp-process - convert kdp capture JSONL into compressed columnar tables\n\
         \n\
         USAGE:\n  \
           kdp-process --in <capture-dir> [--tickers A,B | --all] [--out <dir>] [--format parquet|feather]\n  \
           kdp-process --head <table-file> [--rows N] [--format parquet|feather]\n\
         \n\
         PROCESS FLAGS:\n  \
           --in <dir>        Capture directory (<dir>/<ticker>/<channel>/<date>.jsonl)   [required]\n  \
           --tickers A,B     Comma-separated tickers to process (default: all discovered)\n  \
           --all             Process every discovered ticker (the default)\n  \
           --out <dir>       Output directory (default: <in>_processed alongside --in)\n  \
           --format <fmt>    parquet (default) or feather\n\
         \n\
         INSPECT FLAGS:\n  \
           --head <file>     Print a produced table's schema + first rows as JSON\n  \
           --rows N          How many rows --head prints (default 5)\n  \
           --help            Show this help\n\
         \n\
         Per ticker it writes book_events / book_top / trades / gaps (+ raw if any)\n\
         and a manifest.json. The lossless book_events table replaces the raw\n\
         orderbook JSONL once manifest.complete is true and the output is verified."
    );
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // try_init is fallible only if a global subscriber already exists; ignore.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}
