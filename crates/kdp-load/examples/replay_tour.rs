//! Tour of the full-depth replay API against the committed fixture.
//!
//! Run from the workspace root (no data of your own needed):
//!
//! ```text
//! cargo run -p kdp-load --example replay_tour
//! ```
//!
//! Point it at any kdp-processed per-ticker directory instead:
//!
//! ```text
//! cargo run -p kdp-load --example replay_tour -- path/to/TICKER-DIR
//! ```

use kdp_load::{BookReplayer, Loader, ReplayEvent, TradeSource};

fn dollars(micro: u32) -> f64 {
    f64::from(micro) / 1_000_000.0
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args().nth(1).unwrap_or_else(|| {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixture/KXBTCD-26JUL0306-T61699.99"
        )
        .to_string()
    });

    // 1. Open: the completeness verdict (R3) is decided here, not mid-stream.
    let loader = Loader::open(&dir)?;
    println!("opened {dir}");
    println!("completeness: {:?}", loader.completeness());
    // An incomplete directory refuses to iterate until acknowledged:
    // `Loader::open(dir)?.allow_incomplete()`.

    // 2. The full merged stream: deterministic, time-ordered, typed.
    let (mut snaps, mut deltas, mut ws, mut rest, mut gaps) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let (mut first_ts, mut last_ts) = (None, None);
    let mut replayer = BookReplayer::default();
    for ev in loader.events()? {
        let ev = ev?;
        first_ts.get_or_insert(ev.ts().0);
        last_ts = Some(ev.ts().0);
        match &ev {
            ReplayEvent::Snapshot { .. } => snaps += 1,
            ReplayEvent::Delta { .. } => deltas += 1,
            ReplayEvent::Trade { source, .. } => match source {
                TradeSource::Ws => ws += 1,
                TradeSource::RestBackfill => rest += 1,
            },
            ReplayEvent::Gap { .. } => gaps += 1,
        }
        replayer.apply(&ev); // fold everything into full-depth book state
    }
    let (Some(t_first), Some(t_last)) = (first_ts, last_ts) else {
        println!("stream is empty");
        return Ok(());
    };
    println!(
        "events: {snaps} snapshots, {deltas} deltas, {ws} WS + {rest} REST trades, {gaps} gaps \
         over {:.1} s",
        (t_last - t_first) as f64 / 1e6
    );
    // NB: on a settled market the stream's last timestamp can trail the
    // manifest's `last_recv_ts_us` by up to one verify interval — REST verify
    // observations are not replay events (see the data guide, "verify").

    // 3. Final full-depth state, replayed from the log.
    let book = replayer.book();
    println!(
        "final book: {} yes / {} no levels, underflows={}, suspect_gaps={}",
        book.yes.len(),
        book.no.len(),
        book.underflows,
        replayer.pending_gaps().len()
    );

    // 4. Point-in-time: the exact ladder at any instant, via `between`.
    //    The range opens with a synthetic snapshot = the book at t0.
    let t0 = t_first + (t_last - t_first) / 2;
    let mut range = loader.between(t0, t_last + 1)?;
    match range.next().transpose()? {
        Some(ReplayEvent::Snapshot {
            yes,
            no,
            synthetic: true,
            ..
        }) => {
            println!(
                "book at mid-capture t0 ({} yes / {} no levels):",
                yes.len(),
                no.len()
            );
            for l in yes.iter().rev().take(3) {
                println!(
                    "  yes {:>7.2} x {:>10.2}",
                    dollars(l.price.0),
                    l.qty.0 as f64 / 100.0
                );
            }
            for l in no.iter().rev().take(3) {
                println!(
                    "  no  {:>7.2} x {:>10.2}",
                    dollars(l.price.0),
                    l.qty.0 as f64 / 100.0
                );
            }
        }
        Some(ev) => println!("leading gap before the opener (book suspect): {ev:?}"),
        None => println!("empty range"),
    }
    println!("events remaining after t0: {}", range.count());
    Ok(())
}
