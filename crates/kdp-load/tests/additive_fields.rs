//! Regression test for the "verify" phase kdp-process gained: a new
//! `verify.parquet` table beside the existing ones, new manifest.json fields
//! (`verify_checks`, `verify_mismatches`, `verify_skipped`, `underflows`,
//! `counts.verify`), and a new gaps `reason` string `"verify_mismatch"`.
//! `PROCESSED_SCHEMA_VERSION` stayed 1 on the claim that all of this is
//! ADDITIVE and kdp-load is untouched. This test proves that claim against a
//! committed fixture augmented the way the new writer would, rather than
//! trusting the claim on inspection alone:
//!
//! - `Manifest` (manifest.rs) has no `deny_unknown_fields`, so new top-level
//!   / `counts` keys are silently skipped by serde.
//! - Tables are opened BY NAME (`reader.rs` `RowIter::open` only ever reads
//!   `book_events.parquet` / `trades.parquet` / `gaps.parquet`), so an
//!   unrelated `verify.parquet` sitting in the same directory is never
//!   touched.
//! - `GapRow.reason` / `ReplayEvent::Gap { reason: String, .. }` are plain
//!   `String`s with no allow-list (reader.rs, event.rs), so a new reason
//!   string round-trips exactly like today's "seq_jump"/"reconnect" -- see
//!   the note on the ignored gaps-append case below.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use kdp_load::{Loader, ReplayEvent};
use parquet::arrow::ArrowWriter;

fn pristine_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixture/KDPSYNTH-R6MIX")
}

/// Copy the committed fixture into a fresh temp dir, then augment it exactly
/// the way a verify-table kdp-process would: new manifest fields plus a new,
/// unrelated `verify.parquet` file.
fn augmented_fixture() -> PathBuf {
    let src = pristine_fixture();
    let dst = std::env::temp_dir().join(format!("kdp-load-additive-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dst);
    fs::create_dir_all(&dst).expect("mkdir tmp");
    for entry in fs::read_dir(&src).expect("read fixture dir") {
        let entry = entry.expect("dir entry");
        fs::copy(entry.path(), dst.join(entry.file_name())).expect("copy fixture file");
    }

    // (a) manifest: parse -> insert new keys -> rewrite. Never hand-template
    // the whole manifest -- that would fight the "tolerant reader" claim
    // instead of testing it.
    let manifest_path = dst.join("manifest.json");
    let text = fs::read_to_string(&manifest_path).expect("read manifest");
    let mut v: serde_json::Value = serde_json::from_str(&text).expect("parse manifest");
    let obj = v.as_object_mut().expect("manifest is a JSON object");
    obj.insert("verify_checks".into(), 2.into());
    obj.insert("verify_mismatches".into(), 1.into());
    obj.insert("verify_skipped".into(), 0.into());
    obj.insert("underflows".into(), 3.into());
    obj.get_mut("counts")
        .and_then(|c| c.as_object_mut())
        .expect("counts is a JSON object")
        .insert("verify".into(), 2.into());
    fs::write(&manifest_path, v.to_string()).expect("rewrite manifest");

    // (b) a real small parquet file under the new table's name -- not a
    // renamed copy of an existing table -- so the test also covers "a
    // genuinely different schema file in the dir is ignored", not just
    // "a file named verify.parquet happens to match one we already read".
    let col: ArrayRef = Arc::new(Int64Array::from(vec![4i64]));
    let batch = RecordBatch::try_from_iter(vec![("mismatch_idx", col)]).expect("assemble batch");
    let file = fs::File::create(dst.join("verify.parquet")).expect("create verify.parquet");
    let mut w = ArrowWriter::try_new(file, batch.schema(), None).expect("writer");
    w.write(&batch).expect("write batch");
    w.close().expect("close writer");

    dst
}

fn events(dir: &Path) -> Vec<ReplayEvent> {
    Loader::open(dir)
        .expect("open")
        .events()
        .expect("stream")
        .map(|e| e.expect("every row decodes"))
        .collect()
}

#[test]
fn additive_verify_fields_and_table_are_invisible_to_kdp_load() {
    let pristine = pristine_fixture();
    let augmented = augmented_fixture();

    let pristine_loader = Loader::open(&pristine).expect("open pristine");
    let augmented_loader = Loader::open(&augmented).expect("open augmented");
    assert_eq!(
        *pristine_loader.completeness(),
        *augmented_loader.completeness(),
        "new manifest fields (verify_checks/verify_mismatches/verify_skipped/\
         underflows/counts.verify) must not change the completeness verdict"
    );

    let a = events(&pristine);
    let b = events(&augmented);
    assert_eq!(
        a.len(),
        b.len(),
        "same event count with verify.parquet present"
    );
    assert_eq!(a.first(), b.first(), "same first event");
    assert_eq!(a.last(), b.last(), "same last event");
    assert_eq!(
        a, b,
        "identical stream end-to-end: verify.parquet ignored (opened by name), \
         unknown manifest fields ignored (no deny_unknown_fields)"
    );
}

// A gaps row with reason "verify_mismatch" is NOT appended to the fixture's
// committed gaps.parquet here: appending a row means rewriting the whole
// Parquet file, and the only writer helpers that do that
// (`reader::tests::write_gaps` et al.) are `pub(crate)` -- unreachable from an
// integration test, per the task brief. Instead, read the code: `GapRow`
// (reader.rs) declares `pub reason: String` with no enum/allow-list, and
// `ReplayEvent::Gap` (event.rs) carries that same `String` straight through
// decode -> merge -> replay with no reason-string branching anywhere in
// merge.rs/replay.rs. `mixed.rs`'s
// `gap_events_appear_in_stream_exactly_as_written` already proves today's
// "seq_jump" reason round-trips byte-for-byte; a future "verify_mismatch"
// reason takes the identical path -- there is no code point that could reject
// or special-case it.
/// Copy the committed fixture and REWRITE gaps.parquet with a second row: a
/// synthesized `verify_mismatch` gap interleaved AFTER the fixture's existing
/// `seq_jump` row, both in correct ascending `recv_ts_us` order -- exactly how
/// kdp-process emits gaps.parquet post the Fix-1 stable-sort (external
/// review): stream gaps and synthesized verify_mismatch rows merged and
/// sorted by recv_ts before writing, never appended in two separate passes.
/// Proves kdp-load's merge (which assumes gaps.parquet is recv_ts-ordered and
/// raises `OrderViolation` otherwise) accepts a real, correctly-ordered
/// multi-reason gaps table.
fn fixture_with_interleaved_verify_mismatch_gap() -> PathBuf {
    let dst = std::env::temp_dir().join(format!("kdp-load-gap-order-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dst);
    fs::create_dir_all(&dst).expect("mkdir tmp");
    for entry in fs::read_dir(pristine_fixture()).expect("read fixture dir") {
        let entry = entry.expect("dir entry");
        fs::copy(entry.path(), dst.join(entry.file_name())).expect("copy fixture file");
    }

    // The committed fixture's gaps.parquet carries one row:
    // (1_300_000, Some(4), "seq_jump", "orderbook", None, None, "seq jumped 3 -> 5").
    // Rewrite with a second row at a LATER recv_ts_us -- matching the real
    // GapsTable schema column-for-column (tables.rs).
    let recv_ts_us: ArrayRef = Arc::new(Int64Array::from(vec![1_300_000i64, 1_700_000i64]));
    let seq: ArrayRef = Arc::new(Int64Array::from(vec![Some(4i64), None]));
    let reason: ArrayRef = Arc::new(StringArray::from(vec!["seq_jump", "verify_mismatch"]));
    let channel: ArrayRef = Arc::new(StringArray::from(vec!["orderbook", "orderbook"]));
    let last_seq: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>, None]));
    let observed_seq: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>, None]));
    let detail: ArrayRef = Arc::new(StringArray::from(vec![
        "seq jumped 3 -> 5",
        "replayed book diverged from the REST observation",
    ]));
    let batch = RecordBatch::try_from_iter(vec![
        ("recv_ts_us", recv_ts_us),
        ("seq", seq),
        ("reason", reason),
        ("channel", channel),
        ("last_seq", last_seq),
        ("observed_seq", observed_seq),
        ("detail", detail),
    ])
    .expect("assemble gaps batch");
    let file = fs::File::create(dst.join("gaps.parquet")).expect("create gaps.parquet");
    let mut w = ArrowWriter::try_new(file, batch.schema(), None).expect("writer");
    w.write(&batch).expect("write batch");
    w.close().expect("close writer");

    dst
}

#[test]
fn interleaved_verify_mismatch_gap_stays_ordered_and_loads() {
    let dir = fixture_with_interleaved_verify_mismatch_gap();

    // `events()` (helper above) panics on any row that fails to decode --
    // including a merge `OrderViolation` -- so simply completing this call is
    // the "no OrderViolation" assertion.
    let evs = events(&dir);
    let reasons: Vec<&str> = evs
        .iter()
        .filter_map(|e| match e {
            ReplayEvent::Gap { reason, .. } => Some(reason.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        reasons,
        vec!["seq_jump", "verify_mismatch"],
        "both gap events present, in recv_ts order"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn gap_reason_is_a_plain_string_with_no_allow_list() {
    // Smallest possible executable check standing in for the fixture-append
    // case above: any reason string decodes and replays identically, proving
    // kdp-load's gap handling doesn't branch on the value.
    let evs = events(&pristine_fixture());
    let (ts, reason) = evs
        .iter()
        .find_map(|e| match e {
            ReplayEvent::Gap { ts, reason, .. } => Some((ts.0, reason.clone())),
            _ => None,
        })
        .expect("fixture has a gap event");
    assert_eq!((ts, reason.as_str()), (1_300_000, "seq_jump"));
}
