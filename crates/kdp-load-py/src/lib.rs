//! Python bindings over `kdp-load` — the deterministic full-depth replay.
//!
//! Thin translation layer only: ordering, dedup, gap semantics, and the
//! replay rule all live in `kdp-load`; this crate converts its typed events
//! into Python dicts with integer units (ADR-004 — `price_micro`,
//! `qty_centi`; dollar floats are the consumer's display decision).
//!
//! ```python
//! from kdp_load import Loader, IncompleteData
//!
//! ld = Loader("path/to/TICKER-DIR")        # raises IncompleteData unless
//! ld = Loader(path, allow_incomplete=True) # acknowledged (R3)
//! for ev in ld.events(): ...               # merged, time-ordered dicts
//! ladder = ld.book_at(t_us)                # full depth at any instant
//! ```

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use ::kdp_core::{BookSide, Side};
use ::kdp_load::{Completeness, LoadError, ReplayEvent, TradeSource};

pyo3::create_exception!(
    kdp_load,
    IncompleteData,
    pyo3::exceptions::PyException,
    "The directory's manifest says the capture is incomplete (R3); pass allow_incomplete=True to acknowledge."
);

fn to_py_err(e: LoadError) -> PyErr {
    match e {
        LoadError::Incomplete { reasons } => {
            IncompleteData::new_err(format!("incomplete capture: {}", reasons.join("; ")))
        }
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

fn side_str(s: Side) -> &'static str {
    match s {
        Side::Yes => "yes",
        Side::No => "no",
    }
}

fn levels(ls: &[kdp_load::Level]) -> Vec<(u32, u64)> {
    ls.iter().map(|l| (l.price.0, l.qty.0)).collect()
}

fn ev_to_py(py: Python<'_>, ev: &ReplayEvent) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("ts_us", ev.ts().0)?;
    match ev {
        ReplayEvent::Snapshot {
            event_idx,
            yes,
            no,
            synthetic,
            ..
        } => {
            d.set_item("kind", "snapshot")?;
            d.set_item("event_idx", event_idx)?;
            d.set_item("yes", levels(yes))?;
            d.set_item("no", levels(no))?;
            d.set_item("synthetic", synthetic)?;
        }
        ReplayEvent::Delta {
            event_idx,
            side,
            price,
            delta,
            ..
        } => {
            d.set_item("kind", "delta")?;
            d.set_item("event_idx", event_idx)?;
            d.set_item("side", side_str(*side))?;
            d.set_item("price_micro", price.0)?;
            d.set_item("delta_centi", delta.0)?;
        }
        ReplayEvent::Trade {
            price,
            count,
            taker_side,
            taker_book_side,
            trade_id,
            source,
            ..
        } => {
            d.set_item("kind", "trade")?;
            d.set_item("price_micro", price.0)?;
            d.set_item("count_centi", count.0)?;
            d.set_item("taker_side", side_str(*taker_side))?;
            d.set_item(
                "taker_book_side",
                taker_book_side.map(|b| match b {
                    BookSide::Bid => "bid",
                    BookSide::Ask => "ask",
                }),
            )?;
            d.set_item("trade_id", trade_id.as_deref())?;
            d.set_item(
                "source",
                match source {
                    TradeSource::Ws => "ws",
                    TradeSource::RestBackfill => "rest_backfill",
                },
            )?;
        }
        ReplayEvent::Gap { reason, detail, .. } => {
            d.set_item("kind", "gap")?;
            d.set_item("reason", reason)?;
            d.set_item("detail", detail)?;
        }
    }
    Ok(d.into_any().unbind())
}

/// Iterator over replay events as dicts (wraps either stream shape).
#[pyclass(unsendable)]
struct EventStream {
    inner: Box<dyn Iterator<Item = Result<ReplayEvent, LoadError>>>,
}

#[pymethods]
impl EventStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
        match slf.inner.next() {
            None => Ok(None),
            Some(Err(e)) => Err(to_py_err(e)),
            Some(Ok(ev)) => Python::attach(|py| ev_to_py(py, &ev).map(Some)),
        }
    }
}

/// A processed per-ticker directory, opened and validated (R3 at open).
#[pyclass]
struct Loader {
    inner: kdp_load::Loader,
}

#[pymethods]
impl Loader {
    /// Open a kdp-processed ticker directory. An incomplete directory raises
    /// `IncompleteData` at first iteration unless `allow_incomplete=True`.
    #[new]
    #[pyo3(signature = (dir, allow_incomplete = false))]
    fn new(dir: String, allow_incomplete: bool) -> PyResult<Self> {
        let mut inner = kdp_load::Loader::open(dir).map_err(to_py_err)?;
        if allow_incomplete {
            inner = inner.allow_incomplete();
        }
        Ok(Loader { inner })
    }

    /// True when the manifest's completeness verdict is Complete.
    #[getter]
    fn complete(&self) -> bool {
        matches!(self.inner.completeness(), Completeness::Complete)
    }

    /// The incompleteness reasons (empty when complete).
    #[getter]
    fn reasons(&self) -> Vec<String> {
        match self.inner.completeness() {
            Completeness::Complete => Vec::new(),
            Completeness::Incomplete { reasons } => reasons.clone(),
        }
    }

    /// The full merged, deterministic, time-ordered event stream.
    fn events(&self) -> PyResult<EventStream> {
        let iter = self.inner.events().map_err(to_py_err)?;
        Ok(EventStream {
            inner: Box::new(iter),
        })
    }

    /// Range replay over `[t0_us, t1_us)`: leading unresolved-gap events (the
    /// book is suspect if any), then a synthetic snapshot = the full book at
    /// `t0_us`, then the events in range.
    fn between(&self, t0_us: i64, t1_us: i64) -> PyResult<EventStream> {
        let iter = self.inner.between(t0_us, t1_us).map_err(to_py_err)?;
        Ok(EventStream {
            inner: Box::new(iter),
        })
    }

    /// The full ladder at one instant:
    /// `{"ts_us", "yes": [(price_micro, qty_centi), ...] high->low? no — as
    /// stored (ascending price), "no": [...], "suspect_gaps": [gap dicts]}`.
    /// `suspect_gaps` non-empty means a capture hole since the last
    /// re-anchoring snapshot — treat the ladder with suspicion.
    fn book_at(&self, py: Python<'_>, t_us: i64) -> PyResult<Py<PyAny>> {
        let mut gaps: Vec<Py<PyAny>> = Vec::new();
        for ev in self.inner.between(t_us, t_us + 1).map_err(to_py_err)? {
            let ev = ev.map_err(to_py_err)?;
            match &ev {
                ReplayEvent::Gap { .. } => gaps.push(ev_to_py(py, &ev)?),
                ReplayEvent::Snapshot {
                    yes,
                    no,
                    synthetic: true,
                    ..
                } => {
                    let d = PyDict::new(py);
                    d.set_item("ts_us", t_us)?;
                    d.set_item("yes", levels(yes))?;
                    d.set_item("no", levels(no))?;
                    d.set_item("suspect_gaps", gaps)?;
                    return Ok(d.into_any().unbind());
                }
                // Nothing else can precede the synthetic opener.
                _ => break,
            }
        }
        Err(PyRuntimeError::new_err(
            "range replay ended before the synthetic opener (empty directory?)",
        ))
    }
}

#[pymodule(name = "kdp_load")]
fn bindings(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Loader>()?;
    m.add_class::<EventStream>()?;
    m.add("IncompleteData", py.get_type::<IncompleteData>())?;
    Ok(())
}
