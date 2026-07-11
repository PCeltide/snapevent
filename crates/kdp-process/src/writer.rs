//! Persist an Arrow [`RecordBatch`] as Parquet (default) or Feather (Arrow IPC).
//!
//! Both formats are produced from the *same* `RecordBatch`, so the only
//! difference is the final encoder. Parquet is the default for the
//! capture-and-archive goal — far smaller on disk (dictionary + RLE + ZSTD,
//! per-column) and queryable with predicate/row-group pushdown. Feather
//! (`--format feather`) is offered for fast zero-copy local reload at the cost of
//! size; see `docs/dev-context/phase-3-processing.md`.

use std::fs::File;
use std::path::Path;

use anyhow::{bail, Context, Result};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

/// Default ZSTD level: a good size/speed balance for archival output.
const ZSTD_LEVEL: i32 = 3;

/// Output columnar format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// Apache Parquet, ZSTD-compressed. Smaller, queryable, archival (default).
    Parquet,
    /// Feather (Arrow IPC file). Larger but zero-copy / fast to reload.
    Feather,
}

impl Format {
    /// Parse the `--format` value (case-insensitive).
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "parquet" => Ok(Format::Parquet),
            "feather" | "arrow" | "ipc" => Ok(Format::Feather),
            other => bail!("unknown --format {other:?} (expected `parquet` or `feather`)"),
        }
    }

    /// File extension for this format (no leading dot).
    pub fn extension(self) -> &'static str {
        match self {
            Format::Parquet => "parquet",
            Format::Feather => "feather",
        }
    }
}

/// Write `batch` to `path` in `format`, overwriting any existing file.
#[tracing::instrument(skip(batch), fields(rows = batch.num_rows(), path = %path.display()))]
pub fn write_batch(batch: &RecordBatch, path: &Path, format: Format) -> Result<()> {
    let file =
        File::create(path).with_context(|| format!("creating output file {}", path.display()))?;
    match format {
        Format::Parquet => {
            let level = ZstdLevel::try_new(ZSTD_LEVEL).context("invalid ZSTD level")?;
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(level))
                .build();
            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
                .context("opening Parquet writer")?;
            writer.write(batch).context("writing Parquet batch")?;
            writer.close().context("closing Parquet writer")?;
        }
        Format::Feather => {
            let mut writer =
                FileWriter::try_new(file, &batch.schema()).context("opening Feather writer")?;
            writer.write(batch).context("writing Feather batch")?;
            writer.finish().context("closing Feather writer")?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::ipc::reader::FileReader;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn sample() -> RecordBatch {
        RecordBatch::try_from_iter_with_nullable(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as arrow::array::ArrayRef,
                false,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as arrow::array::ArrayRef,
                false,
            ),
        ])
        .unwrap()
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("kdp_proc_{}_{}", std::process::id(), name));
        p
    }

    #[test]
    fn format_parses_and_extends() {
        assert_eq!(Format::parse("Parquet").unwrap(), Format::Parquet);
        assert_eq!(Format::parse("FEATHER").unwrap(), Format::Feather);
        assert_eq!(Format::parse("arrow").unwrap(), Format::Feather);
        assert!(Format::parse("csv").is_err());
        assert_eq!(Format::Parquet.extension(), "parquet");
        assert_eq!(Format::Feather.extension(), "feather");
    }

    #[test]
    fn parquet_round_trips_rows_and_values() {
        let path = temp_path("rt.parquet");
        write_batch(&sample(), &path, Format::Parquet).unwrap();

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let mut rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            rows += batch.num_rows();
            assert_eq!(ids.value(0), 1);
        }
        assert_eq!(rows, 3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn feather_round_trips_rows() {
        let path = temp_path("rt.feather");
        write_batch(&sample(), &path, Format::Feather).unwrap();

        let file = File::open(&path).unwrap();
        let reader = FileReader::try_new(file, None).unwrap();
        let mut rows = 0;
        for batch in reader {
            rows += batch.unwrap().num_rows();
        }
        assert_eq!(rows, 3);
        let _ = std::fs::remove_file(&path);
    }
}
