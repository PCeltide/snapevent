//! `--head` inspector: print a table's schema + first rows as JSON.
//!
//! A no-dependency way to peek at the processed output from the same binary
//! (handy when pandas/duckdb aren't installed). Reads a Parquet or Feather file
//! and writes the first `rows` records as newline-delimited JSON to stdout.

use std::fs::File;
use std::path::Path;

use anyhow::{bail, Context, Result};
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::writer::Format;

/// Infer the format from a file extension (`.parquet` / `.feather` / `.arrow`).
#[tracing::instrument(fields(path = %path.display()))]
pub fn format_from_ext(path: &Path) -> Result<Format> {
    match path.extension().and_then(|e| e.to_str()) {
        Some("parquet") => Ok(Format::Parquet),
        Some("feather") | Some("arrow") => Ok(Format::Feather),
        _ => bail!(
            "cannot infer format from {} — pass --format parquet|feather",
            path.display()
        ),
    }
}

/// Print the schema and the first `rows` records of `path` as JSON to stdout.
#[tracing::instrument(fields(path = %path.display(), rows, ?format))]
pub fn head(path: &Path, rows: usize, format: Format) -> Result<()> {
    let (schema_line, total, first) = read_first(path, rows, format)?;
    println!("# {}", path.display());
    println!("# schema: {schema_line}");
    match total {
        Some(n) => println!("# rows: {n} (showing first {})", rows.min(n as usize)),
        None => println!("# showing first {rows}"),
    }
    if let Some(batch) = first {
        let take = rows.min(batch.num_rows());
        if take > 0 {
            let slice = batch.slice(0, take);
            let mut buf = Vec::new();
            {
                let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
                writer.write(&slice).context("encoding rows as JSON")?;
                writer.finish().context("finishing JSON writer")?;
            }
            print!("{}", String::from_utf8_lossy(&buf));
        }
    }
    Ok(())
}

/// Returns `(schema summary, total rows if known, first batch)`.
fn read_first(
    path: &Path,
    rows: usize,
    format: Format,
) -> Result<(String, Option<i64>, Option<RecordBatch>)> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    match format {
        Format::Parquet => {
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .with_context(|| format!("opening Parquet {}", path.display()))?;
            let total = builder.metadata().file_metadata().num_rows();
            let schema_line = schema_summary(builder.schema());
            let mut reader = builder
                .with_batch_size(rows.max(1))
                .build()
                .context("building Parquet reader")?;
            let first = reader.next().transpose().context("reading first batch")?;
            Ok((schema_line, Some(total), first))
        }
        Format::Feather => {
            let mut reader = FileReader::try_new(file, None)
                .with_context(|| format!("opening Feather {}", path.display()))?;
            let schema_line = schema_summary(&reader.schema());
            let first = reader.next().transpose().context("reading first batch")?;
            Ok((schema_line, None, first))
        }
    }
}

fn schema_summary(schema: &arrow::datatypes::Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|f| format!("{}:{}", f.name(), f.data_type()))
        .collect::<Vec<_>>()
        .join(", ")
}
