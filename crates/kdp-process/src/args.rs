//! Minimal std-only `--key value` / `--flag` argument parser.
//!
//! `kdp-process` has no subcommand — it's a single command driven entirely by
//! flags — so this is a flags-only variant of `kdp-cli`'s parser (decision D10:
//! no `clap`, keep the dependency surface minimal). A bare `--flag` (no following
//! value, or followed by another `--`) records the value `"true"`.

use std::collections::HashMap;

/// Parsed `--key value` / `--flag` options.
#[derive(Debug, Default)]
pub struct Args {
    flags: HashMap<String, String>,
}

impl Args {
    /// Parse an argument iterator (already skipping the program name).
    pub fn parse(iter: impl Iterator<Item = String>) -> Self {
        let rest: Vec<String> = iter.collect();
        let mut flags = HashMap::new();
        let mut i = 0;
        while i < rest.len() {
            match rest[i].strip_prefix("--") {
                Some(key) if !key.is_empty() => {
                    if i + 1 < rest.len() && !rest[i + 1].starts_with("--") {
                        flags.insert(key.to_string(), rest[i + 1].clone());
                        i += 2;
                    } else {
                        flags.insert(key.to_string(), "true".to_string());
                        i += 1;
                    }
                }
                _ => i += 1,
            }
        }
        Self { flags }
    }

    /// The value of `--key`, if present (a bare `--flag` reads as `"true"`).
    pub fn get(&self, key: &str) -> Option<&str> {
        self.flags.get(key).map(String::as_str)
    }

    /// Whether `--key` was supplied at all.
    pub fn has(&self, key: &str) -> bool {
        self.flags.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(input: &[&str]) -> Args {
        Args::parse(input.iter().map(|s| s.to_string()))
    }

    #[test]
    fn parses_value_flags() {
        let a = args(&["--in", "data_mlb", "--format", "feather", "--out", "out"]);
        assert_eq!(a.get("in"), Some("data_mlb"));
        assert_eq!(a.get("format"), Some("feather"));
        assert_eq!(a.get("out"), Some("out"));
    }

    #[test]
    fn treats_bare_flag_as_true() {
        let a = args(&["--in", "data_mlb", "--all"]);
        assert_eq!(a.get("all"), Some("true"));
        assert!(a.has("all"));
        assert!(!a.has("tickers"));
    }

    #[test]
    fn missing_flag_is_none() {
        let a = args(&["--in", "x"]);
        assert_eq!(a.get("out"), None);
    }
}
