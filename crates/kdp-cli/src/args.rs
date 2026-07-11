//! Minimal std-only `--key value` / `--flag` argument parser.
//!
//! The CLI's flag set is small, so a tiny parser (no `clap`) keeps the
//! dependency surface minimal and consistent with the rest of the binary
//! (decision D10). The first non-consumed argument is the subcommand; remaining
//! `--key value` pairs become flags, and a bare `--flag` with no following value
//! (or followed by another `--`) is treated as a boolean `"true"`.

use std::collections::HashMap;

/// Parsed command line: a subcommand plus `--key value` / `--flag` options.
///
/// A bare `--flag` (no following value) is recorded with the value `"true"`, so
/// presence is testable via `get(flag) == Some("true")`.
#[derive(Debug, Default)]
pub struct Args {
    /// The subcommand (first argument), if any.
    pub command: Option<String>,
    flags: HashMap<String, String>,
}

impl Args {
    /// Parse an argument iterator (already skipping the program name).
    pub fn parse(iter: impl Iterator<Item = String>) -> Self {
        let mut iter = iter;
        let command = iter.next();
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

        Self { command, flags }
    }

    /// The value of `--key`, if present (a bare `--flag` reads as `"true"`).
    pub fn get(&self, key: &str) -> Option<&str> {
        self.flags.get(key).map(String::as_str)
    }

    /// The value of `--key`, or `default` if absent.
    pub fn get_or<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.get(key).unwrap_or(default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(input: &[&str]) -> Args {
        Args::parse(input.iter().map(|s| s.to_string()))
    }

    #[test]
    fn parses_command_and_value_flags() {
        let a = args(&["ws-probe", "--tickers", "A,B", "--frames", "30"]);
        assert_eq!(a.command.as_deref(), Some("ws-probe"));
        assert_eq!(a.get("tickers"), Some("A,B"));
        assert_eq!(a.get("frames"), Some("30"));
    }

    #[test]
    fn treats_bare_flag_as_boolean_true() {
        let a = args(&["capture", "--verbose", "--out", "data"]);
        assert_eq!(a.get("verbose"), Some("true"));
        assert_eq!(a.get("out"), Some("data"));
    }

    #[test]
    fn get_or_falls_back_to_default() {
        let a = args(&["x"]);
        assert_eq!(a.get_or("frames", "30"), "30");
        assert_eq!(a.get("frames"), None);
    }

    #[test]
    fn no_command_is_none() {
        let a = args(&[]);
        assert!(a.command.is_none());
    }
}
