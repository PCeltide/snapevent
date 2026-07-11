//! Lossless scaled-integer money & quantity units.
//!
//! Kalshi sends prices and sizes on the wire as fixed-point **decimal strings**
//! (`"0.0800"`, `"300.00"`). This module is the source of truth for turning those
//! strings into exact integers and back, and it is the reason kdp never lets a
//! float touch money (see ADR-004). Prices become [`MicroDollars`] (dollars x
//! 1e6, capturing the wire's 6-dp precision); sizes become centi-contracts
//! ([`RestingQty`] for resting, [`QtyDelta`] for signed change — contracts x
//! 100, capturing the 2-dp `_fp` precision).
//!
//! Parsing is **integer string surgery** — split on `.`, validate digits, scale,
//! parse — so no floating-point rounding ever occurs. A value that is empty,
//! non-numeric, too precise for its scale, out of range, or wrongly signed is a
//! typed [`FixedPointError`]; the caller persists a raw-fallback record rather
//! than dropping the message or silently substituting zero. (The Python
//! reference's `fp_to_int` silently returns 0 on bad input — deliberately not
//! copied.)

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Why a fixed-point decimal string could not be parsed into a scaled integer.
///
/// Each variant carries the offending input so the caller can attach it to a
/// raw-fallback record. There is deliberately no "defaulted to zero" outcome:
/// an unparseable money/quantity value is always an error, never a silent 0.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FixedPointError {
    /// The input string was empty.
    #[error("empty fixed-point string")]
    Empty,

    /// The input contained a character that is not a digit, sign, or single
    /// decimal point (or was otherwise malformed, e.g. a bare `-` or `.5`).
    #[error("not a numeric fixed-point value: {0:?}")]
    NotNumeric(String),

    /// The fractional part had more digits than the unit's scale can represent,
    /// which would lose precision — refused rather than rounded.
    #[error("too many decimal places (max {max}): {input:?}")]
    TooManyDecimals {
        /// Maximum fractional digits this unit can represent losslessly.
        max: u8,
        /// The offending input.
        input: String,
    },

    /// The scaled value did not fit the target integer type.
    #[error("fixed-point value out of range: {0:?}")]
    Overflow(String),

    /// A negative value was supplied where only non-negative values are valid
    /// (prices and resting quantities; signed deltas allow negatives).
    #[error("negative value not allowed here: {0:?}")]
    Negative(String),
}

/// Parse a fixed-point decimal string into its non-negative magnitude scaled by
/// `10^scale` plus a sign flag, returned as `(negative, magnitude_i128)`.
///
/// Returning the sign separately (rather than a single signed value) lets the
/// caller distinguish lexical negativity from numeric value: unsigned units
/// reject any `-`-prefixed input — including `"-0.00"`, which scales to 0 — while
/// signed deltas apply the sign. The `i128` magnitude is wide enough that the
/// caller does the final range check into `u32`/`u64`/`i64` (and `i64::MIN` is
/// reachable precisely because the magnitude stays positive until negation).
///
/// Pure integer string surgery — no `f64` ever touches the value. Accepts an
/// optional leading `-`, a required non-empty all-digit integer part, and an
/// optional fractional part of 1..=`scale` digits. A dangling `.` with no
/// fractional digits (`"1."`) is rejected as malformed — Kalshi always pads to
/// the unit scale — so anomalies surface as a raw fallback rather than being
/// silently normalized. The fractional part is right-padded with zeros to
/// exactly `scale` digits before parsing, so `"0.08"` and `"0.080000"` scale
/// identically.
fn parse_scaled(input: &str, scale: u8) -> Result<(bool, i128), FixedPointError> {
    if input.is_empty() {
        return Err(FixedPointError::Empty);
    }

    let (negative, body) = match input.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, input),
    };

    // A bare "-" (empty body) is not a number.
    if body.is_empty() {
        return Err(FixedPointError::NotNumeric(input.to_string()));
    }

    let (int_part, frac_part, had_point) = match body.split_once('.') {
        Some((i, f)) => (i, f, true),
        None => (body, "", false),
    };

    // Require a non-empty, all-digit integer part (rejects ".5", "1.2.3", "abc").
    if int_part.is_empty() || !int_part.bytes().all(|b| b.is_ascii_digit()) {
        return Err(FixedPointError::NotNumeric(input.to_string()));
    }
    // A decimal point with no fractional digits ("1.") is malformed.
    if had_point && frac_part.is_empty() {
        return Err(FixedPointError::NotNumeric(input.to_string()));
    }
    // Fractional part must be all digits.
    if !frac_part.bytes().all(|b| b.is_ascii_digit()) {
        return Err(FixedPointError::NotNumeric(input.to_string()));
    }
    if frac_part.len() > scale as usize {
        return Err(FixedPointError::TooManyDecimals {
            max: scale,
            input: input.to_string(),
        });
    }

    // Compute the scaled magnitude arithmetically — no heap allocation on this
    // per-field hot path (every price and size on every wire message):
    //   magnitude = int_part * 10^scale + frac_part * 10^(scale - frac_len).
    // Both parts are short, already-validated all-digit strings.
    let int_value: i128 = int_part
        .parse()
        .map_err(|_| FixedPointError::Overflow(input.to_string()))?;
    let frac_contribution: i128 = if frac_part.is_empty() {
        0
    } else {
        let frac_value: i128 = frac_part
            .parse()
            .map_err(|_| FixedPointError::Overflow(input.to_string()))?;
        let pad = scale as usize - frac_part.len();
        frac_value
            .checked_mul(10_i128.pow(pad as u32))
            .ok_or_else(|| FixedPointError::Overflow(input.to_string()))?
    };
    let magnitude = int_value
        .checked_mul(10_i128.pow(scale as u32))
        .and_then(|scaled| scaled.checked_add(frac_contribution))
        .ok_or_else(|| FixedPointError::Overflow(input.to_string()))?;

    Ok((negative, magnitude))
}

/// A price in micro-dollars: dollars x 1_000_000.
///
/// Captures the wire's up-to-6-decimal price precision exactly. `u32` (max
/// ~4.29e9) has ample headroom for binary-market prices (`$0..$1`).
/// `#[serde(transparent)]` keeps the stored JSONL value a bare integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MicroDollars(pub u32);

impl MicroDollars {
    /// Number of fractional decimal digits this unit represents (6 = micro).
    pub const SCALE: u8 = 6;

    /// Parse a decimal-dollar wire string (e.g. `"0.0800"`) into micro-dollars.
    ///
    /// Lexically negative values are rejected ([`FixedPointError::Negative`]),
    /// including `"-0.00"`; prices are always non-negative.
    pub fn parse(input: &str) -> Result<Self, FixedPointError> {
        let (negative, magnitude) = parse_scaled(input, Self::SCALE)?;
        if negative {
            return Err(FixedPointError::Negative(input.to_string()));
        }
        let value =
            u32::try_from(magnitude).map_err(|_| FixedPointError::Overflow(input.to_string()))?;
        Ok(MicroDollars(value))
    }
}

/// A resting (non-negative) quantity in centi-contracts: contracts x 100.
///
/// Captures the wire's 2-decimal `_fp` size precision exactly (Kalshi sizes can
/// be fractional). `#[serde(transparent)]` keeps the stored value a bare integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RestingQty(pub u64);

impl RestingQty {
    /// Number of fractional decimal digits this unit represents (2 = centi).
    pub const SCALE: u8 = 2;

    /// Parse a fixed-point size wire string (e.g. `"300.00"`) into
    /// centi-contracts. Lexically negative values are rejected (incl. `"-0.00"`).
    pub fn parse(input: &str) -> Result<Self, FixedPointError> {
        let (negative, magnitude) = parse_scaled(input, Self::SCALE)?;
        if negative {
            return Err(FixedPointError::Negative(input.to_string()));
        }
        let value =
            u64::try_from(magnitude).map_err(|_| FixedPointError::Overflow(input.to_string()))?;
        Ok(RestingQty(value))
    }
}

/// A signed change in quantity in centi-contracts: contracts x 100.
///
/// Used for order-book deltas, where liquidity can be added (positive) or
/// removed (negative). `#[serde(transparent)]` keeps the stored value a bare
/// signed integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct QtyDelta(pub i64);

impl QtyDelta {
    /// Number of fractional decimal digits this unit represents (2 = centi).
    pub const SCALE: u8 = 2;

    /// Parse a signed fixed-point delta wire string (e.g. `"-5.00"`) into
    /// centi-contracts. Negative values are valid here. `i64::MIN` is reachable
    /// because the magnitude is built positive then negated.
    pub fn parse(input: &str) -> Result<Self, FixedPointError> {
        let (negative, magnitude) = parse_scaled(input, Self::SCALE)?;
        let signed = if negative { -magnitude } else { magnitude };
        let value =
            i64::try_from(signed).map_err(|_| FixedPointError::Overflow(input.to_string()))?;
        Ok(QtyDelta(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- MicroDollars: price, 6 decimal places, unsigned ---------------------

    #[test]
    fn micro_dollars_parses_four_decimal_price() {
        assert_eq!(MicroDollars::parse("0.0800").unwrap(), MicroDollars(80_000));
    }

    #[test]
    fn micro_dollars_parses_full_six_decimal_precision() {
        assert_eq!(
            MicroDollars::parse("0.999999").unwrap(),
            MicroDollars(999_999)
        );
    }

    #[test]
    fn micro_dollars_trailing_zeros_are_equivalent() {
        assert_eq!(
            MicroDollars::parse("0.080000").unwrap(),
            MicroDollars::parse("0.0800").unwrap()
        );
    }

    #[test]
    fn micro_dollars_parses_whole_dollar_with_and_without_point() {
        assert_eq!(MicroDollars::parse("1").unwrap(), MicroDollars(1_000_000));
        assert_eq!(
            MicroDollars::parse("1.00").unwrap(),
            MicroDollars(1_000_000)
        );
    }

    #[test]
    fn micro_dollars_rejects_seven_decimals() {
        assert_eq!(
            MicroDollars::parse("0.1234567"),
            Err(FixedPointError::TooManyDecimals {
                max: 6,
                input: "0.1234567".to_string(),
            })
        );
    }

    #[test]
    fn micro_dollars_rejects_non_numeric() {
        assert_eq!(
            MicroDollars::parse("abc"),
            Err(FixedPointError::NotNumeric("abc".to_string()))
        );
    }

    #[test]
    fn micro_dollars_rejects_empty() {
        assert_eq!(MicroDollars::parse(""), Err(FixedPointError::Empty));
    }

    #[test]
    fn micro_dollars_rejects_negative() {
        assert_eq!(
            MicroDollars::parse("-0.5"),
            Err(FixedPointError::Negative("-0.5".to_string()))
        );
    }

    #[test]
    fn micro_dollars_overflow_is_an_error() {
        // 5000 dollars = 5e9 micro-dollars > u32::MAX (~4.29e9).
        assert!(matches!(
            MicroDollars::parse("5000"),
            Err(FixedPointError::Overflow(_))
        ));
    }

    // --- RestingQty: size, 2 decimal places, unsigned ------------------------

    #[test]
    fn resting_qty_parses_two_decimal_size() {
        assert_eq!(RestingQty::parse("300.00").unwrap(), RestingQty(30_000));
    }

    #[test]
    fn resting_qty_parses_fractional_contracts() {
        assert_eq!(RestingQty::parse("1.50").unwrap(), RestingQty(150));
    }

    #[test]
    fn resting_qty_rejects_three_decimals() {
        assert_eq!(
            RestingQty::parse("1.234"),
            Err(FixedPointError::TooManyDecimals {
                max: 2,
                input: "1.234".to_string(),
            })
        );
    }

    #[test]
    fn resting_qty_rejects_negative() {
        assert_eq!(
            RestingQty::parse("-5.00"),
            Err(FixedPointError::Negative("-5.00".to_string()))
        );
    }

    // --- QtyDelta: signed size, 2 decimal places -----------------------------

    #[test]
    fn qty_delta_parses_negative() {
        assert_eq!(QtyDelta::parse("-5.00").unwrap(), QtyDelta(-500));
    }

    #[test]
    fn qty_delta_parses_positive() {
        assert_eq!(QtyDelta::parse("5.00").unwrap(), QtyDelta(500));
    }

    #[test]
    fn qty_delta_parses_zero() {
        assert_eq!(QtyDelta::parse("0").unwrap(), QtyDelta(0));
    }

    #[test]
    fn qty_delta_parses_min_i64_boundary() {
        // Magnitude is built as a positive i128 then negated, so i64::MIN (whose
        // positive counterpart overflows i64) is representable. -92233720368547758.08
        // contracts x100 = -9223372036854775808 centi-contracts = i64::MIN.
        assert_eq!(
            QtyDelta::parse("-92233720368547758.08").unwrap(),
            QtyDelta(i64::MIN)
        );
    }

    #[test]
    fn qty_delta_positive_overflow_is_an_error() {
        // 92233720368547758.08 x100 = 9223372036854775808 = i64::MAX + 1. The
        // positive counterpart of i64::MIN is NOT representable, so the Overflow
        // path must fire rather than silently wrapping.
        assert!(matches!(
            QtyDelta::parse("92233720368547758.08"),
            Err(FixedPointError::Overflow(_))
        ));
    }

    // --- strictness: lexical negativity & malformed shapes (review hardening) -

    #[test]
    fn micro_dollars_rejects_negative_zero() {
        // A '-'-prefixed price is malformed even if it scales to 0; flag it
        // rather than silently normalize to a legitimate zero level.
        assert_eq!(
            MicroDollars::parse("-0.00"),
            Err(FixedPointError::Negative("-0.00".to_string()))
        );
    }

    #[test]
    fn resting_qty_rejects_negative_zero() {
        assert_eq!(
            RestingQty::parse("-0.00"),
            Err(FixedPointError::Negative("-0.00".to_string()))
        );
    }

    #[test]
    fn qty_delta_negative_zero_is_zero() {
        // Signed deltas legitimately allow a '-' sign; "-0.00" is just zero.
        assert_eq!(QtyDelta::parse("-0.00").unwrap(), QtyDelta(0));
    }

    #[test]
    fn rejects_trailing_decimal_point() {
        // A dangling '.' with no fractional digits is a shape Kalshi never sends;
        // reject it so it surfaces as a RawFallback rather than being normalized.
        assert_eq!(
            MicroDollars::parse("1."),
            Err(FixedPointError::NotNumeric("1.".to_string()))
        );
        assert_eq!(
            RestingQty::parse("300."),
            Err(FixedPointError::NotNumeric("300.".to_string()))
        );
    }

    #[test]
    fn accepts_leading_zeros() {
        // Leading zeros are harmless and kept (no panic, exact value).
        assert_eq!(
            MicroDollars::parse("007.5").unwrap(),
            MicroDollars(7_500_000)
        );
    }

    // --- serde: newtypes are transparent (bare integers on the wire) ---------

    #[test]
    fn micro_dollars_serializes_as_bare_integer() {
        assert_eq!(
            serde_json::to_string(&MicroDollars(80_000)).unwrap(),
            "80000"
        );
    }

    #[test]
    fn micro_dollars_round_trips_through_json() {
        let v = MicroDollars(80_000);
        let s = serde_json::to_string(&v).unwrap();
        let back: MicroDollars = serde_json::from_str(&s).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn qty_delta_serializes_as_bare_signed_integer() {
        assert_eq!(serde_json::to_string(&QtyDelta(-500)).unwrap(), "-500");
    }
}
