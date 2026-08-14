//! Inferred event start times from Kalshi event-ticker date-time segments.
//!
//! Match-style event tickers encode the scheduled start as `-YYMONDDHHMM`
//! (e.g. `KXWT20MATCH-26JUN121330SRIENG` = 2026-06-12 13:30 **US Eastern** —
//! verified against the real WT20 capture, which started 17:30Z). Hourly
//! tickers carry a 2-digit expiry hour instead (`KXBTCD-26JUL3117`) and
//! date-only tickers no time at all; both yield `None` — "no inferred start"
//! is a normal answer, never an error (the caller decides how loudly to
//! react). ET->UTC uses the US DST rule (second Sunday of March .. first
//! Sunday of November = UTC-4, else UTC-5) — deliberately no chrono-tz dep;
//! sub-hour DST-transition edge cases don't apply to market start times.

use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc, Weekday};

const MONTHS: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];

/// The inferred UTC start encoded in an event ticker, else `None`.
pub(crate) fn parse_event_start(event_ticker: &str) -> Option<DateTime<Utc>> {
    event_ticker.split('-').skip(1).find_map(parse_segment)
}

/// One `-`-separated segment: `YYMONDDHHMM<rest>` where `<rest>` must not
/// begin with another digit (a longer numeric tail is not the HHMM shape).
fn parse_segment(seg: &str) -> Option<DateTime<Utc>> {
    if seg.len() < 11 || !seg.is_char_boundary(11) {
        return None;
    }
    let yy: i32 = seg.get(0..2)?.parse().ok()?;
    let mon_str = seg.get(2..5)?;
    let mon = MONTHS.iter().position(|m| *m == mon_str)? as u32 + 1;
    let dd: u32 = seg.get(5..7)?.parse().ok()?;
    let hh: u32 = seg.get(7..9)?.parse().ok()?;
    let mm: u32 = seg.get(9..11)?.parse().ok()?;
    if seg.as_bytes().get(11).is_some_and(|c| c.is_ascii_digit()) {
        return None;
    }
    if hh > 23 || mm > 59 {
        return None;
    }
    let date = NaiveDate::from_ymd_opt(2000 + yy, mon, dd)?;
    let naive = date.and_hms_opt(hh, mm, 0)?;
    let utc = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
    Some(utc + Duration::hours(us_eastern_offset_hours(date)))
}

/// UTC offset (hours to ADD to an ET wall-clock time) for a given ET date.
fn us_eastern_offset_hours(d: NaiveDate) -> i64 {
    match (
        nth_weekday(d.year(), 3, Weekday::Sun, 2),
        nth_weekday(d.year(), 11, Weekday::Sun, 1),
    ) {
        (Some(dst_start), Some(dst_end)) if d >= dst_start && d < dst_end => 4,
        _ => 5,
    }
}

/// The n-th (1-based) given weekday of a month, e.g. second Sunday of March.
fn nth_weekday(year: i32, month: u32, wd: Weekday, n: u32) -> Option<NaiveDate> {
    let first = NaiveDate::from_ymd_opt(year, month, 1)?;
    let offset =
        (7 + wd.num_days_from_monday() as i64 - first.weekday().num_days_from_monday() as i64) % 7;
    NaiveDate::from_ymd_opt(year, month, 1 + offset as u32 + (n - 1) * 7)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn real_wt20_ticker_is_edt_minus_4() {
        // Verified against the real capture: 1330 ET on 2026-06-12 = 17:30Z.
        assert_eq!(
            parse_event_start("KXWT20MATCH-26JUN121330SRIENG"),
            Some(Utc.with_ymd_and_hms(2026, 6, 12, 17, 30, 0).unwrap())
        );
    }

    #[test]
    fn winter_ticker_is_est_minus_5_and_can_roll_the_date() {
        // 19:00 EST on Jan 15 = 00:00Z Jan 16.
        assert_eq!(
            parse_event_start("KXTEST-26JAN151900AAABBB"),
            Some(Utc.with_ymd_and_hms(2026, 1, 16, 0, 0, 0).unwrap())
        );
    }

    #[test]
    fn dst_boundary_2026_is_march_8_and_november_1() {
        // 2026: second Sunday of March = Mar 8; first Sunday of November = Nov 1.
        assert_eq!(
            parse_event_start("KXTEST-26MAR071200AAABBB"), // still EST (-5)
            Some(Utc.with_ymd_and_hms(2026, 3, 7, 17, 0, 0).unwrap())
        );
        assert_eq!(
            parse_event_start("KXTEST-26MAR081200AAABBB"), // EDT (-4)
            Some(Utc.with_ymd_and_hms(2026, 3, 8, 16, 0, 0).unwrap())
        );
        assert_eq!(
            parse_event_start("KXTEST-26NOV011200AAABBB"), // back to EST (-5)
            Some(Utc.with_ymd_and_hms(2026, 11, 1, 17, 0, 0).unwrap())
        );
    }

    #[test]
    fn hourly_two_digit_expiry_hour_is_not_a_start() {
        assert_eq!(parse_event_start("KXBTCD-26JUL3117"), None);
    }

    #[test]
    fn date_only_segment_is_not_a_start() {
        assert_eq!(parse_event_start("KXBTC-26DEC31"), None);
    }

    #[test]
    fn malformed_segments_are_none_never_a_panic() {
        assert_eq!(parse_event_start("KXTEST"), None); // no segment at all
        assert_eq!(parse_event_start("KXTEST-26XXX121330AAA"), None); // bad month
        assert_eq!(parse_event_start("KXTEST-26FEB301330AAA"), None); // Feb 30
        assert_eq!(parse_event_start("KXTEST-26JUN122530AAA"), None); // hh > 23
        assert_eq!(parse_event_start("KXTEST-26JUN121299AAA"), None); // mm > 59
        assert_eq!(parse_event_start("KXTEST-26JUN1213305AAA"), None); // 5-digit tail
        assert_eq!(parse_event_start(""), None);
    }
}
