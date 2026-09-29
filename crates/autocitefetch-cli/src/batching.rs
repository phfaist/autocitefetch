//! `--refresh-batching PREFIX:SETTING[,SETTING…]`: per-source overrides of the
//! core's [`RefreshBatching`].
//!
//! Settings are applied in order on top of the source's built-in default, so
//! `arxiv:min=50` changes only the minimum batch, and `doi:eager,min=3` starts
//! from scratch:
//!
//! | setting | effect |
//! |---|---|
//! | `eager` | reset to [`RefreshBatching::EAGER`] (fetch what is due, when due) |
//! | `min=N` | minimum batch worth a request when nothing is needed |
//! | `defer=DUR` | how long past hard expiry an entry may wait (`0`, `90s`, `12h`, `2d`, …) |
//! | `topup=P%` / `topup=off` | pull entries ≥ P% through their lifetime forward / never |
//! | `fill=chunk` / `fill=N` | top up to the chunk boundary / add up to N entries |

use std::time::Duration;

use autocitefetch::{Fill, RefreshBatching, TopUp};
use clap::ValueEnum;

use crate::cli::SourceKind;

/// One `--refresh-batching` argument, syntax-checked by clap.
#[derive(Clone, Debug)]
pub struct BatchingSpec {
    pub source: SourceKind,
    settings: Vec<Setting>,
}

#[derive(Clone, Copy, Debug)]
enum Setting {
    Eager,
    Min(usize),
    Defer(Duration),
    TopUpOff,
    TopUp(u32),
    Fill(Fill),
}

/// clap `value_parser` for [`BatchingSpec`].
pub fn parse_spec(arg: &str) -> Result<BatchingSpec, String> {
    let (prefix, rest) = arg
        .split_once(':')
        .ok_or("expected PREFIX:SETTING[,SETTING...], e.g. `arxiv:min=50`")?;
    let source = SourceKind::from_str(prefix.trim(), true)
        .map_err(|_| format!("unknown source `{prefix}`"))?;
    let settings = rest
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(parse_setting)
        .collect::<Result<Vec<_>, _>>()?;
    if settings.is_empty() {
        return Err(format!("no settings given for `{prefix}`"));
    }
    Ok(BatchingSpec { source, settings })
}

fn parse_setting(s: &str) -> Result<Setting, String> {
    if s == "eager" {
        return Ok(Setting::Eager);
    }
    let (name, value) = s
        .split_once('=')
        .ok_or_else(|| format!("expected NAME=VALUE or `eager`, got `{s}`"))?;
    let value = value.trim();
    match name.trim() {
        "min" => value
            .parse()
            .map(Setting::Min)
            .map_err(|_| format!("`min` takes a count, got `{value}`")),
        "defer" => parse_duration(value).map(Setting::Defer),
        "topup" if value == "off" => Ok(Setting::TopUpOff),
        "topup" => {
            let p: u32 = value
                .strip_suffix('%')
                .unwrap_or(value)
                .parse()
                .map_err(|_| format!("`topup` takes a percentage or `off`, got `{value}`"))?;
            if p > 100 {
                return Err(format!("`topup` is a percentage (0-100), got `{value}`"));
            }
            Ok(Setting::TopUp(p))
        }
        "fill" if value == "chunk" => Ok(Setting::Fill(Fill::ChunkBoundary)),
        "fill" => value
            .parse()
            .map(|n| Setting::Fill(Fill::Extra(n)))
            .map_err(|_| format!("`fill` takes `chunk` or a count, got `{value}`")),
        other => Err(format!(
            "unknown setting `{other}` (expected eager, min, defer, topup or fill)"
        )),
    }
}

/// `0`, or a whole number followed by `ms`, `s`, `m`, `h` or `d`.
fn parse_duration(s: &str) -> Result<Duration, String> {
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let bad = || format!("expected a duration like `90s`, `12h` or `2d`, got `{s}`");
    let n: u64 = num.parse().map_err(|_| bad())?;
    let secs = match unit {
        "ms" => return Ok(Duration::from_millis(n)),
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        _ => return Err(bad()),
    };
    Ok(Duration::from_secs(n.saturating_mul(secs)))
}

impl BatchingSpec {
    /// This spec's settings applied, in order, on top of `base`.
    pub fn apply(&self, mut base: RefreshBatching) -> Result<RefreshBatching, String> {
        for setting in &self.settings {
            match *setting {
                Setting::Eager => base = RefreshBatching::EAGER,
                Setting::Min(n) => base.min_batch = n,
                Setting::Defer(d) => base.max_defer = d,
                Setting::TopUpOff => base.top_up = None,
                Setting::TopUp(p) => {
                    let fill = base.top_up.map_or(Fill::ChunkBoundary, |t| t.fill);
                    base.top_up = Some(TopUp {
                        min_age_percent: p,
                        fill,
                    });
                }
                Setting::Fill(fill) => match &mut base.top_up {
                    Some(t) => t.fill = fill,
                    None => {
                        return Err(format!(
                            "`fill` for `{}` needs top-up enabled (add `topup=P%` before it)",
                            self.source.prefix()
                        ));
                    }
                },
            }
        }
        Ok(base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply(arg: &str, base: RefreshBatching) -> Result<RefreshBatching, String> {
        parse_spec(arg)?.apply(base)
    }

    #[test]
    fn settings_apply_in_order_over_the_default() {
        let got = apply("arxiv:eager,min=3,defer=2d,topup=50%,fill=4", RefreshBatching::EAGER)
            .unwrap();
        assert_eq!(
            got,
            RefreshBatching {
                min_batch: 3,
                max_defer: Duration::from_secs(2 * 24 * 3600),
                top_up: Some(TopUp {
                    min_age_percent: 50,
                    fill: Fill::Extra(4),
                }),
            }
        );
        // Only `min` changes; the rest is kept.
        let changed = apply("arxiv:min=50", got).unwrap();
        assert_eq!(changed, RefreshBatching { min_batch: 50, ..got });
        assert_eq!(apply("doi:topup=off", got).unwrap().top_up, None);
    }

    #[test]
    fn bad_specs_are_rejected() {
        for bad in [
            "arxiv",
            "arxiv:",
            "nope:min=1",
            "arxiv:min=x",
            "arxiv:defer=3w",
            "arxiv:topup=150%",
            "arxiv:size=3",
        ] {
            assert!(parse_spec(bad).is_err(), "{bad} should not parse");
        }
        assert!(apply("doi:eager,fill=chunk", RefreshBatching::EAGER).is_err());
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("0"), Ok(Duration::ZERO));
        assert_eq!(parse_duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_duration("90s"), Ok(Duration::from_secs(90)));
        assert_eq!(parse_duration("12h"), Ok(Duration::from_secs(12 * 3600)));
        assert!(parse_duration("h").is_err());
        assert!(parse_duration("5").is_err());
    }
}
