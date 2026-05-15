//! Human-readable size and speed parsing.

use anyhow::{bail, Context, Result};

/// Parse a human-readable byte size (e.g. "500M", "2T", "100G", "1.5T").
/// Returns the size in bytes.
pub fn parse_human_size(input: &str) -> Result<u64> {
    let s = input.trim().to_ascii_lowercase();
    // Remove optional /s suffix if someone passes "50M/s" to --max-size by mistake
    let s = s.strip_suffix("/s").unwrap_or(&s).to_string();

    let (num_part, unit) = if let Some(pos) = s.find(|c: char| !c.is_ascii_digit() && c != '.') {
        (&s[..pos], &s[pos..])
    } else {
        (s.as_str(), "")
    };

    let value: f64 = num_part.parse().context("Invalid number in size")?;

    let multiplier: u64 = match unit.trim_start_matches([' ', 'b']) {
        "" | "b"     => 1,
        "k" | "kb"   => 1024,
        "m" | "mb"   => 1024 * 1024,
        "g" | "gb"   => 1024 * 1024 * 1024,
        "t" | "tb"   => 1024 * 1024 * 1024 * 1024,
        other => bail!(
            "Unknown unit '{}' in size. Supported units: K, M, G, T (case insensitive)",
            other
        ),
    };

    let bytes = (value * multiplier as f64) as u64;

    if bytes == 0 {
        bail!("Size cannot be zero");
    }

    Ok(bytes)
}

/// Parse a human-readable speed string (e.g. "50M", "2G", "500K", "1.5M/s")
/// into bytes per second.
pub fn parse_speed(input: &str) -> Result<u64> {
    let bytes = parse_human_size(input)?;

    if bytes == 0 {
        bail!("--limit-rate cannot be zero");
    }

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_units() {
        assert_eq!(parse_speed("100").unwrap(), 100);
        assert_eq!(parse_speed("50K").unwrap(), 50 * 1024);
        assert_eq!(parse_speed("2M").unwrap(), 2 * 1024 * 1024);
        assert_eq!(parse_speed("1G").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_with_slash_s() {
        assert_eq!(parse_speed("100M/s").unwrap(), 100 * 1024 * 1024);
        assert_eq!(parse_speed("500K/s").unwrap(), 500 * 1024);
    }

    #[test]
    fn test_case_insensitive() {
        assert_eq!(parse_speed("50m").unwrap(), 50 * 1024 * 1024);
        assert_eq!(parse_speed("2g").unwrap(), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_zero() {
        assert!(parse_speed("0").is_err());
        assert!(parse_speed("0M").is_err());
    }

    #[test]
    fn test_invalid_unit() {
        assert!(parse_speed("50X").is_err());
    }

    #[test]
    fn test_decimal() {
        let result = parse_speed("1.5M").unwrap();
        assert_eq!(result, (1.5 * 1024.0 * 1024.0) as u64);
    }

    #[test]
    fn test_terabytes() {
        assert_eq!(parse_speed("2T").unwrap(), 2 * 1024 * 1024 * 1024 * 1024);
        assert_eq!(parse_speed("1.5TB").unwrap(), (1.5 * 1024.0 * 1024.0 * 1024.0 * 1024.0) as u64);
    }
}
