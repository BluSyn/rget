//! URL range expansion logic (e.g. `model-{001..040}-of-00040.safetensors`).

use regex::Regex;

/// Expands range patterns in a URL, e.g.:
/// `model-{001..040}-of-00040.safetensors` → 40 URLs with zero-padded numbers.
/// `model-{1..5}-part-{01..03}.bin` → 5 × 3 = 15 combinations.
///
/// Supports multiple independent ranges (cartesian product).
/// Zero-padding is determined by the number of digits in the **start** of each range.
pub fn expand_ranges(raw: &str) -> Vec<String> {
    // Match {digits..digits}, e.g. {001..040} or {1..100}
    let re = Regex::new(r"\{(\d+)\.\.(\d+)\}").unwrap();

    // Find all matches with their positions
    let matches: Vec<_> = re.find_iter(raw).collect();

    if matches.is_empty() {
        return vec![raw.to_string()];
    }

    // For each match, compute the list of string replacements (with proper padding)
    let mut replacements: Vec<Vec<String>> = Vec::new();

    for m in &matches {
        let caps = re.captures(m.as_str()).unwrap();
        let start_str = caps.get(1).unwrap().as_str();
        let end_str = caps.get(2).unwrap().as_str();

        let start: u64 = start_str.parse().unwrap_or(0);
        let end: u64 = end_str.parse().unwrap_or(0);

        if start > end {
            // Invalid range, just keep original
            replacements.push(vec![m.as_str().to_string()]);
            continue;
        }

        let width = start_str.len(); // zero-padding width from left side
        let mut variants = Vec::new();

        for n in start..=end {
            let s = format!("{:0width$}", n, width = width);
            variants.push(s);
        }

        replacements.push(variants);
    }

    // Generate all combinations using cartesian product
    let mut results = vec![raw.to_string()];

    for (i, repls) in replacements.iter().enumerate() {
        let mut new_results = Vec::new();
        let mat = &matches[i];

        for base in &results {
            for repl in repls {
                let mut new_url = base.clone();
                if let Some(pos) = new_url.find(mat.as_str()) {
                    new_url.replace_range(pos..pos + mat.as_str().len(), repl);
                }
                new_results.push(new_url);
            }
        }
        results = new_results;
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_range() {
        let urls = expand_ranges("model-{1..3}.bin");
        assert_eq!(urls, vec![
            "model-1.bin",
            "model-2.bin",
            "model-3.bin",
        ]);
    }

    #[test]
    fn test_zero_padded_range() {
        let urls = expand_ranges("model-{001..005}.bin");
        assert_eq!(urls, vec![
            "model-001.bin",
            "model-002.bin",
            "model-003.bin",
            "model-004.bin",
            "model-005.bin",
        ]);
    }

    #[test]
    fn test_different_padding() {
        let urls = expand_ranges("model-{1..5}.bin");
        assert_eq!(urls, vec![
            "model-1.bin",
            "model-2.bin",
            "model-3.bin",
            "model-4.bin",
            "model-5.bin",
        ]);
    }

    #[test]
    fn test_multiple_ranges() {
        let urls = expand_ranges("part-{1..2}-chunk-{01..02}.bin");
        assert_eq!(urls.len(), 4);
        assert!(urls.contains(&"part-1-chunk-01.bin".to_string()));
        assert!(urls.contains(&"part-1-chunk-02.bin".to_string()));
        assert!(urls.contains(&"part-2-chunk-01.bin".to_string()));
        assert!(urls.contains(&"part-2-chunk-02.bin".to_string()));
    }

    #[test]
    fn test_no_range() {
        let urls = expand_ranges("https://example.com/file.bin");
        assert_eq!(urls, vec!["https://example.com/file.bin"]);
    }

    #[test]
    fn test_invalid_range_start_greater_than_end() {
        let urls = expand_ranges("model-{10..5}.bin");
        // Should return original string
        assert_eq!(urls, vec!["model-{10..5}.bin"]);
    }
}
