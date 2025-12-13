use std::collections::HashMap;
use crate::math::{Distribution, Histogram};
use rand::prelude::*;
use rand::Rng;
use anyhow::{Context, Result};

pub fn synthesize_value(dist: &Distribution, rng: &mut ThreadRng) -> Result<String> {

    if should_generate_null(dist, rng) {
        return Ok("\\N".to_string()); 
    }

    //Generate non-null value based on histogram type
    match &dist.histogram {
        Histogram::Categorical { frequencies, .. } => {
            synthesize_categorical(frequencies, rng)
        }
        Histogram::Numeric { bins, frequencies } => {
            synthesize_numeric(bins, frequencies, rng)
        }
    }
}

fn should_generate_null(dist: &Distribution, rng: &mut ThreadRng) -> bool {
    if dist.total_count == 0 {
        return false;
    }

    let null_probability = dist.null_count as f64 / dist.total_count as f64;

    // Roll the dice
    rng.gen_bool(null_probability)
}

fn synthesize_categorical(
    frequencies: &HashMap<String, u64>,
    rng: &mut ThreadRng,
) -> Result<String> {
    if frequencies.is_empty() {
        return Ok("unknown".to_string());
    }

    // Calculate total weight
    let total_weight: u64 = frequencies.values().sum();

    if total_weight == 0 {
        // Fallback: uniform selection if all frequencies are 0
        let keys: Vec<_> = frequencies.keys().collect();
        return Ok(keys.choose(rng)
            .map(|s| (*s).clone())
            .unwrap_or_else(|| "unknown".to_string()));
    }

    // Weighted random selection
    let mut random_weight = rng.gen_range(0..total_weight);

    for (value, &weight) in frequencies {
        if random_weight < weight {
            return Ok(value.clone());
        }
        random_weight -= weight;
    }

    // Fallback (shouldn't reach here due to mathematics, but handle gracefully)
    Ok(frequencies.keys().next()
        .map(|s| s.clone())
        .unwrap_or_else(|| "unknown".to_string()))
}

fn synthesize_numeric(
    bins: &[f64],
    frequencies: &[u64],
    rng: &mut ThreadRng,
) -> Result<String> {
    if bins.len() < 2 || frequencies.is_empty() {
        return Ok("0".to_string());
    }

    // Step 1: Select bin via weighted sampling
    let total_weight: u64 = frequencies.iter().sum();

    if total_weight == 0 {
        // No samples - return midpoint of first bin
        if bins.len() >= 2 {
            let midpoint = (bins[0] + bins[1]) / 2.0;
            return Ok(format_numeric(midpoint));
        }
        return Ok("0".to_string());
    }

    let mut random_weight = rng.gen_range(0..total_weight);
    let mut selected_bin_idx = 0;

    for (idx, &weight) in frequencies.iter().enumerate() {
        if random_weight < weight {
            selected_bin_idx = idx;
            break;
        }
        random_weight -= weight;
    }

    // Step 2: Interpolate within selected bin
    let bin_min = bins[selected_bin_idx];
    let bin_max = bins[selected_bin_idx + 1];

    // Generate uniform random value in [bin_min, bin_max)
    let value = rng.gen_range(bin_min..bin_max);

    Ok(format_numeric(value))
}

fn format_numeric(value: f64) -> String {
    // Check if value is effectively an integer
    if value.fract().abs() < 1e-9 && value.abs() < i64::MAX as f64 {
        format!("{}", value as i64)
    } else {
        // Format with reasonable precision
        format!("{:.6}", value).trim_end_matches('0').trim_end_matches('.').to_string()
    }
}

pub fn synthesize_primary_key(
    data_type: &crate::schema::DataType,
    counter: &mut i64,
) -> String {
    use crate::schema::DataType;

    match data_type {
        DataType::Integer => {
            *counter += 1;
            counter.to_string()
        }
        DataType::Uuid => {
            uuid::Uuid::new_v4().to_string()
        }
        _ => {
            // Fallback: treat as integer
            *counter += 1;
            counter.to_string()
        }
    }
}

pub fn synthesize_foreign_key(
    parent_keys: &[String],
    rng: &mut ThreadRng,
) -> Result<String> {
    parent_keys.choose(rng)
        .map(|s| s.clone())
        .context("Parent key list is empty (should have been validated earlier)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::math::{Distribution, Histogram};

    #[test]
    fn test_null_generation() {
        let mut rng = rand::thread_rng();

        // 100% null rate
        let dist = Distribution::new(
            None,
            None,
            100,
            100,
            0,
            Histogram::Categorical {
                frequencies: HashMap::new(),
                truncated: false,
            },
        );

        // Should always generate NULL
        for _ in 0..10 {
            let value = synthesize_value(&dist, &mut rng).unwrap();
            assert_eq!(value, "\\N");
        }
    }

    #[test]
    fn test_categorical_generation() {
        let mut rng = rand::thread_rng();
        let mut frequencies = HashMap::new();
        frequencies.insert("apple".to_string(), 70);
        frequencies.insert("banana".to_string(), 30);

        let dist = Distribution::new(
            None,
            None,
            0,
            100,
            2,
            Histogram::Categorical {
                frequencies,
                truncated: false,
            },
        );

        // Generate multiple values
        let mut results = HashMap::new();
        for _ in 0..100 {
            let value = synthesize_value(&dist, &mut rng).unwrap();
            *results.entry(value).or_insert(0) += 1;
        }

        // Should have generated both values
        assert!(results.contains_key("apple"));
        assert!(results.contains_key("banana"));
        // Apple should appear more frequently (roughly 70/30 ratio)
        // Due to randomness, we just check both exist
    }

    #[test]
    fn test_numeric_generation() {
        let mut rng = rand::thread_rng();

        let dist = Distribution::new(
            Some(0.0),
            Some(100.0),
            0,
            100,
            100,
            Histogram::Numeric {
                bins: vec![0.0, 50.0, 100.0],
                frequencies: vec![50, 50],
            },
        );

        // Generate values and verify they're in range
        for _ in 0..20 {
            let value = synthesize_value(&dist, &mut rng).unwrap();
            let parsed: f64 = value.parse().unwrap();
            assert!(parsed >= 0.0 && parsed < 100.0);
        }
    }

    #[test]
    fn test_format_numeric_integer() {
        assert_eq!(format_numeric(42.0), "42");
        assert_eq!(format_numeric(100.0), "100");
    }

    #[test]
    fn test_format_numeric_float() {
        let result = format_numeric(std::f64::consts::PI);
        assert!(result.contains("3.14"));
    }

    #[test]
    fn test_synthesize_primary_key_integer() {
        use crate::schema::DataType;
        let mut counter = 0;

        let pk1 = synthesize_primary_key(&DataType::Integer, &mut counter);
        let pk2 = synthesize_primary_key(&DataType::Integer, &mut counter);

        assert_eq!(pk1, "1");
        assert_eq!(pk2, "2");
    }

    #[test]
    fn test_synthesize_primary_key_uuid() {
        use crate::schema::DataType;
        let mut counter = 0;

        let pk = synthesize_primary_key(&DataType::Uuid, &mut counter);

        // Verify it's a valid UUID format
        assert!(uuid::Uuid::parse_str(&pk).is_ok());
    }

    #[test]
    fn test_synthesize_foreign_key() {
        let mut rng = rand::thread_rng();
        let parent_keys = vec!["1".to_string(), "2".to_string(), "3".to_string()];

        let fk = synthesize_foreign_key(&parent_keys, &mut rng).unwrap();

        assert!(parent_keys.contains(&fk));
    }
}