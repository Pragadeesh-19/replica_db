use std::collections::HashMap;
use crate::math::{Distribution, Histogram};
use rand::prelude::*;
use rand::Rng;
use anyhow::{Context, Result};

pub fn synthesize_value(dist: &Distribution, rng: &mut ThreadRng, quantile: Option<f64>) -> Result<String> {

    if should_generate_null(dist, rng) {
        return Ok("\\N".to_string()); 
    }

    //Generate non-null value based on histogram type
    match &dist.histogram {
        Histogram::Categorical { frequencies, .. } => {
            synthesize_categorical(frequencies, rng)
        }
        Histogram::Numeric { bins, frequencies } => {
            synthesize_numeric(bins, frequencies, rng, quantile)
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
    quantile: Option<f64>,
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

    let value = if let Some(q) = quantile {
        inverse_transform_sample(bins, frequencies, q, total_weight)?
    } else {
        weighted_random_sample(bins, frequencies, rng, total_weight)
    };

    Ok(format_numeric(value))
}

fn inverse_transform_sample(bins: &[f64], frequencies: &[u64], quantile: f64, total_weight: u64) -> Result<f64> {
    let target_cumulative = quantile * total_weight as f64;
    let mut cumulative = 0.0;

    for (bin_idx, &frequency) in frequencies.iter().enumerate() {
        let prev_cumulative = cumulative;
        cumulative += frequency as f64;

        if cumulative >= target_cumulative {
            let bin_min = bins[bin_idx];
            let bin_max = bins[bin_idx + 1];

            if frequency == 0 {
                return Ok((bin_min + bin_max) / 2.0);
            }

            let position_in_bin = (target_cumulative - prev_cumulative) / frequency as f64;
            let value = bin_min + position_in_bin * (bin_max - bin_min);

            return Ok(value.clamp(bin_min, bin_max));
        }
    }

    Ok(bins[bins.len() - 1])
}

fn weighted_random_sample(
    bins: &[f64],
    frequencies: &[u64],
    rng: &mut ThreadRng,
    total_weight: u64,
) -> f64 {
    let mut random_weight = rng.gen_range(0..total_weight);
    let mut selected_bin_idx = 0;

    for (idx, &weight) in frequencies.iter().enumerate() {
        if random_weight < weight {
            selected_bin_idx = idx;
            break;
        }
        random_weight -= weight;
    }

    let bin_min = bins[selected_bin_idx];
    let bin_max = bins[selected_bin_idx + 1];

    rng.gen_range(bin_min..bin_max)
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
    fn test_inverse_transform_sampling() {
        // Histogram: [0-50): 25, [50-100): 75
        let bins = vec![0.0, 50.0, 100.0];
        let frequencies = vec![25, 75];
        let total = 100;

        // q=0.0 → should be near 0
        let v1 = inverse_transform_sample(&bins, &frequencies, 0.0, total).unwrap();
        assert!(v1 >= 0.0 && v1 < 50.0);

        // q=0.25 → exactly at boundary (25/100)
        let v2 = inverse_transform_sample(&bins, &frequencies, 0.25, total).unwrap();
        assert!((v2 - 50.0).abs() < 0.1);

        // q=0.5 → middle of second bin
        let v3 = inverse_transform_sample(&bins, &frequencies, 0.5, total).unwrap();
        assert!(v3 >= 50.0 && v3 < 100.0);

        // q=1.0 → near max
        let v4 = inverse_transform_sample(&bins, &frequencies, 1.0, total).unwrap();
        assert!(v4 >= 50.0 && v4 <= 100.0);
    }

    #[test]
    fn test_synthesize_with_quantile() {
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

        // With quantile=0.5, should be in upper half
        let value = synthesize_value(&dist, &mut rng, Some(0.5)).unwrap();
        let parsed: f64 = value.parse().unwrap();
        assert!(parsed >= 25.0); // Should be around midpoint

        // With quantile=1.0, should be near max
        let value = synthesize_value(&dist, &mut rng, Some(1.0)).unwrap();
        let parsed: f64 = value.parse().unwrap();
        assert!(parsed >= 75.0);
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