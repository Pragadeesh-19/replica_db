// Implements Reservoir algorithm for constant memory statistical analysis of large datasets

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use rand::Rng;
use serde::{Deserialize, Serialize};

const MAX_UNIQUE_TRACKING: usize = 10_000;

pub const DEFAULT_RESERVOIR_CAPACITY: usize = 10_000;

pub const NUMERIC_HISTOGRAM_BINS: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Distribution {
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub null_count: u64,
    pub total_count: u64,
    pub unique_count: usize,
    pub histogram: Histogram,
}

impl Distribution {
    pub fn new(
        min: Option<f64>,
        max: Option<f64>,
        null_count: u64,
        total_count: u64,
        unique_count: usize,
        histogram: Histogram,
    ) -> Self {
        Self {
            min,
            max,
            null_count,
            total_count,
            unique_count,
            histogram,
        }
    }

    pub fn non_null_percentage(&self) -> f64 {
        if self.total_count == 0 {
            return 0.0;
        }
        ((self.total_count - self.null_count) as f64 / self.total_count as f64) * 100.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Histogram {
    Numeric {
        bins: Vec<f64>,
        frequencies: Vec<u64>,
    },
    Categorical {
        frequencies: HashMap<String, u64>,
        truncated: bool,
    }
}

pub struct Reservoir<T: Clone> {
    capacity: usize,
    items: Vec<T>,
    total_seen: Arc<AtomicU64>,
}

impl<T: Clone> Reservoir<T> {

    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            items: Vec::with_capacity(capacity),
            total_seen: Arc::new(AtomicU64::new(0)),
        }
    }

    /*
    Algorithm R:
        1. fill the reservoir until the capacity is reached
        2. For item i > capacity:
            Generate random j in [0, i)
            If j < capacity, replace items[j] with item i
     */
    pub fn add(&mut self, item: T) {
        let current_count = self.total_seen.fetch_add(1, Ordering::Relaxed);
        let index = current_count as usize;

        if index < self.capacity {
            self.items.push(item);
        } else {
            let mut rng = rand::thread_rng();
            let j = rng.gen_range(0..=index);

            if j < self.capacity {
                self.items[j] = item;
            }
        }
    }

    pub fn sample(&self) -> &[T] {
        &self.items
    }

    pub fn total_seen(&self) -> u64 {
        self.total_seen.load(Ordering::Relaxed)
    }

    pub fn sample_size(&self) -> usize {
        self.items.len()
    }

    pub fn into_sample(self) -> Vec<T> {
        self.items
    }
}

pub struct DistributionBuilder {
    min: Option<f64>,
    max: Option<f64>,
    null_count: u64,
    total_count: u64,
    unique_values: HashSet<String>,
    numeric_samples: Vec<f64>,
    categorical_samples: Vec<String>,
}

impl DistributionBuilder {
    pub fn new(total_count: u64, null_count: u64) -> Self {
        Self {
            min: None,
            max: None,
            null_count,
            total_count,
            unique_values: HashSet::new(),
            numeric_samples: Vec::new(),
            categorical_samples: Vec::new(),
        }
    }

    pub fn add_numeric(&mut self, value: f64) {
        self.numeric_samples.push(value);

        if self.unique_values.len() < MAX_UNIQUE_TRACKING {
            self.unique_values.insert(value.to_string());
        }

        self.min = Some(self.min.map_or(value, |m| m.min(value)));
        self.max = Some(self.max.map_or(value, |m| m.max(value)));
    }

    pub fn add_categorical(&mut self, value: String) {
        if self.unique_values.len() < MAX_UNIQUE_TRACKING {
            self.unique_values.insert(value.clone());
        }
        self.categorical_samples.push(value);
    }

    pub fn build(self) -> Distribution {
        let unique_count = self.unique_values.len();

        let histogram = if !self.numeric_samples.is_empty() {
            self.build_numeric_histogram()
        } else {
            self.build_categorical_histogram()
        };

        Distribution::new(
            self.min,
            self.max,
            self.null_count,
            self.total_count,
            unique_count,
            histogram,
        )
    }

    fn build_numeric_histogram(&self) -> Histogram {
        let (min, max) = match (self.min, self.max) {
            (Some(min), Some(max)) if min < max => (min, max),
            (Some(val), Some(_)) => (val, val + 1.0), // Handle constant values
            _ => return Histogram::Numeric {
                bins: vec![],
                frequencies: vec![],
            },
        };

        let bin_count = NUMERIC_HISTOGRAM_BINS;
        let bin_width = (max - min) / bin_count as f64;

        // Generate bin edges
        let mut bins = Vec::with_capacity(bin_count + 1);
        for i in 0..=bin_count {
            bins.push(min + (i as f64 * bin_width));
        }

        let mut frequencies = vec![0u64; bin_count];

        for &value in &self.numeric_samples {
            let bin_idx = if value >= max {
                bin_count - 1 // Edge case: assign max value to last bin
            } else {
                let idx = ((value - min) / bin_width) as usize;
                idx.min(bin_count - 1)
            };
            frequencies[bin_idx] += 1;
        }

        Histogram::Numeric { bins, frequencies }
    }

    fn build_categorical_histogram(&self) -> Histogram {
        let mut frequencies: HashMap<String, u64> = HashMap::new();

        for value in &self.categorical_samples {
            *frequencies.entry(value.clone()).or_insert(0) += 1;
        }

        let truncated = self.unique_values.len() >= MAX_UNIQUE_TRACKING;

        Histogram::Categorical {
            frequencies,
            truncated,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reservoir_fill_phase() {
        let mut reservoir = Reservoir::new(5);

        for i in 0..5 {
            reservoir.add(i);
        }

        assert_eq!(reservoir.sample_size(), 5);
        assert_eq!(reservoir.total_seen(), 5);
    }

    #[test]
    fn test_reservoir_replacement_phase() {
        let mut reservoir = Reservoir::new(5);

        for i in 0..100 {
            reservoir.add(i);
        }

        assert_eq!(reservoir.sample_size(), 5);
        assert_eq!(reservoir.total_seen(), 100);
    }

    #[test]
    fn test_distribution_builder_numeric() {
        let mut builder = DistributionBuilder::new(100, 5);

        for i in 0..10 {
            builder.add_numeric(i as f64);
        }

        let dist = builder.build();

        assert_eq!(dist.min, Some(0.0));
        assert_eq!(dist.max, Some(9.0));
        assert_eq!(dist.null_count, 5);
        assert_eq!(dist.total_count, 100);

        match dist.histogram {
            Histogram::Numeric { bins, frequencies } => {
                assert_eq!(bins.len(), NUMERIC_HISTOGRAM_BINS + 1);
                assert_eq!(frequencies.len(), NUMERIC_HISTOGRAM_BINS);
            }
            _ => panic!("Expected numeric histogram"),
        }
    }

    #[test]
    fn test_distribution_builder_categorical() {
        let mut builder = DistributionBuilder::new(50, 2);

        builder.add_categorical("apple".to_string());
        builder.add_categorical("banana".to_string());
        builder.add_categorical("apple".to_string());

        let dist = builder.build();

        assert_eq!(dist.unique_count, 2);

        match dist.histogram {
            Histogram::Categorical { frequencies, truncated } => {
                assert_eq!(frequencies.get("apple"), Some(&2));
                assert_eq!(frequencies.get("banana"), Some(&1));
                assert!(!truncated);
            }
            _ => panic!("Expected categorical histogram"),
        }
    }
}