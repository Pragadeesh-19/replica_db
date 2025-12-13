//! Gaussian Copula implementation for preserving cross column correlation.
//! This file makes sure that correlated variables maintain their relationship.

//! # Mathematical Foundation
//!
//! **Gaussian Copula:** A multivariate distribution function that captures the
//! dependency structure between random variables while allowing each variable
//! to have its own marginal distribution.
//!
//! **Cholesky Decomposition:** Given correlation matrix R, finds lower triangular
//! matrix L such that R = L * L^T. This enables efficient generation of correlated
//! samples by transforming independent random variables.
//!
//! # Algorithm
//!
//! 1. **Training (Scan Phase):**
//!    - Compute Pearson correlation matrix from numeric column samples
//!    - R[i,j] = Cov(X_i, X_j) / (σ_i * σ_j)
//!
//! 2. **Generation (Synthesis Phase):**
//!    - Generate independent standard normals: Z ~ N(0, 1)
//!    - Transform: Y = L * Z (where L is Cholesky decomposition of R)
//!    - Convert to uniform [0,1]: U = Φ(Y) (standard normal CDF)
//!    - Use U to sample from marginal histograms
//!
//! # Performance
//!
//! - Correlation matrix computation: O(n²m) where n=columns, m=samples
//! - Cholesky decomposition: O(n³) - done once during initialization
//! - Sample generation: O(n²) per row - matrix multiplication
//!
//! For typical schemas (n < 100 columns), this adds ~10ms to scan,
//! negligible overhead to generation (~0.1ms per row).

use serde::{Deserialize, Serialize};
use anyhow::{Context, Result};
use nalgebra::{DMatrix, DVector};
use rand::prelude::*;
use statrs::distribution::{ContinuousCDF, Normal};
use tracing::debug;
use rand::Rng;
use rand::rngs::ThreadRng;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CovarianceMatrix {
    pub columns: Vec<String>,

    #[serde(rename = "correlation_matrix")]
    pub matrix_data: Vec<f64>,
    pub dimension: usize,
}

impl CovarianceMatrix {

    pub fn compute(column_names: Vec<String>, samples: &[Vec<f64>]) -> Result<Self> {

        let n_samples = samples.len();
        let n_cols = column_names.len();

        if n_samples == 0 {
            anyhow::bail!("Cannot compute covariance from zero samples");
        }
        if n_cols == 0 {
            anyhow::bail!("Cannot compute covariance from zero columns");
        }

        for (i, sample) in samples.iter().enumerate() {
            if sample.len() != n_cols {
                anyhow::bail!(
                    "Sample {} has {} values, expected {}",
                    i,
                    sample.len(),
                    n_cols
                );
            }
        }

        let mut data = Vec::with_capacity(n_samples * n_cols);
        for sample in samples {
            data.extend_from_slice(sample);
        }

        let sample_matrix = DMatrix::from_row_slice(n_samples, n_cols, &data);
        let means: DVector<f64> = sample_matrix.column_mean();

        // computing standard deviation
        let mut std_devs = DVector::zeros(n_cols);
        for col_idx in 0..n_cols {
            let col = sample_matrix.column(col_idx);
            let mean = means[col_idx];
            let variance: f64 = col.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / (n_samples - 1) as f64;
            std_devs[col_idx] = variance.sqrt();
        }

        // standardize matrix (Z-Score)
        let mut standardized = DMatrix::zeros(n_samples, n_cols);
        for col_idx in 0..n_cols {
            let mean = means[col_idx];
            let std = std_devs[col_idx];

            if std < 1e-10 {
                // Constant column - correlation undefined, treat as uncorrelated
                for row_idx in 0..n_samples {
                    standardized[(row_idx, col_idx)] = 0.0;
                }
            } else {
                for row_idx in 0..n_samples {
                    let value = sample_matrix[(row_idx, col_idx)];
                    standardized[(row_idx, col_idx)] = (value - mean) / std;
                }
            }
        }

        // Computing correlational matrix: R = (Z^T * Z) / (n-1)
        let correlational_matrix = (standardized.transpose() * standardized) / (n_samples - 1) as f64;

        let matrix_data: Vec<f64> = correlational_matrix.iter().copied().collect();

        debug!(
            columns = n_cols,
            samples = n_samples,
            "Computed correlational matrix"
        );

        Ok(Self {
            columns: column_names,
            matrix_data,
            dimension: n_cols,
        })
    }

    pub fn to_matrix(&self) -> DMatrix<f64> {
        DMatrix::from_row_slice(self.dimension, self.dimension, &self.matrix_data)
    }
}

#[derive(Debug, Clone)]
pub struct GaussianCopula {
    // Lower triangular Cholesky decomposition: L where R = L * L^T
    cholesky_lower: DMatrix<f64>,

    columns: Vec<String>,
    standard_normal: Normal
}

impl GaussianCopula {

    pub fn new(covariance: &CovarianceMatrix) -> Result<Self> {
        let correlation_matrix = covariance.to_matrix();

        // Perform Cholesky decomposition
        let cholesky = correlation_matrix
            .clone()
            .cholesky()
            .context("Failed to compute Cholesky decomposition - correlation matrix not positive definite")?;

        let standard_normal = Normal::new(0.0, 1.0)
            .context("Failed to create standard normal distribution")?;

        debug!(
            dimension = covariance.dimension,
            "Initialized Gaussian copula"
        );

        Ok(Self {
            cholesky_lower: cholesky.l(),
            columns: covariance.columns.clone(),
            standard_normal,
        })
    }

    /// Generates a vector of correlated uniform [0,1] samples.
    ///
    /// # Algorithm
    /// 1. Generate Z ~ N(0, 1)^n (independent standard normals)
    /// 2. Transform: Y = L * Z (correlated normals)
    /// 3. Convert to uniforms: U_i = Φ(Y_i) where Φ is standard normal CDF
    ///
    /// # Arguments
    /// * `rng` - Random number generator
    ///
    /// # Returns
    /// Vector of n uniform [0,1] values with correlation structure
    pub fn generate_correlated_uniforms(&self, rng: &mut ThreadRng) -> Vec<f64> {
        let dimension = self.cholesky_lower.nrows();

        // Step 1: Generate independent standard normals
        let mut independent_normals = DVector::zeros(dimension);
        for i in 0..dimension {
            // Box-Muller transform for standard normal
            let u1: f64 = rng.r#gen();
            let u2: f64 = rng.r#gen();
            independent_normals[i] = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
        }

        // Step 2: Transform to correlated normals via Cholesky matrix
        let correlated_normals = &self.cholesky_lower * independent_normals;

        // Step 3: Convert to uniform [0,1] via standard normal CDF
        let mut uniforms = Vec::with_capacity(dimension);
        for i in 0..dimension {
            let normal_value = correlated_normals[i];
            let uniform = self.standard_normal.cdf(normal_value);
            // Clamp to [0,1] for numerical stability
            uniforms.push(uniform.clamp(0.0, 1.0));
        }

        uniforms
    }

    pub fn dimension(&self) -> usize {
        self.cholesky_lower.nrows()
    }

    /// Returns the column names.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

pub struct CovarianceBuilder {
    columns: Vec<String>,
    samples: Vec<Vec<f64>>,
}

impl CovarianceBuilder {
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
            samples: Vec::new(),
        }
    }

    pub fn add_sample(&mut self, values: Vec<f64>) -> Result<()> {
        if values.len() != self.columns.len() {
            anyhow::bail!(
                "Sample has {} values, expected {} columns",
                values.len(),
                self.columns.len()
            );
        }
        self.samples.push(values);
        Ok(())
    }

    pub fn build(self) -> Result<CovarianceMatrix> {
        CovarianceMatrix::compute(self.columns, &self.samples)
    }

    /// Returns the number of samples collected.
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }
}
