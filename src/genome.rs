//! The portable DNA of database schema

use std::collections::HashMap;
use std::path::Path;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use crate::copula::CovarianceMatrix;
use crate::math::Distribution;
use crate::schema::{DataType, Table};

type TableColumn = (String, String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseGenome {

    #[serde(default = "default_version")]
    pub version: String,

    #[serde(default)]
    pub created_at: Option<String>,

    #[serde(default)]
    pub source_database: Option<String>,

    pub tables: Vec<Table>,
    pub distributions: HashMap<String, Distribution>,

    #[serde(default)]
    pub correlations: HashMap<String, CovarianceMatrix>,

}

fn default_version() -> String {
    "1.0.0".to_string()
}

impl DatabaseGenome {
    pub fn new(tables: Vec<Table>, distributions: HashMap<String, Distribution>) -> Self {
        Self {
            version: default_version(),
            created_at: Some(chrono::Utc::now().to_rfc3339()),
            source_database: None,
            tables,
            distributions,
            correlations: HashMap::new(),
        }
    }

    pub fn with_metadata(
        tables: Vec<Table>,
        distributions: HashMap<String, Distribution>,
        source_database: Option<String>,
    ) -> Self {
        Self {
            version: default_version(),
            created_at: Some(chrono::Utc::now().to_rfc3339()),
            source_database,
            tables,
            distributions,
            correlations: HashMap::new(),
        }
    }

    pub fn with_correlations(
        tables: Vec<Table>,
        distributions: HashMap<String, Distribution>,
        correlations: HashMap<String, CovarianceMatrix>,
        source_database: Option<String>,
    ) -> Self {
        Self {
            version: default_version(),
            created_at: Some(chrono::Utc::now().to_rfc3339()),
            source_database,
            tables,
            distributions,
            correlations,
        }
    }

    pub fn make_key(table: &str, column: &str) -> String {
        format!("{}.{}", table, column)
    }

    pub fn save_to_file(&self, path: &Path) -> Result<()> {
        info!(path = ?path, "Saving database genome to file");

        let json = serde_json::to_string_pretty(self)
            .context("Failed to serialize databasegenome to JSON")?;

        std::fs::write(path, json)
            .context("Failed to write DatabaseGenome to file")?;

        let file_size = std::fs::metadata(path)
            .map(|m| m.len())
            .unwrap_or(0);

        info!(
            path = ?path,
            size_bytes = file_size,
            tables = self.tables.len(),
            distributions = self.distributions.len(),
            "DatabaseGenome saved successfully"
        );

        Ok(())
    }

    pub fn load_from_file(path: &Path) -> Result<Self> {
        info!(path = ?path, "Loading DatabaseGenome from file");

        let json = std::fs::read_to_string(path)
            .context("Failed to read DatabaseGenome file")?;

        let genome: DatabaseGenome = serde_json::from_str(&json)
            .context("Failed to deserialize DatabaseGenome from JSON")?;

        debug!(
            version = %genome.version,
            tables = genome.tables.len(),
            distributions = genome.distributions.len(),
            "DatabaseGenome loaded successfully"
        );

        Ok(genome)
    }

    pub fn get_distribution(&self, table: &str, column: &str) -> Option<&Distribution> {
        let key = Self::make_key(table, column);
        self.distributions.get(&key)
    }

    pub fn get_correlation(&self, table: &str) -> Option<&CovarianceMatrix> {
        self.correlations.get(table)
    }

    pub fn get_correlation_mut(&mut self, table: &str) -> Option<&mut CovarianceMatrix> {
        self.correlations.get_mut(table)
    }

    /// Returns a table by name.
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// Returns the total number of columns across all tables.
    pub fn total_columns(&self) -> usize {
        self.tables.iter().map(|t| t.columns.len()).sum()
    }

    /// Returns the total number of foreign keys across all tables.
    pub fn total_foreign_keys(&self) -> usize {
        self.tables.iter().map(|t| t.foreign_keys.len()).sum()
    }

    /// Validates that distributions exist for all columns in all tables.
    pub fn validate(&self) -> Result<()> {
        let mut missing_distributions = Vec::new();
        let mut correlation_errors = Vec::new();

        for table in &self.tables {
            // Validate distributions for all columns
            for column in &table.columns {
                let key = Self::make_key(&table.name, &column.name);
                if !self.distributions.contains_key(&key) {
                    missing_distributions.push(format!("{}.{}", table.name, column.name));
                }
            }

            // Validate correlation matrix if present
            if let Some(corr_matrix) = self.correlations.get(&table.name) {
                // Get numeric columns from table
                let numeric_columns: Vec<&str> = table
                    .columns
                    .iter()
                    .filter(|c| matches!(c.data_type, DataType::Integer | DataType::Float))
                    .map(|c| c.name.as_str())
                    .collect();

                // Check dimension matches
                if corr_matrix.dimension != corr_matrix.columns.len() {
                    correlation_errors.push(format!(
                        "Table '{}': correlation matrix dimension ({}) does not match column count ({})",
                        table.name,
                        corr_matrix.dimension,
                        corr_matrix.columns.len()
                    ));
                }

                // Check that correlation columns are subset of numeric columns
                for corr_col in &corr_matrix.columns {
                    if !numeric_columns.contains(&corr_col.as_str()) {
                        correlation_errors.push(format!(
                            "Table '{}': correlation matrix references non-numeric or non-existent column '{}'",
                            table.name,
                            corr_col
                        ));
                    }
                }

                // Check matrix data size
                let expected_size = corr_matrix.dimension * corr_matrix.dimension;
                if corr_matrix.matrix_data.len() != expected_size {
                    correlation_errors.push(format!(
                        "Table '{}': correlation matrix data size ({}) does not match dimension² ({})",
                        table.name,
                        corr_matrix.matrix_data.len(),
                        expected_size
                    ));
                }
            }
        }

        // Report all validation errors
        let mut errors = Vec::new();

        if !missing_distributions.is_empty() {
            errors.push(format!(
                "Missing distributions for {} columns: {}",
                missing_distributions.len(),
                missing_distributions.join(", ")
            ));
        }

        if !correlation_errors.is_empty() {
            errors.push(format!(
                "Correlation validation errors:\n  - {}",
                correlation_errors.join("\n  - ")
            ));
        }

        if !errors.is_empty() {
            anyhow::bail!(
                "DatabaseGenome validation failed:\n{}",
                errors.join("\n")
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, DataType};

    #[test]
    fn test_genome_with_correlations() {
        let tables = vec![
            Table::new(
                "users".to_string(),
                vec![
                    Column::new("id".to_string(), DataType::Integer, false, true),
                    Column::new("age".to_string(), DataType::Integer, false, false),
                    Column::new("salary".to_string(), DataType::Float, false, false),
                ],
                vec![],
            ),
        ];

        let mut distributions = HashMap::new();
        distributions.insert(
            DatabaseGenome::make_key("users", "id"),
            crate::math::Distribution::new(Some(1.0), Some(100.0), 0, 100, 100, crate::math::Histogram::Numeric { bins: vec![], frequencies: vec![] }),
        );
        distributions.insert(
            DatabaseGenome::make_key("users", "age"),
            crate::math::Distribution::new(Some(18.0), Some(65.0), 0, 100, 48, crate::math::Histogram::Numeric { bins: vec![], frequencies: vec![] }),
        );
        distributions.insert(
            DatabaseGenome::make_key("users", "salary"),
            crate::math::Distribution::new(Some(30000.0), Some(200000.0), 0, 100, 95, crate::math::Histogram::Numeric { bins: vec![], frequencies: vec![] }),
        );

        let mut correlations = HashMap::new();
        let cov = CovarianceMatrix {
            columns: vec!["age".to_string(), "salary".to_string()],
            matrix_data: vec![1.0, 0.8, 0.8, 1.0],
            dimension: 2,
        };
        correlations.insert("users".to_string(), cov);

        let genome = DatabaseGenome::with_correlations(
            tables,
            distributions,
            correlations,
            Some("test_db".to_string()),
        );

        assert_eq!(genome.correlations.len(), 1);
        assert!(genome.get_correlation("users").is_some());

        // Validate should pass
        genome.validate().expect("Validation should pass");
    }

    #[test]
    fn test_correlation_validation_invalid_column() {
        let tables = vec![
            Table::new(
                "users".to_string(),
                vec![
                    Column::new("id".to_string(), DataType::Integer, false, true),
                    Column::new("age".to_string(), DataType::Integer, false, false),
                ],
                vec![],
            ),
        ];

        let mut distributions = HashMap::new();
        distributions.insert(
            DatabaseGenome::make_key("users", "id"),
            crate::math::Distribution::new(Some(1.0), Some(100.0), 0, 100, 100, crate::math::Histogram::Numeric { bins: vec![], frequencies: vec![] }),
        );
        distributions.insert(
            DatabaseGenome::make_key("users", "age"),
            crate::math::Distribution::new(Some(18.0), Some(65.0), 0, 100, 48, crate::math::Histogram::Numeric { bins: vec![], frequencies: vec![] }),
        );

        let mut correlations = HashMap::new();
        // Invalid: references "salary" which doesn't exist
        let cov = CovarianceMatrix {
            columns: vec!["age".to_string(), "salary".to_string()],
            matrix_data: vec![1.0, 0.8, 0.8, 1.0],
            dimension: 2,
        };
        correlations.insert("users".to_string(), cov);

        let genome = DatabaseGenome::with_correlations(
            tables,
            distributions,
            correlations,
            None,
        );

        // Validation should fail
        assert!(genome.validate().is_err());
    }

    #[test]
    fn test_get_correlation_mut() {
        let tables = vec![
            Table::new("test".to_string(), vec![], vec![]),
        ];

        let mut correlations = HashMap::new();
        let cov = CovarianceMatrix {
            columns: vec!["a".to_string(), "b".to_string()],
            matrix_data: vec![1.0, 0.5, 0.5, 1.0],
            dimension: 2,
        };
        correlations.insert("test".to_string(), cov);

        let mut genome = DatabaseGenome::with_correlations(
            tables,
            HashMap::new(),
            correlations,
            None,
        );

        // Test mutable access
        if let Some(corr) = genome.get_correlation_mut("test") {
            corr.matrix_data[1] = 0.9;
        }

        let corr = genome.get_correlation("test").unwrap();
        assert_eq!(corr.matrix_data[1], 0.9);
    }
}