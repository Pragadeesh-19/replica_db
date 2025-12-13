mod strategy;

use std::collections::HashMap;
use std::sync::Arc;
use crate::genome::DatabaseGenome;
use anyhow::{bail, Context, Result};
use rand::thread_rng;
use tracing::{debug, info, warn};
use crate::order::calculate_execution_order;
use crate::schema::{ForeignKey, Table};
use crate::synth::strategy::synthesize_primary_key;

pub type KeyStore = HashMap<String, Vec<PrimaryKeyValue>>;

pub type PrimaryKeyValue = String;

#[derive(Debug, Clone)]
pub struct SynthesisConfig {
    pub rows_per_table: usize,
    pub seed: Option<u64>,
    pub strict_fk_enforcement: bool,
}

impl Default for SynthesisConfig {
    fn default() -> Self {
        Self {
            rows_per_table: 1000,
            seed: None,
            strict_fk_enforcement: true,
        }
    }
}

pub struct Synthesizer {
    genome: Arc<DatabaseGenome>,
    execution_order: Vec<String>,
    config: SynthesisConfig,
}

impl Synthesizer {

    pub fn new(genome: DatabaseGenome, config: SynthesisConfig) -> Result<Self> {
        info!("Initializing Synthesizer");

        let execution_order = calculate_execution_order(&genome.tables)
            .context("Failed to calculate topological execution order")?;

        info!(
            tables = execution_order.len(),
            "Synthesizer initialized with execution order: {:?}",
            execution_order
        );

        Ok(Self {
            genome: Arc::new(genome),
            execution_order,
            config,
        })
    }

    pub fn execution_order(&self) -> &[String] {
        &self.execution_order
    }

    pub fn genome(&self) -> &DatabaseGenome {
        &self.genome
    }

    pub fn generate(&self) -> Result<GenerationResult> {
        info!("Starting data generation for {} tables", self.execution_order.len());

        let mut key_store: KeyStore = HashMap::new();
        let mut table_data: HashMap<String, TableData> = HashMap::new();

        for table_name in &self.execution_order {
            let table = self.genome.get_table(table_name)
                .ok_or_else(|| anyhow::anyhow!("Table '{}' not found in genome", table_name))?;

            debug!(table = %table_name, "Generating data for table");

            let (copy_data, pk_values) = self.generate_table_data(table, &key_store)?;

            // Cache primary keys for FK resolution
            if !pk_values.is_empty() {
                key_store.insert(table_name.clone(), pk_values);
            }

            table_data.insert(table_name.clone(), TableData {
                copy_format: copy_data,
                row_count: self.config.rows_per_table,
            });
        }

        let total_rows: usize = table_data.values().map(|t| t.row_count).sum();
        info!(
            tables_generated = table_data.len(),
            total_rows = total_rows,
            "Data generation complete"
        );

        Ok(GenerationResult { table_data })
    }

    fn generate_table_data(
        &self,
        table: &Table,
        key_store: &KeyStore,
    ) -> Result<(String, Vec<PrimaryKeyValue>)> {
        // Validate FK dependencies first
        self.validate_foreign_key_dependencies(table, key_store)?;

        let mut rng = thread_rng();
        let mut primary_key_counter: i64 = 0;
        let mut primary_key_values: Vec<PrimaryKeyValue> = Vec::new();

        // Pre-allocate string buffer (estimate: 100 bytes per row)
        let estimated_size = self.config.rows_per_table * 100;
        let mut copy_data = String::with_capacity(estimated_size);

        // Build FK lookup map for fast access
        let fk_map: HashMap<&str, &ForeignKey> = table
            .foreign_keys
            .iter()
            .map(|fk| (fk.source_col.as_str(), fk))
            .collect();

        // Generate rows
        for _ in 0..self.config.rows_per_table {
            let mut row_values: Vec<String> = Vec::with_capacity(table.columns.len());

            for column in &table.columns {
                let value = if column.is_primary_key {
                    // Primary Key: Auto-increment or UUID
                    let pk = synthesize_primary_key(&column.data_type, &mut primary_key_counter);
                    primary_key_values.push(pk.clone());
                    pk
                } else if let Some(fk) = fk_map.get(column.name.as_str()) {
                    // Foreign Key: Sample from parent KeyStore
                    let parent_keys = key_store.get(&fk.target_table)
                        .context(format!(
                            "KeyStore missing parent table '{}' for FK '{}'",
                            fk.target_table,
                            column.name
                        ))?;

                    strategy::synthesize_foreign_key(parent_keys, &mut rng)
                        .context(format!(
                            "Failed to generate FK '{}' from parent '{}'",
                            column.name,
                            fk.target_table
                        ))?
                } else {
                    // Regular Column: Sample from Distribution
                    let distribution = self.genome.get_distribution(&table.name, &column.name)
                        .context(format!(
                            "Distribution not found for column '{}.{}'",
                            table.name,
                            column.name
                        ))?;

                    strategy::synthesize_value(distribution, &mut rng)
                        .context(format!(
                            "Failed to synthesize value for column '{}.{}'",
                            table.name,
                            column.name
                        ))?
                };

                row_values.push(value);
            }

            // Join columns with TAB and append newline
            copy_data.push_str(&row_values.join("\t"));
            copy_data.push('\n');
        }

        Ok((copy_data, primary_key_values))
    }

    fn validate_foreign_key_dependencies(
        &self,
        table: &Table,
        key_store: &KeyStore,
    ) -> Result<()> {
        for fk in &table.foreign_keys {
            if self.config.strict_fk_enforcement {
                match key_store.get(&fk.target_table) {
                    None => {
                        bail!(
                            "Topological sort failed or parent table {} has zero rows. \
                            Cannot generate foreign key '{}' for table '{}'",
                            fk.target_table,
                            fk.source_col,
                            table.name
                        );
                    }
                    Some(keys) if keys.is_empty() => {
                        bail!(
                            "Parent table '{}' has zero rows. \
                             Cannot generate foreign key '{}' for table '{}'",
                            fk.target_table,
                            fk.source_col,
                            table.name
                        );
                    }
                    _ => {}
                }
            } else {
                if !key_store.contains_key(&fk.target_table) {
                    warn!(
                        table = %table.name,
                        fk_target = %fk.target_table,
                        "Parent table not yet generated; FK may be invalid"
                    );
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct GenerationResult {
    pub table_data: HashMap<String, TableData>,
}

impl GenerationResult {

    pub fn total_rows(&self) -> usize {
        self.table_data.values().map(|t| t.row_count).sum()
    }

    /// Returns data for a specific table.
    pub fn get_table_data(&self, table_name: &str) -> Option<&TableData> {
        self.table_data.get(table_name)
    }

    pub fn get_copy_data(&self, table_name: &str) -> Option<&str> {
        self.table_data.get(table_name).map(|t| t.copy_format.as_str())
    }
}

#[derive(Debug)]
pub struct TableData {
    pub copy_format: String,
    pub row_count: usize,
}

impl TableData {

    pub fn size_bytes(&self) -> usize {
        self.copy_format.len()
    }

    pub fn as_copy_data(&self) -> &str {
        &self.copy_format
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, DataType, ForeignKey};
    use crate::math::{Distribution, Histogram};

    fn create_test_genome() -> DatabaseGenome {
        let tables = vec![
            Table::new(
                "users".to_string(),
                vec![
                    Column::new("id".to_string(), DataType::Integer, false, true),
                    Column::new("name".to_string(), DataType::Text, false, false),
                ],
                vec![],
            ),
            Table::new(
                "orders".to_string(),
                vec![
                    Column::new("id".to_string(), DataType::Integer, false, true),
                    Column::new("user_id".to_string(), DataType::Integer, false, false),
                ],
                vec![ForeignKey::new(
                    "user_id".to_string(),
                    "users".to_string(),
                    "id".to_string(),
                )],
            ),
        ];

        // Create sample distributions
        let mut distributions = HashMap::new();

        // users.id - not needed (primary key)
        // users.name
        distributions.insert(
            DatabaseGenome::make_key("users", "name"), 
            Distribution::new(
                None,
                None,
                0,
                100,
                3,
                Histogram::Categorical {
                    frequencies: [
                        ("Alice".to_string(), 30),
                        ("Bob".to_string(), 40),
                        ("Charlie".to_string(), 30),
                    ].iter().cloned().collect(),
                    truncated: false,
                },
            ),
        );

        // orders.id - not needed (primary key)
        // orders.user_id - not needed (foreign key)

        DatabaseGenome::new(tables, distributions)
    }

    #[test]
    fn test_synthesizer_initialization() -> Result<()> {
        let genome = create_test_genome();
        let config = SynthesisConfig::default();

        let synth = Synthesizer::new(genome, config)?;

        assert_eq!(synth.execution_order().len(), 2);
        assert_eq!(synth.execution_order()[0], "users");
        assert_eq!(synth.execution_order()[1], "orders");

        Ok(())
    }

    #[test]
    fn test_synthesizer_with_cycle() {
        let tables = vec![
            Table::new(
                "table_a".to_string(),
                vec![],
                vec![ForeignKey::new(
                    "b_id".to_string(),
                    "table_b".to_string(),
                    "id".to_string(),
                )],
            ),
            Table::new(
                "table_b".to_string(),
                vec![],
                vec![ForeignKey::new(
                    "a_id".to_string(),
                    "table_a".to_string(),
                    "id".to_string(),
                )],
            ),
        ];

        let genome = DatabaseGenome::new(tables, HashMap::new());
        let config = SynthesisConfig::default();

        let result = Synthesizer::new(genome, config);
        assert!(result.is_err());
    }

    #[test]
    fn test_config_defaults() {
        let config = SynthesisConfig::default();
        assert_eq!(config.rows_per_table, 1000);
        assert!(config.seed.is_none());
        assert!(config.strict_fk_enforcement);
    }

    #[test]
    fn test_generation_result_methods() {
        let mut table_data = HashMap::new();
        table_data.insert(
            "users".to_string(),
            TableData {
                copy_format: "1\tAlice\n2\tBob\n".to_string(),
                row_count: 2,
            },
        );

        let result = GenerationResult { table_data };

        assert_eq!(result.total_rows(), 2);
        assert!(result.get_table_data("users").is_some());
        assert!(result.get_copy_data("users").is_some());
    }
}
