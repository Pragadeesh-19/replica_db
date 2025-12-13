use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use anyhow::{Context, Result};
use sqlx::{Row, ValueRef};
use sqlx::postgres::{PgPool, PgRow};
use sqlx::query::Query;
use tracing::{debug, info, warn};
use crate::copula::CovarianceMatrix;
use crate::math::{Distribution, DistributionBuilder, Reservoir, DEFAULT_RESERVOIR_CAPACITY};
use crate::schema::{Column, DataType, Table};

struct ColumnState {
    data_type: DataType,
    null_count: u64,
    numeric_reservoir: Option<Reservoir<f64>>,
    text_reservoir: Option<Reservoir<String>>,
}

impl ColumnState {
    fn new(data_type: DataType) -> Self {
        let (numeric_reservoir, text_reservoir) = match data_type {
            DataType::Integer | DataType::Float | DataType::Timestamp => {
                (Some(Reservoir::new(DEFAULT_RESERVOIR_CAPACITY)), None)
            }
            DataType::Text | DataType::Boolean | DataType::Uuid => {
                (None, Some(Reservoir::new(DEFAULT_RESERVOIR_CAPACITY)))
            }
        };

        Self {
            data_type,
            null_count: 0,
            numeric_reservoir,
            text_reservoir,
        }
    }
}

pub async fn profile_columns(
    pool: &PgPool,
    table: &Table,
) -> Result<(HashMap<String, Distribution>, Option<CovarianceMatrix>)> {
    info!(table = %table.name, "Starting column profiling");

    if table.columns.is_empty() {
        return Ok((HashMap::new(), None));
    }

    let column_names: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
    let query = build_select_query(&table.name, &column_names);

    debug!(
        table = %table.name,
        columns = column_names.len(),
        query = %query,
        "Constructed profiling query"
    );

    let numeric_columns: Vec<&Column> = table
        .columns
        .iter()
        .filter(|c| matches!(c.data_type, DataType::Integer | DataType::Float))
        .collect();

    let has_numeric_columns = !numeric_columns.is_empty();

    debug!(
        table = %table.name,
        numeric_columns = numeric_columns.len(),
        "Identified numeric columns for correlation tracking"
    );

    let mut column_states = initialize_column_states(&table.columns);
    let total_rows = Arc::new(AtomicU64::new(0));

    // Joint numeric reservoir for correlation
    let mut numeric_row_reservoir: Option<Reservoir<Vec<f64>>> = if has_numeric_columns {
        Some(Reservoir::new(DEFAULT_RESERVOIR_CAPACITY))
    } else {
        None
    };

    //Stream and process rows
    stream_and_profile(
        pool,
        &query,
        &table.columns,
        &numeric_columns,
        &mut column_states,
        &mut numeric_row_reservoir,
        &total_rows,
    )
        .await
        .context("Failed during streaming profiling")?;

    //Convert reservoir samples to distributions
    let distributions = build_distributions(&table.columns, column_states, &total_rows);

    //Compute covariance matrix if applicable
    let covariance = if numeric_columns.len() >= 2 {
        if let Some(ref reservoir) = numeric_row_reservoir {
            if reservoir.sample_size() > 1 {
                let ordered_names: Vec<String> = numeric_columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();

                let samples: Vec<Vec<f64>> = reservoir.sample().to_vec();

                match CovarianceMatrix::compute(ordered_names, &samples) {
                    Ok(cov) => {
                        info!(
                            table = %table.name,
                            numeric_cols = numeric_columns.len(),
                            samples = samples.len(),
                            "Computed correlation matrix"
                        );
                        Some(cov)
                    }
                    Err(e) => {
                        warn!(
                            table = %table.name,
                            error = %e,
                            "Failed to compute correlation matrix"
                        );
                        None
                    }
                }
            } else {
                debug!(
                    table = %table.name,
                    "Insufficient samples for correlation matrix"
                );
                None
            }
        } else {
            None
        }
    } else {
        debug!(
            table = %table.name,
            numeric_cols = numeric_columns.len(),
            "Less than 2 numeric columns, skipping correlation"
        );
        None
    };

    let row_count = total_rows.load(Ordering::Relaxed);
    info!(
        table = %table.name,
        rows_processed = row_count,
        columns_profiled = distributions.len(),
        has_correlations = covariance.is_some(),
        "Profiling complete"
    );

    Ok((distributions, covariance))
}

fn build_select_query(table_name: &str, column_names: &[&str]) -> String {
    let columns_clause = column_names.join(", ");
    format!("SELECT {} FROM {}", columns_clause, table_name)
}

fn initialize_column_states(columns: &[Column]) -> HashMap<String, ColumnState> {
    columns
        .iter()
        .map(|col| {
            let state = ColumnState::new(col.data_type.clone());
            (col.name.clone(), state)
        })
        .collect()
}

async fn stream_and_profile(
    pool: &PgPool,
    query: &str,
    columns: &[Column],
    numeric_columns: &[&Column],
    column_states: &mut HashMap<String, ColumnState>,
    numeric_row_reservoir: &mut Option<Reservoir<Vec<f64>>>,
    total_rows: &Arc<AtomicU64>,
) -> Result<()> {
    use futures::TryStreamExt;

    // Build index map for numeric columns
    let numeric_indices: Vec<usize> = numeric_columns
        .iter()
        .filter_map(|nc| columns.iter().position(|c| c.name == nc.name))
        .collect();

    // Execute query and get a stream
    let mut stream = sqlx::query(query).fetch(pool);

    // Process each row from the stream
    while let Some(row) = stream.try_next().await? {
        total_rows.fetch_add(1, Ordering::Relaxed);

        //Track numeric values for correlation (pairwise deletion)
        let mut numeric_row: Option<Vec<f64>> = if !numeric_indices.is_empty() {
            Some(Vec::with_capacity(numeric_indices.len()))
        } else {
            None
        };

        let mut row_has_null_numeric = false;

        for (col_idx, col) in columns.iter().enumerate() {
            if let Some(state) = column_states.get_mut(&col.name) {
                // Process for individual column distribution
                let process_result = process_row_value(&row, &col.name, state);

                // Extract numeric value for correlation tracking
                if let Some(ref mut num_row) = numeric_row {
                    if numeric_indices.contains(&col_idx) {
                        match extract_numeric_value(&row, &col.name, &col.data_type) {
                            Ok(Some(value)) => {
                                num_row.push(value);
                            }
                            Ok(None) => {
                                row_has_null_numeric = true;
                            }
                            Err(_) => {
                                row_has_null_numeric = true;
                            }
                        }
                    }
                }

                if let Err(e) = process_result {
                    warn!(
                        column = %col.name,
                        error = %e,
                        "Failed to process column value"
                    );
                }
            }
        }

        //Add to numeric row reservoir if no NULLs in numeric columns
        if let Some(num_row) = numeric_row {
            if !row_has_null_numeric && num_row.len() == numeric_indices.len() {
                if let Some(reservoir) = numeric_row_reservoir {
                    reservoir.add(num_row);
                }
            }
        }
    }

    Ok(())
}

fn extract_numeric_value(row: &PgRow, column_name: &str, data_type: &DataType) -> Result<Option<f64>> {
    let value_ref = row.try_get_raw(column_name);
    if value_ref?.is_null() {
        return Ok(None);
    }

    match data_type {
        DataType::Integer => {
            let value = row.try_get::<i64, _>(column_name)
                .or_else(|_| row.try_get::<i32, _>(column_name).map(|v| v as i64))
                .or_else(|_| row.try_get::<i16, _>(column_name).map(|v| v as i64))
                .context("Failed to extract integer value")?;

            Ok(Some(value as f64))
        }

        DataType::Float => {
            let value = row.try_get::<f64, _>(column_name)
                .or_else(|_| row.try_get::<f32, _>(column_name).map(|v| v as f64))
                .context("Failed to extract float value")?;

            Ok(Some(value))
        }

        _ => {
            anyhow::bail!("Non-numeric data type")
        }
    }
}

fn process_row_value(row: &PgRow, column_name: &str, state: &mut ColumnState) -> Result<()> {
    // Check if value is NULL
    let value_ref = row.try_get_raw(column_name)?;

    if value_ref.is_null() {
        state.null_count += 1;
        return Ok(());
    }

    // Extract value based on data type
    match &state.data_type {
        DataType::Integer => {
            // Try i64 first, then i32, then i16
            let value = row.try_get::<i64, _>(column_name)
                .or_else(|_| row.try_get::<i32, _>(column_name).map(|v| v as i64))
                .or_else(|_| row.try_get::<i16, _>(column_name).map(|v| v as i64))
                .context("Failed to extract integer value")?;

            if let Some(ref mut reservoir) = state.numeric_reservoir {
                reservoir.add(value as f64);
            }
        }

        DataType::Float => {
            // Try f64 first, then f32
            let value = row.try_get::<f64, _>(column_name)
                .or_else(|_| row.try_get::<f32, _>(column_name).map(|v| v as f64))
                .context("Failed to extract float value")?;

            if let Some(ref mut reservoir) = state.numeric_reservoir {
                reservoir.add(value);
            }
        }

        DataType::Timestamp => {
            // Extract timestamp and convert to Unix epoch for numeric analysis
            if let Ok(ts) = row.try_get::<chrono::NaiveDateTime, _>(column_name) {
                let epoch_seconds = ts.and_utc().timestamp() as f64;
                if let Some(ref mut reservoir) = state.numeric_reservoir {
                    reservoir.add(epoch_seconds);
                }
            } else if let Ok(ts) = row.try_get::<chrono::DateTime<chrono::Utc>, _>(column_name) {
                let epoch_seconds = ts.timestamp() as f64;
                if let Some(ref mut reservoir) = state.numeric_reservoir {
                    reservoir.add(epoch_seconds);
                }
            } else {
                // Fallback: treat as text
                let value: String = row.try_get(column_name)?;
                if let Some(ref mut reservoir) = state.text_reservoir {
                    reservoir.add(value);
                }
            }
        }

        DataType::Text | DataType::Uuid => {
            let value: String = row.try_get(column_name)
                .context("Failed to extract text value")?;

            if let Some(ref mut reservoir) = state.text_reservoir {
                reservoir.add(value);
            }
        }

        DataType::Boolean => {
            let value: bool = row.try_get(column_name)
                .context("Failed to extract boolean value")?;

            if let Some(ref mut reservoir) = state.text_reservoir {
                reservoir.add(value.to_string());
            }
        }
    }

    Ok(())
}

fn build_distributions(
    columns: &[Column],
    column_states: HashMap<String, ColumnState>,
    total_rows: &Arc<AtomicU64>,
) -> HashMap<String, Distribution> {
    let total_count = total_rows.load(Ordering::Relaxed);

    column_states
        .into_iter()
        .map(|(col_name, state)| {
            let distribution = build_single_distribution(state, total_count);
            (col_name, distribution)
        })
        .collect()
}

fn build_single_distribution(state: ColumnState, total_count: u64) -> Distribution {
    let mut builder = DistributionBuilder::new(total_count, state.null_count);

    // Process numeric reservoir
    if let Some(reservoir) = state.numeric_reservoir {
        for &value in reservoir.sample() {
            builder.add_numeric(value);
        }
    }

    // Process text reservoir
    if let Some(reservoir) = state.text_reservoir {
        for value in reservoir.sample() {
            builder.add_categorical(value.clone());
        }
    }

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Column;

    #[test]
    fn test_build_select_query() {
        let query = build_select_query("users", &["id", "name", "email"]);
        assert_eq!(query, "SELECT id, name, email FROM users");
    }

    #[test]
    fn test_column_state_numeric() {
        let state = ColumnState::new(DataType::Integer);
        assert!(state.numeric_reservoir.is_some());
        assert!(state.text_reservoir.is_none());
    }

    #[test]
    fn test_column_state_text() {
        let state = ColumnState::new(DataType::Text);
        assert!(state.numeric_reservoir.is_none());
        assert!(state.text_reservoir.is_some());
    }

    #[test]
    fn test_initialize_column_states() {
        let columns = vec![
            Column::new("id".to_string(), DataType::Integer, false, true),
            Column::new("name".to_string(), DataType::Text, false, false),
        ];

        let states = initialize_column_states(&columns);

        assert_eq!(states.len(), 2);
        assert!(states.contains_key("id"));
        assert!(states.contains_key("name"));
    }
}