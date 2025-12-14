extern crate core;

#[allow(unused_imports)]
#[allow(unused_variables)]
#[allow(dead_code)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Semaphore;
use tracing_subscriber::EnvFilter;
use crate::genome::DatabaseGenome;
use crate::postgres::introspect;
use crate::scanner::profile_columns;
use crate::synth::{SynthesisConfig, Synthesizer};

mod schema;
mod postgres;
mod math;
mod scanner;
mod genome;
mod order;
mod synth;
mod copula;

#[derive(Parser)]
#[command(
    name = "replica_db",
    version,
    about = "Fast statistical database twin generator",
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {

    Scan {

        #[arg(short = 'u', long = "url", required = true)]
        url: String,

        /// Output genome file path
        #[arg(short = 'o', long = "output", default_value = "genome.json")]
        output: String,

        /// Maximum concurrent table profiling tasks
        #[arg(short = 'j', long = "jobs", default_value_t = 10)]
        parallel: usize,
    },

    Gen {
        /// Input genome file path
        #[arg(short = 'g', long = "genome", required = true)]
        genome: String,

        /// Number of rows to generate per table
        #[arg(short = 'r', long = "rows", default_value_t = 1000)]
        rows: usize,

        /// Random seed for reproducibility (optional)
        #[arg(short = 's', long = "seed")]
        seed: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Scan {
            url,
            output,
            parallel,
        } => {
            scan_database(&url, &output, parallel).await?;
        }
        Commands::Gen { genome, rows, seed } => {
            generate_data(&genome, rows, seed).await?;
        }
    }

    Ok(())
}

async fn scan_database(url: &str, output_path: &str, parallel_jobs: usize) -> Result<()> {
    eprintln!("replica_db Scanner");

    eprintln!("Connecting to database...");
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(30))
        .connect(url)
        .await
        .context("Failed to connect to database")?;

    eprintln!("Connected");

    let multi_progress = MultiProgress::new();

    let introspect_spinner = multi_progress.add(ProgressBar::new_spinner());
    introspect_spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .context("Invalid spinner template")?,
    );
    introspect_spinner.set_message("Introspecting schema...");
    introspect_spinner.enable_steady_tick(Duration::from_millis(100));

    let tables = introspect(&pool)
        .await
        .context("Failed to introspect database schema")?;

    introspect_spinner.finish_with_message(format!("✓ Discovered {} tables", tables.len()));

    if tables.is_empty() {
        eprintln!("No tables found in database");
        return Ok(());
    }

    eprintln!("\nProfiling column statistics...");

    let (all_distributions, all_correlations) = profile_tables_parallel(&pool, &tables, parallel_jobs, &multi_progress)
        .await
        .context("Failed to profile tables")?;

    eprintln!(
        "\nProfiled {} columns across {} tables",
        all_distributions.len(),
        tables.len()
    );

    if !all_correlations.is_empty() {
        eprintln!("Computed correlations for {} tables", all_correlations.len());
    }

    eprintln!("\nCreating genome...");

    let genome = DatabaseGenome::with_correlations(
        tables,
        all_distributions,
        all_correlations,
        Some(extract_db_name(url)),
    );

    genome
        .validate()
        .context("Genome validation failed")?;

    genome
        .save_to_file(Path::new(output_path))
        .context("Failed to save genome file")?;

    let file_size = std::fs::metadata(output_path)
        .map(|m| m.len())
        .unwrap_or(0);

    eprintln!("Genome saved to: {}", output_path);
    eprintln!(
        "  Size: {} KB ({} tables, {} columns)",
        file_size / 1024,
        genome.tables.len(),
        genome.total_columns()
    );

    eprintln!("\nScan complete!");

    Ok(())
}

async fn profile_tables_parallel(
    pool: &PgPool,
    tables: &[schema::Table],
    parallel_jobs: usize,
    multi_progress: &MultiProgress,
) -> Result<(
    HashMap<String, math::Distribution>,
    HashMap<String, copula::CovarianceMatrix>,
)> {
    let semaphore = Arc::new(Semaphore::new(parallel_jobs));
    let pool = Arc::new(pool.clone());

    // Create progress bars for each table
    let progress_bars: Vec<_> = tables
        .iter()
        .map(|table| {
            let pb = multi_progress.add(ProgressBar::new_spinner());
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.cyan} {prefix:>20} {msg}")
                    .unwrap_or_else(|_| ProgressStyle::default_spinner()),
            );
            pb.set_prefix(table.name.clone());
            pb.set_message("waiting...");
            pb
        })
        .collect();

    // Spawn profiling tasks
    let tasks: Vec<_> = tables
        .iter()
        .zip(progress_bars.iter())
        .map(|(table, pb)| {
            let table = table.clone();
            let pb = pb.clone();
            let pool = Arc::clone(&pool);
            let semaphore = Arc::clone(&semaphore);

            tokio::spawn(async move {
                // Acquire semaphore permit
                let _permit = semaphore.acquire().await.map_err(|e| {
                    anyhow::anyhow!("Failed to acquire semaphore: {}", e)
                })?;

                pb.set_message("profiling...");

                //Now returns tuple (distributions, covariance)
                let (distributions, covariance) = profile_columns(&pool, &table).await.map_err(|e| {
                    pb.finish_with_message(format!("✗ failed: {}", e));
                    e
                })?;

                //Update progress message to show correlation status
                let msg = if covariance.is_some() {
                    format!("{} columns + correlations", distributions.len())
                } else {
                    format!("{} columns", distributions.len())
                };
                pb.finish_with_message(msg);

                Ok::<_, anyhow::Error>((table.name.clone(), distributions, covariance))
            })
        })
        .collect();

    // Collect results
    let mut all_distributions = HashMap::new();
    let mut all_correlations = HashMap::new();  // ⭐ NEW

    let mut stream = futures_util::stream::iter(tasks).buffer_unordered(parallel_jobs);

    while let Some(result) = stream.next().await {
        let (table_name, distributions, covariance) = result
            .context("Task panicked")?
            .context("Profiling failed")?;

        for (col_name, dist) in distributions {
            // Use the new key format: "table_name.column_name"
            let key = genome::DatabaseGenome::make_key(&table_name, &col_name);
            all_distributions.insert(key, dist);
        }

        //Collect correlation matrix if computed
        if let Some(cov) = covariance {
            all_correlations.insert(table_name, cov);
        }
    }

    Ok((all_distributions, all_correlations))
}

async fn generate_data(genome_path: &str, rows_per_table: usize, seed: Option<u64>) -> Result<()> {
    eprintln!("replica_db Generator");

    eprintln!("Loading genome from: {}", genome_path);

    let genome = DatabaseGenome::load_from_file(Path::new(genome_path))
        .context("Failed to load genome file")?;

    eprintln!(
        "Loaded: {} tables, {} columns",
        genome.tables.len(),
        genome.total_columns()
    );

    let config = SynthesisConfig {
        rows_per_table,
        seed,
        strict_fk_enforcement: true,
    };

    if let Some(s) = seed {
        eprintln!("Using seed: {} (reproducible mode)", s);
    }

    eprintln!("Initializing synthesizer...");

    let synthesizer = Synthesizer::new(genome, config)
        .context("Failed to initialize synthesizer (check for circular dependencies)")?;

    eprintln!("Execution order: {:?}", synthesizer.execution_order());

    eprintln!("Generating {} rows per table...", rows_per_table);

    let result = synthesizer
        .generate()
        .context("Failed to generate synthetic data")?;

    eprintln!(
        "Generated {} total rows across {} tables",
        result.total_rows(),
        result.table_data.len()
    );

    eprintln!("\nOutputting SQL to stdout...");
    eprintln!("Tip: Pipe to psql → ghost_forge gen -g genome.json | psql target_db");
    eprintln!();

    // Output in execution order for proper FK resolution
    for table_name in synthesizer.execution_order() {
        if let Some(table_data) = result.get_table_data(table_name) {
            // Get column names from genome
            let table = synthesizer
                .genome()
                .get_table(table_name)
                .context(format!("Table '{}' not found in genome", table_name))?;

            let column_names: Vec<_> = table.columns.iter().map(|c| c.name.as_str()).collect();

            println!(
                "COPY {} ({}) FROM stdin;",
                table_name,
                column_names.join(", ")
            );

            // Output data
            print!("{}", table_data.as_copy_data());

            // End of data marker
            println!("\\.");
            println!();
        }
    }

    eprintln!("Generation complete!");

    Ok(())
}

fn extract_db_name(url: &str) -> String {
    url.rsplit('/')
        .next()
        .and_then(|s| s.split('?').next())
        .unwrap_or("unknown")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_db_name() {
        assert_eq!(
            extract_db_name("postgresql://localhost/production"),
            "production"
        );
        assert_eq!(
            extract_db_name("postgresql://user:pass@host:5432/mydb?sslmode=require"),
            "mydb"
        );
        assert_eq!(extract_db_name("postgresql://localhost/"), "");
    }

    #[test]
    fn test_cli_parsing() {
        // Test that CLI can be parsed
        let cli = Cli::try_parse_from(&["ghost_forge", "scan", "-u", "postgresql://localhost/db"])
            .unwrap();

        match cli.command {
            Commands::Scan { url, output, .. } => {
                assert_eq!(url, "postgresql://localhost/db");
                assert_eq!(output, "genome.json");
            }
            _ => panic!("Expected Scan command"),
        }
    }
}