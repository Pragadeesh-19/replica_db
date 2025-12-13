use std::collections::{HashMap, HashSet};
use anyhow::{Result, Context};
use sqlx::{PgPool, Row};
use tracing::{debug, warn};
use crate::schema::{Column, DataType, ForeignKey, Table};

pub async fn introspect(pool: &PgPool) -> Result<Vec<Table>> {
    debug!("Starting schema introspection");

    let table_names = fetch_table_names(pool).await?;
    debug!("Discovered {} tables", table_names.len());

    let columns_map = fetch_columns(pool, &table_names).await?;

    let primary_keys = fetch_primary_keys(pool).await?;

    let foreign_keys_map = fetch_foreign_keys(pool).await?;

    let mut tables = Vec::with_capacity(table_names.len());

    for table_name in table_names {
        let mut columns = columns_map
            .get(&table_name)
            .cloned()
            .unwrap_or_default();

        if let Some(pk_cols) = primary_keys.get(&table_name) {
            for col in &mut columns {
                if pk_cols.contains(&col.name) {
                    col.is_primary_key = true;
                }
            }
        }

        let foreign_keys = foreign_keys_map
            .get(&table_name)
            .cloned()
            .unwrap_or_default();
        tables.push(Table::new(table_name, columns, foreign_keys));
    }

    debug!("Introspection complete: {} table processed", tables.len());
    Ok(tables)
}

async fn fetch_table_names(pool: &PgPool) -> Result<Vec<String>> {
    let query = r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    "#;

    let rows = sqlx::query(query)
        .fetch_all(pool)
        .await
        .context("Failed to fetch table names from information_schema")?;

    let tables: Vec<String> = rows
        .into_iter()
        .map(|row| row.try_get::<String, _>("table_name"))
        .collect::<Result<Vec<_>, _>>()
        .context("Failed to parse table names")?;

    Ok(tables)
}

async fn fetch_columns(pool: &PgPool, table_names: &[String]) -> Result<HashMap<String, Vec<Column>>> {
    if table_names.is_empty() {
        return Ok(HashMap::new());
    }

    let query = r#"
        SELECT
            table_name,
            column_name,
            data_type,
            udt_name,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_name, ordinal_position
    "#;

    let rows = sqlx::query(query)
        .fetch_all(pool)
        .await
        .context("Failed to fetch columns")?;

    let mut columns_map: HashMap<String, Vec<Column>> = HashMap::new();

    for row in rows {
        let table_name: String = row.try_get("table_name")?;
        let column_name: String = row.try_get("column_name")?;
        let sql_type: String = row.try_get("data_type")?;
        let udt_name: String = row.try_get("udt_name")?;
        let is_nullable: String = row.try_get("is_nullable")?;

        let data_type = map_sql_type_to_datatype(&sql_type, &udt_name, &table_name, &column_name);
        let is_nullable = is_nullable.eq_ignore_ascii_case("YES");

        let column = Column::new(
            column_name,
            data_type,
            is_nullable,
            false,
        );

        columns_map
            .entry(table_name)
            .or_insert_with(Vec::new)
            .push(column);
    }

    Ok(columns_map)
}

fn map_sql_type_to_datatype(sql_type: &str, udt_name: &str, table_name: &str, column_name: &str) -> DataType {

    let normalized = sql_type.to_lowercase();
    let udt_normalized = udt_name.to_lowercase();

    match normalized.as_str() {
        "integer" | "int" | "smallint" | "bigint" => DataType::Integer,

        "real" | "double precision" | "numeric" | "decimal" | "float" => DataType::Float,

        "character varying" | "varchar" | "character" | "char" | "text" => DataType::Text,

        "timestamp" | "timestamp without time zone" | "timestamp with time zone"
        | "timestamptz" | "date" | "time" => DataType::Timestamp,

        "boolean" | "bool" => DataType::Boolean,

        "uuid" => DataType::Uuid,

        "user-defined" => map_udt_type(&udt_normalized, table_name, column_name),

        "array" => {
            if udt_normalized.starts_with('_') {
                let base_type = &udt_normalized[1..];
                map_udt_type(base_type, table_name, column_name)
            } else {
                warn_unknown_type(sql_type, udt_name, table_name, column_name)
            }
        }

        _ => {
            map_udt_type(&udt_normalized, table_name, column_name)
        }
    }
}

fn map_udt_type(udt_name: &str, table_name: &str, column_name: &str) -> DataType {
    match udt_name {
        "int2" | "int4" | "int8" | "smallint" | "integer" | "bigint" => DataType::Integer,

        "float4" | "float8" | "numeric" => DataType::Float,

        "varchar" | "text" | "bpchar" | "char" => DataType::Text,

        "timestamp" | "timestamptz" | "date" | "time" | "timetz" => DataType::Timestamp,

        "bool" => DataType::Boolean,

        "uuid" => DataType::Uuid,

        _ => warn_unknown_type(udt_name, udt_name, table_name, column_name),
    }
}

fn warn_unknown_type(sql_type: &str, udt_name: &str, table_name: &str, column_name: &str) -> DataType {
    warn!(
        table = %table_name,
        column_name = %column_name,
        sql_type = %sql_type,
        udt_name = %udt_name,
        "Unknown data type encountered, defaulting to Text"
    );
    DataType::Text
}

async fn fetch_primary_keys(pool: &PgPool) -> Result<HashMap<String, HashSet<String>>> {
    let query = r#"
        SELECT
            kcu.table_name,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
    "#;

    let rows = sqlx::query(query)
        .fetch_all(pool)
        .await
        .context("Failed to fetch primary key")?;

    let mut pk_map: HashMap<String, HashSet<String>> = HashMap::new();

    for row in rows {
        let table_name = row.try_get("table_name")?;
        let column_name = row.try_get("column_name")?;

        pk_map
            .entry(table_name)
            .or_insert_with(HashSet::new)
            .insert(column_name);
    }
    Ok(pk_map)
}

async fn fetch_foreign_keys(pool: &PgPool) -> Result<HashMap<String, Vec<ForeignKey>>> {
    let query = r#"
        SELECT
            kcu.table_name AS source_table,
            kcu.column_name AS source_column,
            ccu.table_name AS target_table,
            ccu.column_name AS target_column
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.referential_constraints rc
            ON kcu.constraint_name = rc.constraint_name
            AND kcu.table_schema = rc.constraint_schema
        JOIN information_schema.constraint_column_usage ccu
            ON rc.unique_constraint_name = ccu.constraint_name
            AND rc.unique_constraint_schema = ccu.constraint_schema
        WHERE kcu.table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY kcu.table_name, kcu.ordinal_position
    "#;

    let rows = sqlx::query(query)
        .fetch_all(pool)
        .await
        .context("Failed to fetch foreign key constraints")?;

    let mut fk_map: HashMap<String, Vec<ForeignKey>> = HashMap::new();

    for row in rows {
        let source_table: String = row.try_get("source_table")?;
        let source_column: String = row.try_get("source_column")?;
        let target_table: String = row.try_get("target_table")?;
        let target_column: String = row.try_get("target_column")?;

        let fk = ForeignKey::new(source_column, target_table, target_column);

        fk_map
            .entry(source_table)
            .or_insert_with(Vec::new)
            .push(fk);
    }

    debug!("Discovered foreign keys in {} tables", fk_map.len());

    Ok(fk_map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_mapping_integers() {
        let dt = map_sql_type_to_datatype("integer", "int4", "test", "id");
        assert_eq!(dt, DataType::Integer);

        let dt = map_sql_type_to_datatype("bigint", "int8", "test", "id");
        assert_eq!(dt, DataType::Integer);
    }

    #[test]
    fn test_type_mapping_text() {
        let dt = map_sql_type_to_datatype("character varying", "varchar", "test", "name");
        assert_eq!(dt, DataType::Text);

        let dt = map_sql_type_to_datatype("text", "text", "test", "desc");
        assert_eq!(dt, DataType::Text);
    }

    #[test]
    fn test_type_mapping_timestamp() {
        let dt = map_sql_type_to_datatype("timestamp without time zone", "timestamp", "test", "created");
        assert_eq!(dt, DataType::Timestamp);
    }

    #[test]
    fn test_type_mapping_unknown_fallback() {
        let dt = map_sql_type_to_datatype("exotic_type", "custom", "test", "col");
        assert_eq!(dt, DataType::Text);
    }
}


