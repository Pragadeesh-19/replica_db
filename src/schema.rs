use std::fmt;
use std::fmt::Formatter;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Integer,
    Float,
    Text,
    Timestamp,
    Boolean,
    Uuid,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Integer => write!(f, "integer"),
            DataType::Float => write!(f, "float"),
            DataType::Text => write!(f, "text"),
            DataType::Timestamp => write!(f, "timestamp"),
            DataType::Boolean => write!(f, "boolean"),
            DataType::Uuid => write!(f, "uuid"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: bool,
    pub is_primary_key: bool,
}

impl Column {
    pub fn new(name: String, data_type: DataType, is_nullable: bool, is_primary_key: bool) -> Self {
        Self {
            name,
            data_type,
            is_nullable,
            is_primary_key,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    pub source_col: String,
    pub target_table: String,
    pub target_col: String,
}

impl ForeignKey {
    pub fn new(source_col: String, target_table: String, target_col: String) -> Self {
        Self {
            source_col,
            target_table,
            target_col,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub foreign_keys: Vec<ForeignKey>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>, foreign_keys: Vec<ForeignKey>) -> Self {
        Self {
            name,
            columns,
            foreign_keys,
        }
    }

    pub fn primary_keys(&self) -> Vec<&Column> {
        self.columns
            .iter()
            .filter(|col| col.is_primary_key)
            .collect()
    }

    pub fn has_foreign_keys(&self) -> bool {
        !self.foreign_keys.is_empty()
    }
}