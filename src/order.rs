//! Topological ordering of tables based on foreign keys dependencies.

use std::collections::{HashMap, HashSet, VecDeque};
use crate::schema::Table;
use anyhow::{bail, Result};
use tracing::debug;

pub fn calculate_execution_order(tables: &[Table]) -> Result<Vec<String>> {
    debug!("Calculating topological execution order for {} tables", tables.len());

    if tables.is_empty() {
        return Ok(Vec::new());
    }

    let graph = build_dependency_graph(tables);
    let mut in_degree = calculate_in_degree(&graph, tables);

    let mut queue: VecDeque<String> = tables
        .iter()
        .filter(|t| in_degree.get(&t.name).copied().unwrap_or(0) == 0)
        .map(|t| t.name.clone())
        .collect();

    debug!("Starting with {} root tables (0 in degree)", queue.len());

    let mut execution_order = Vec::with_capacity(tables.len());
    while let Some(table_name) = queue.pop_front() {
        execution_order.push(table_name.clone());

        if let Some(children) = graph.get(&table_name) {
            for child in children {
                if let Some(degree) = in_degree.get_mut(child) {
                    *degree -= 1;

                    if *degree == 0 {
                        queue.push_back(child.clone());
                    }
                }
            }
        }
    }

    if execution_order.len() != tables.len() {
        let missing: Vec<_> = tables
            .iter()
            .filter(|t| !execution_order.contains(&t.name))
            .map(|t| t.name.as_str())
            .collect();

        let cycle_info = detect_cycle(&graph, tables)?;

        bail!(
            "Circular dependency detected in foreign keys. \
             Unable to process {} tables: [{}]. \
             Cycle: {}",
            missing.len(),
            missing.join(", "),
            cycle_info
        );
    }

    debug!(
        "Topological sort complete: {} tables ordered",
        execution_order.len()
    );

    Ok(execution_order)
}

fn build_dependency_graph(tables: &[Table]) -> HashMap<String, HashSet<String>> {
    let mut graph: HashMap<String, HashSet<String>> = HashMap::new();

    for table in tables {
        graph.entry(table.name.clone()).or_insert_with(HashSet::new);
    }

    for table in tables {
        for fk in &table.foreign_keys {
            graph
                .entry(fk.target_table.clone())
                .or_insert_with(HashSet::new)
                .insert(table.name.clone());
        }
    }

    graph
}

fn calculate_in_degree(
    graph: &HashMap<String, HashSet<String>>,
    tables: &[Table],
) -> HashMap<String, usize> {
    let mut in_degree: HashMap<String, usize> = HashMap::new();

    // Initialize all tables with 0 in-degree
    for table in tables {
        in_degree.insert(table.name.clone(), 0);
    }

    // Count incoming edges for each table
    for table in tables {
        for fk in &table.foreign_keys {
            *in_degree.entry(table.name.clone()).or_insert(0) += 1;
        }
    }

    in_degree
}

fn detect_cycle(graph: &HashMap<String, HashSet<String>>, tables: &[Table]) -> Result<String> {
    let mut visited = HashSet::new();
    let mut rec_stack = HashSet::new();
    let mut path = Vec::new();

    for table in tables {
        if !visited.contains(&table.name) {
            if let Some(cycle_path) = dfs_cycle_detection(
                &table.name,
                graph,
                &mut visited,
                &mut rec_stack,
                &mut path,
            ) {
                return Ok(cycle_path.join("->"));
            }
        }
    }

    Ok("Unknown cycle".to_string())
}

fn dfs_cycle_detection(
    node: &str,
    graph: &HashMap<String, HashSet<String>>,
    visited: &mut HashSet<String>,
    rec_stack: &mut HashSet<String>,
    path: &mut Vec<String>,
) -> Option<Vec<String>> {
    visited.insert(node.to_string());
    rec_stack.insert(node.to_string());
    path.push(node.to_string());

    // Check all children (tables that depend on this one)
    if let Some(children) = graph.get(node) {
        for child in children {
            if !visited.contains(child) {
                if let Some(cycle) = dfs_cycle_detection(child, graph, visited, rec_stack, path) {
                    return Some(cycle);
                }
            } else if rec_stack.contains(child) {
                // Found a cycle!
                let cycle_start_idx = path.iter().position(|n| n == child).unwrap_or(0);
                let mut cycle_path = path[cycle_start_idx..].to_vec();
                cycle_path.push(child.to_string());
                return Some(cycle_path);
            }
        }
    }

    path.pop();
    rec_stack.remove(node);
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, DataType, ForeignKey};

    #[test]
    fn test_simple_linear_order() -> Result<()> {
        // users -> orders (orders.user_id -> users.id)
        let tables = vec![
            Table::new("users".to_string(), vec![], vec![]),
            Table::new(
                "orders".to_string(),
                vec![],
                vec![ForeignKey::new(
                    "user_id".to_string(),
                    "users".to_string(),
                    "id".to_string(),
                )],
            ),
        ];

        let order = calculate_execution_order(&tables)?;

        assert_eq!(order.len(), 2);
        let users_idx = order.iter().position(|t| t == "users").unwrap();
        let orders_idx = order.iter().position(|t| t == "orders").unwrap();
        assert!(users_idx < orders_idx, "users must come before orders");

        Ok(())
    }

    #[test]
    fn test_complex_dependencies() -> Result<()> {
        // users -> orders -> line_items
        let tables = vec![
            Table::new("users".to_string(), vec![], vec![]),
            Table::new(
                "orders".to_string(),
                vec![],
                vec![ForeignKey::new(
                    "user_id".to_string(),
                    "users".to_string(),
                    "id".to_string(),
                )],
            ),
            Table::new(
                "line_items".to_string(),
                vec![],
                vec![ForeignKey::new(
                    "order_id".to_string(),
                    "orders".to_string(),
                    "id".to_string(),
                )],
            ),
        ];

        let order = calculate_execution_order(&tables)?;

        assert_eq!(order.len(), 3);
        let users_idx = order.iter().position(|t| t == "users").unwrap();
        let orders_idx = order.iter().position(|t| t == "orders").unwrap();
        let items_idx = order.iter().position(|t| t == "line_items").unwrap();

        assert!(users_idx < orders_idx);
        assert!(orders_idx < items_idx);

        Ok(())
    }

    #[test]
    fn test_independent_tables() -> Result<()> {
        let tables = vec![
            Table::new("products".to_string(), vec![], vec![]),
            Table::new("categories".to_string(), vec![], vec![]),
            Table::new("users".to_string(), vec![], vec![]),
        ];

        let order = calculate_execution_order(&tables)?;

        assert_eq!(order.len(), 3);
        // All tables should be included, order doesn't matter
        assert!(order.contains(&"products".to_string()));
        assert!(order.contains(&"categories".to_string()));
        assert!(order.contains(&"users".to_string()));

        Ok(())
    }

    #[test]
    fn test_circular_dependency_detection() {
        // Create a cycle: A -> B -> A
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

        let result = calculate_execution_order(&tables);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Circular dependency"));
    }

    #[test]
    fn test_self_referential_table() -> Result<()> {
        // employees table with self-referential manager_id
        let tables = vec![
            Table::new(
                "employees".to_string(),
                vec![],
                vec![ForeignKey::new(
                    "manager_id".to_string(),
                    "employees".to_string(),
                    "id".to_string(),
                )],
            ),
        ];

        // This creates a cycle, should be detected
        let result = calculate_execution_order(&tables);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_empty_tables() -> Result<()> {
        let tables: Vec<Table> = vec![];
        let order = calculate_execution_order(&tables)?;
        assert_eq!(order.len(), 0);
        Ok(())
    }
}
