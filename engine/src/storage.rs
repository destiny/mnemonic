// Copyright 2026 Arion Yau
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use rusqlite::{Connection, params};
use uuid::Uuid;

use crate::error::{EngineError, Result};
use crate::models::{Cell, FabricEdge, RelationType};

pub struct SqliteStorage {
    conn: Connection,
}

impl SqliteStorage {
    pub fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        let storage = Self { conn };
        storage.init()?;
        Ok(storage)
    }

    fn init(&self) -> Result<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS cells (
                    id TEXT NOT NULL,
                    cell_type TEXT NOT NULL,
                    format TEXT NOT NULL,
                    content BLOB NOT NULL,
                    valid_from INTEGER NOT NULL,
                    valid_to INTEGER NOT NULL,
                    children TEXT NOT NULL,
                    PRIMARY KEY (id, valid_to)
                )",
            params![],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS cell_edges (
                    parent_id TEXT NOT NULL,
                    child_id TEXT NOT NULL,
                    relation_type TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    PRIMARY KEY (parent_id, relation_type, ordinal)
                )",
            params![],
        )?;
        Ok(())
    }

    pub fn insert_cell(&self, cell: &Cell) -> Result<()> {
        self.insert_internal(cell, "INSERT")
    }

    pub fn insert_cell_with_replace(&self, cell: &Cell) -> Result<()> {
        self.insert_internal(cell, "INSERT OR REPLACE")
    }

    fn insert_internal(&self, cell: &Cell, verb: &str) -> Result<()> {
        let children = serde_json::to_string(&cell.children)?;
        let valid_from = cell
            .valid_from
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let valid_to = cell
            .valid_to
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let sql = format!(
            "{} INTO cells (id, cell_type, format, content, valid_from, valid_to, children)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            verb
        );

        self.conn.execute(
            &sql,
            params![
                cell.id.to_string(),
                serde_json::to_string(&cell.cell_type)?,
                serde_json::to_string(&cell.format)?,
                cell.content,
                valid_from,
                valid_to,
                children,
            ],
        )?;
        Ok(())
    }

    pub fn insert_edge(&self, edge: &FabricEdge) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO cell_edges (parent_id, child_id, relation_type, ordinal)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                edge.parent_id.to_string(),
                edge.child_id.to_string(),
                serde_json::to_string(&edge.relation_type)?,
                edge.ordinal,
            ],
        )?;
        Ok(())
    }

    pub fn next_relation_ordinal(
        &self,
        parent_id: Uuid,
        relation_type: &RelationType,
    ) -> Result<i64> {
        let mut stmt = self.conn.prepare(
            "SELECT COALESCE(MAX(ordinal), -1) + 1 FROM cell_edges
             WHERE parent_id = ?1 AND relation_type = ?2",
        )?;

        let ordinal = stmt.query_row(
            params![parent_id.to_string(), serde_json::to_string(relation_type)?,],
            |row| row.get(0),
        )?;

        Ok(ordinal)
    }

    pub fn get_children_by_relation(
        &self,
        parent_id: Uuid,
        relation_type: &RelationType,
    ) -> Result<Vec<Uuid>> {
        let mut stmt = self.conn.prepare(
            "SELECT child_id FROM cell_edges
             WHERE parent_id = ?1 AND relation_type = ?2
             ORDER BY ordinal ASC",
        )?;

        let mapped = stmt.query_map(
            params![parent_id.to_string(), serde_json::to_string(relation_type)?,],
            |row| {
                let child_id_str: String = row.get(0)?;
                Uuid::parse_str(&child_id_str).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })
            },
        )?;

        let mut result = Vec::new();
        for row in mapped {
            result.push(row?);
        }

        Ok(result)
    }

    pub fn get_cell(&self, id: Uuid) -> Result<Cell> {
        let mut stmt = self.conn.prepare(
            "SELECT id, cell_type, format, content, valid_from, valid_to, children
            FROM cells WHERE id = ?1 ORDER BY valid_to DESC LIMIT 1",
        )?;
        let cell = stmt
            .query_row(params![id.to_string()], |row| {
                let id_str: String = row.get(0)?;
                let cell_type_str: String = row.get(1)?;
                let format_str: String = row.get(2)?;
                let content: Vec<u8> = row.get(3)?;
                let valid_from_millis: i64 = row.get(4)?;
                let valid_to_millis: i64 = row.get(5)?;
                let children_str: String = row.get(6)?;

                let valid_from = std::time::UNIX_EPOCH
                    + std::time::Duration::from_millis(valid_from_millis as u64);
                let valid_to = std::time::UNIX_EPOCH
                    + std::time::Duration::from_millis(valid_to_millis as u64);

                let parsed_id = Uuid::parse_str(&id_str).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?;

                let cell_type = serde_json::from_str(&cell_type_str).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?;

                let format = serde_json::from_str(&format_str).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?;

                let children = serde_json::from_str(&children_str).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        6,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?;

                Ok(Cell {
                    id: parsed_id,
                    cell_type,
                    format,
                    content,
                    valid_from,
                    valid_to,
                    children,
                })
            })
            .map_err(|_| EngineError::NotFound)?;
        Ok(cell)
    }
}
