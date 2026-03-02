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

use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

use super::Storage;
use crate::error::{EngineError, Result};
use crate::models::{Cell, CellType, FabricEdge, RelationType};

#[derive(Debug, Clone, Copy)]
enum CellTable {
    Data,
    Meta,
}

impl CellTable {
    fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data_cell",
            Self::Meta => "meta_cell",
        }
    }
}

pub struct SqliteStorage {
    conn: Connection,
    temporal_fabric_edges: bool,
}

const SQLITE_OPEN_ENDED_VALID_TO: i64 = i64::MAX;

impl SqliteStorage {
    pub fn new(path: &str, temporal_fabric_edges: bool) -> Result<Self> {
        let conn = Connection::open(path)?;
        let storage = Self {
            conn,
            temporal_fabric_edges,
        };
        storage.init()?;
        Ok(storage)
    }

    fn init(&self) -> Result<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS data_cell (
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
            "CREATE TABLE IF NOT EXISTS meta_cell (
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
            "CREATE TABLE IF NOT EXISTS fabric_cell (
                id TEXT NOT NULL,
                valid_from INTEGER NOT NULL,
                valid_to INTEGER NOT NULL,
                PRIMARY KEY (id, valid_to)
            )",
            params![],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS fabric_edge (
                parent_id TEXT NOT NULL,
                child_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                valid_from INTEGER NOT NULL,
                valid_to INTEGER NOT NULL,
                PRIMARY KEY (parent_id, relation_type, ordinal, valid_to)
            )",
            params![],
        )?;

        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS data_cell_one_active_per_id
             ON data_cell(id)
             WHERE valid_to = 9223372036854775807",
            params![],
        )?;

        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS meta_cell_one_active_per_id
             ON meta_cell(id)
             WHERE valid_to = 9223372036854775807",
            params![],
        )?;

        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS fabric_edge_one_active_per_slot
             ON fabric_edge(parent_id, relation_type, ordinal)
             WHERE valid_to = 9223372036854775807",
            params![],
        )?;

        Ok(())
    }

    pub fn insert_cell(&self, cell: &Cell) -> Result<()> {
        self.insert_cell_in_table(Self::table_for_cell_type(&cell.cell_type), cell)
    }

    fn insert_cell_in_table(&self, table: CellTable, cell: &Cell) -> Result<()> {
        let children = serde_json::to_string(&cell.children)?;

        let sql = format!(
            "INSERT INTO {} (id, cell_type, format, content, valid_from, valid_to, children)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            table.as_str()
        );

        self.conn.execute(
            &sql,
            params![
                cell.id.to_string(),
                serde_json::to_string(&cell.cell_type)?,
                serde_json::to_string(&cell.format)?,
                cell.content,
                cell.valid_from,
                cell.valid_to,
                children,
            ],
        )?;

        if matches!(cell.cell_type, CellType::Container) {
            self.conn.execute(
                "INSERT OR REPLACE INTO fabric_cell (id, valid_from, valid_to)
                 VALUES (?1, ?2, ?3)",
                params![cell.id.to_string(), cell.valid_from, cell.valid_to],
            )?;
        }

        Ok(())
    }

    fn table_for_cell_type(cell_type: &CellType) -> CellTable {
        match cell_type {
            CellType::Meta => CellTable::Meta,
            _ => CellTable::Data,
        }
    }

    fn cell_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Cell> {
        let id_str: String = row.get(0)?;
        let cell_type_str: String = row.get(1)?;
        let format_str: String = row.get(2)?;
        let content: Vec<u8> = row.get(3)?;
        let valid_from: i64 = row.get(4)?;
        let valid_to: i64 = row.get(5)?;
        let children_str: String = row.get(6)?;

        let id = Uuid::parse_str(&id_str).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
        })?;

        let cell_type = serde_json::from_str(&cell_type_str).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(err))
        })?;

        let format = serde_json::from_str(&format_str).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(err))
        })?;

        let children = serde_json::from_str(&children_str).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(err))
        })?;

        Ok(Cell {
            id,
            cell_type,
            format,
            content,
            valid_from,
            valid_to,
            children,
        })
    }

    fn get_cell_at_from_table(&self, table: CellTable, id: Uuid, ts: i64) -> Result<Option<Cell>> {
        let sql = format!(
            "SELECT id, cell_type, format, content, valid_from, valid_to, children
             FROM {}
             WHERE id = ?1 AND valid_from <= ?2 AND valid_to > ?2
             ORDER BY valid_from DESC
             LIMIT 1",
            table.as_str()
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let cell = stmt
            .query_row(params![id.to_string(), ts], Self::cell_from_row)
            .optional()?;

        Ok(cell)
    }

    fn latest_materialized_ts_from_table(&self, table: CellTable, id: Uuid) -> Result<Option<i64>> {
        let sql = format!(
            "SELECT MAX(ts) FROM (
                SELECT MAX(valid_from) AS ts FROM {} WHERE id = ?1
                UNION ALL
                SELECT MAX(CASE WHEN valid_to < {} THEN valid_to ELSE NULL END) AS ts
                FROM {} WHERE id = ?1
            )",
            table.as_str(),
            SQLITE_OPEN_ENDED_VALID_TO,
            table.as_str()
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let ts = stmt
            .query_row(params![id.to_string()], |row| row.get(0))
            .optional()?;

        Ok(ts.flatten())
    }

    pub fn reserve_update_timestamp(&self, id: Uuid, candidate_ts: i64) -> Result<i64> {
        let data_latest = self.latest_materialized_ts_from_table(CellTable::Data, id)?;
        let meta_latest = self.latest_materialized_ts_from_table(CellTable::Meta, id)?;
        let latest = match (data_latest, meta_latest) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        let next = match latest {
            Some(prev) if candidate_ts <= prev => prev.saturating_add(1),
            _ => candidate_ts,
        };

        Ok(next)
    }

    pub fn get_cell_at(&self, id: Uuid, ts: i64) -> Result<Cell> {
        if let Some(cell) = self.get_cell_at_from_table(CellTable::Data, id, ts)? {
            return Ok(cell);
        }

        if let Some(cell) = self.get_cell_at_from_table(CellTable::Meta, id, ts)? {
            return Ok(cell);
        }

        Err(EngineError::NotFound)
    }

    fn active_table_for_id(&self, id: Uuid, ts: i64) -> Result<Option<CellTable>> {
        if self
            .get_cell_at_from_table(CellTable::Data, id, ts)?
            .is_some()
        {
            return Ok(Some(CellTable::Data));
        }

        if self
            .get_cell_at_from_table(CellTable::Meta, id, ts)?
            .is_some()
        {
            return Ok(Some(CellTable::Meta));
        }

        Ok(None)
    }

    pub fn close_active_version(&self, id: Uuid, now_ts: i64) -> Result<()> {
        let Some(table) = self.active_table_for_id(id, now_ts)? else {
            return Err(EngineError::NotFound);
        };

        let sql = format!(
            "UPDATE {}
             SET valid_to = ?2
             WHERE id = ?1 AND valid_from <= ?2 AND valid_to > ?2",
            table.as_str()
        );

        let changed = self.conn.execute(&sql, params![id.to_string(), now_ts])?;
        if changed == 0 {
            return Err(EngineError::NotFound);
        }

        self.conn.execute(
            "UPDATE fabric_cell
             SET valid_to = ?2
             WHERE id = ?1 AND valid_from <= ?2 AND valid_to > ?2",
            params![id.to_string(), now_ts],
        )?;

        Ok(())
    }

    pub fn insert_edge(&self, edge: &FabricEdge, now_ts: i64) -> Result<()> {
        if self.temporal_fabric_edges {
            self.conn.execute(
                "UPDATE fabric_edge
                 SET valid_to = ?4
                 WHERE parent_id = ?1
                   AND relation_type = ?2
                   AND ordinal = ?3
                   AND valid_from <= ?4
                   AND valid_to > ?4",
                params![
                    edge.parent_id.to_string(),
                    serde_json::to_string(&edge.relation_type)?,
                    edge.ordinal,
                    now_ts,
                ],
            )?;

            self.conn.execute(
                "INSERT INTO fabric_edge (parent_id, child_id, relation_type, ordinal, valid_from, valid_to)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    edge.parent_id.to_string(),
                    edge.child_id.to_string(),
                    serde_json::to_string(&edge.relation_type)?,
                    edge.ordinal,
                    now_ts,
                    SQLITE_OPEN_ENDED_VALID_TO,
                ],
            )?;
        } else {
            self.conn.execute(
                "DELETE FROM fabric_edge
                 WHERE parent_id = ?1
                   AND relation_type = ?2
                   AND ordinal = ?3",
                params![
                    edge.parent_id.to_string(),
                    serde_json::to_string(&edge.relation_type)?,
                    edge.ordinal,
                ],
            )?;

            self.conn.execute(
                "INSERT INTO fabric_edge (parent_id, child_id, relation_type, ordinal, valid_from, valid_to)
                 VALUES (?1, ?2, ?3, ?4, 0, ?5)",
                params![
                    edge.parent_id.to_string(),
                    edge.child_id.to_string(),
                    serde_json::to_string(&edge.relation_type)?,
                    edge.ordinal,
                    SQLITE_OPEN_ENDED_VALID_TO,
                ],
            )?;
        }

        Ok(())
    }

    pub fn next_relation_ordinal(
        &self,
        parent_id: Uuid,
        relation_type: &RelationType,
    ) -> Result<i64> {
        let mut stmt = self.conn.prepare(
            "SELECT COALESCE(MAX(ordinal), -1) + 1
             FROM fabric_edge
             WHERE parent_id = ?1
               AND relation_type = ?2
               AND valid_to = 9223372036854775807",
        )?;

        let ordinal = stmt.query_row(
            params![parent_id.to_string(), serde_json::to_string(relation_type)?],
            |row| row.get(0),
        )?;

        Ok(ordinal)
    }

    pub fn get_children_by_relation(
        &self,
        parent_id: Uuid,
        relation_type: &RelationType,
        ts: i64,
    ) -> Result<Vec<Uuid>> {
        let mut stmt = self.conn.prepare(
            "SELECT child_id
             FROM fabric_edge
             WHERE parent_id = ?1
               AND relation_type = ?2
               AND valid_from <= ?3
               AND valid_to > ?3
             ORDER BY ordinal ASC",
        )?;

        let mapped = stmt.query_map(
            params![
                parent_id.to_string(),
                serde_json::to_string(relation_type)?,
                ts,
            ],
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

    pub fn get_edges_for_parent(&self, parent_id: Uuid, ts: i64) -> Result<Vec<FabricEdge>> {
        let mut stmt = self.conn.prepare(
            "SELECT parent_id, child_id, relation_type, ordinal
             FROM fabric_edge
             WHERE parent_id = ?1
               AND valid_from <= ?2
               AND valid_to > ?2
             ORDER BY relation_type ASC, ordinal ASC",
        )?;

        let mapped = stmt.query_map(params![parent_id.to_string(), ts], |row| {
            let parent_id_str: String = row.get(0)?;
            let child_id_str: String = row.get(1)?;
            let relation_type_str: String = row.get(2)?;
            let ordinal: i64 = row.get(3)?;

            let parent_id = Uuid::parse_str(&parent_id_str).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;

            let child_id = Uuid::parse_str(&child_id_str).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;

            let relation_type = serde_json::from_str(&relation_type_str).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;

            Ok(FabricEdge {
                parent_id,
                child_id,
                relation_type,
                ordinal,
            })
        })?;

        let mut result = Vec::new();
        for row in mapped {
            result.push(row?);
        }

        Ok(result)
    }
}

impl Storage for SqliteStorage {
    fn current_timestamp(&self) -> Result<i64> {
        let now: i64 = self.conn.query_row(
            "SELECT CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)",
            params![],
            |row| row.get(0),
        )?;
        Ok(now)
    }

    fn open_ended_valid_to(&self) -> i64 {
        SQLITE_OPEN_ENDED_VALID_TO
    }

    fn insert_cell(&self, cell: &Cell) -> Result<()> {
        SqliteStorage::insert_cell(self, cell)
    }

    fn reserve_update_timestamp(&self, id: Uuid, candidate_ts: i64) -> Result<i64> {
        SqliteStorage::reserve_update_timestamp(self, id, candidate_ts)
    }

    fn get_cell_at(&self, id: Uuid, ts: i64) -> Result<Cell> {
        SqliteStorage::get_cell_at(self, id, ts)
    }

    fn close_active_version(&self, id: Uuid, now_ts: i64) -> Result<()> {
        SqliteStorage::close_active_version(self, id, now_ts)
    }

    fn insert_edge(&self, edge: &FabricEdge, now_ts: i64) -> Result<()> {
        SqliteStorage::insert_edge(self, edge, now_ts)
    }

    fn next_relation_ordinal(&self, parent_id: Uuid, relation_type: &RelationType) -> Result<i64> {
        SqliteStorage::next_relation_ordinal(self, parent_id, relation_type)
    }

    fn get_children_by_relation(
        &self,
        parent_id: Uuid,
        relation_type: &RelationType,
        ts: i64,
    ) -> Result<Vec<Uuid>> {
        SqliteStorage::get_children_by_relation(self, parent_id, relation_type, ts)
    }

    fn get_edges_for_parent(&self, parent_id: Uuid, ts: i64) -> Result<Vec<FabricEdge>> {
        SqliteStorage::get_edges_for_parent(self, parent_id, ts)
    }
}
