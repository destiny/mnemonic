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

use std::sync::Mutex;

use postgres::{Client, NoTls};
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

pub struct PostgresStorage {
    client: Mutex<Client>,
    temporal_fabric_edges: bool,
}

const POSTGRES_OPEN_ENDED_VALID_TO: i64 = i64::MAX;

impl PostgresStorage {
    pub fn new(connection_str: &str, temporal_fabric_edges: bool) -> Result<Self> {
        let client = Client::connect(connection_str, NoTls)?;
        let storage = Self {
            client: Mutex::new(client),
            temporal_fabric_edges,
        };
        storage.init()?;
        Ok(storage)
    }

    fn with_client<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Client) -> Result<T>,
    {
        let mut guard = self
            .client
            .lock()
            .map_err(|_| EngineError::Internal("postgres client mutex poisoned".to_string()))?;
        f(&mut guard)
    }

    fn init(&self) -> Result<()> {
        self.with_client(|client| {
            client.batch_execute(
                "
                CREATE TABLE IF NOT EXISTS data_cell (
                    id TEXT NOT NULL,
                    cell_type TEXT NOT NULL,
                    format TEXT NOT NULL,
                    content BYTEA NOT NULL,
                    valid_from BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT,
                    valid_to BIGINT NOT NULL DEFAULT 9223372036854775807,
                    children TEXT NOT NULL,
                    PRIMARY KEY (id, valid_to)
                );

                CREATE TABLE IF NOT EXISTS meta_cell (
                    id TEXT NOT NULL,
                    cell_type TEXT NOT NULL,
                    format TEXT NOT NULL,
                    content BYTEA NOT NULL,
                    valid_from BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT,
                    valid_to BIGINT NOT NULL DEFAULT 9223372036854775807,
                    children TEXT NOT NULL,
                    PRIMARY KEY (id, valid_to)
                );

                CREATE TABLE IF NOT EXISTS fabric_cell (
                    id TEXT NOT NULL,
                    valid_from BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT,
                    valid_to BIGINT NOT NULL DEFAULT 9223372036854775807,
                    PRIMARY KEY (id, valid_to)
                );

                CREATE TABLE IF NOT EXISTS fabric_edge (
                    parent_id TEXT NOT NULL,
                    child_id TEXT NOT NULL,
                    relation_type TEXT NOT NULL,
                    ordinal BIGINT NOT NULL,
                    valid_from BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT,
                    valid_to BIGINT NOT NULL DEFAULT 9223372036854775807,
                    PRIMARY KEY (parent_id, relation_type, ordinal, valid_to)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS data_cell_one_active_per_id
                ON data_cell(id)
                WHERE valid_to = 9223372036854775807;

                CREATE UNIQUE INDEX IF NOT EXISTS meta_cell_one_active_per_id
                ON meta_cell(id)
                WHERE valid_to = 9223372036854775807;

                CREATE UNIQUE INDEX IF NOT EXISTS fabric_edge_one_active_per_slot
                ON fabric_edge(parent_id, relation_type, ordinal)
                WHERE valid_to = 9223372036854775807;
                ",
            )?;
            Ok(())
        })
    }

    fn insert_cell_in_table(&self, table: CellTable, cell: &Cell) -> Result<()> {
        let children = serde_json::to_string(&cell.children)?;
        let cell_type = serde_json::to_string(&cell.cell_type)?;
        let format = serde_json::to_string(&cell.format)?;
        let sql = format!(
            "INSERT INTO {} (id, cell_type, format, content, valid_from, valid_to, children)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            table.as_str()
        );

        self.with_client(|client| {
            client.execute(
                &sql,
                &[
                    &cell.id.to_string(),
                    &cell_type,
                    &format,
                    &cell.content,
                    &cell.valid_from,
                    &cell.valid_to,
                    &children,
                ],
            )?;

            if matches!(cell.cell_type, CellType::Container) {
                client.execute(
                    "INSERT INTO fabric_cell (id, valid_from, valid_to)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (id, valid_to)
                     DO UPDATE SET valid_from = EXCLUDED.valid_from",
                    &[&cell.id.to_string(), &cell.valid_from, &cell.valid_to],
                )?;
            }

            Ok(())
        })
    }

    fn table_for_cell_type(cell_type: &CellType) -> CellTable {
        match cell_type {
            CellType::Meta => CellTable::Meta,
            _ => CellTable::Data,
        }
    }

    fn cell_from_row(row: &postgres::Row) -> Result<Cell> {
        let id_str: String = row.get(0);
        let cell_type_str: String = row.get(1);
        let format_str: String = row.get(2);
        let content: Vec<u8> = row.get(3);
        let valid_from: i64 = row.get(4);
        let valid_to: i64 = row.get(5);
        let children_str: String = row.get(6);

        let id = Uuid::parse_str(&id_str)
            .map_err(|err| EngineError::InvalidData(format!("invalid UUID in row: {err}")))?;
        let cell_type = serde_json::from_str(&cell_type_str)?;
        let format = serde_json::from_str(&format_str)?;
        let children = serde_json::from_str(&children_str)?;

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
             WHERE id = $1 AND valid_from <= $2 AND valid_to > $2
             ORDER BY valid_from DESC
             LIMIT 1",
            table.as_str()
        );

        self.with_client(|client| {
            let id_str = id.to_string();
            let row = client.query_opt(&sql, &[&id_str, &ts])?;
            row.map(|r| Self::cell_from_row(&r)).transpose()
        })
    }

    fn latest_materialized_ts_from_table(&self, table: CellTable, id: Uuid) -> Result<Option<i64>> {
        let sql = format!(
            "WITH timeline AS (
                SELECT MAX(valid_from) AS ts FROM {table} WHERE id = $1
                UNION ALL
                SELECT MAX(valid_to) AS ts FROM {table} WHERE id = $1 AND valid_to < $2
            )
            SELECT MAX(ts) FROM timeline",
            table = table.as_str()
        );

        self.with_client(|client| {
            let id_str = id.to_string();
            let row = client.query_one(&sql, &[&id_str, &POSTGRES_OPEN_ENDED_VALID_TO])?;
            Ok(row.get(0))
        })
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
}

impl Storage for PostgresStorage {
    fn current_timestamp(&self) -> Result<i64> {
        self.with_client(|client| {
            let row = client.query_one(
                "SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT",
                &[],
            )?;
            Ok(row.get(0))
        })
    }

    fn open_ended_valid_to(&self) -> i64 {
        POSTGRES_OPEN_ENDED_VALID_TO
    }

    fn insert_cell(&self, cell: &Cell) -> Result<()> {
        self.insert_cell_in_table(Self::table_for_cell_type(&cell.cell_type), cell)
    }

    fn reserve_update_timestamp(&self, id: Uuid, candidate_ts: i64) -> Result<i64> {
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

    fn get_cell_at(&self, id: Uuid, ts: i64) -> Result<Cell> {
        if let Some(cell) = self.get_cell_at_from_table(CellTable::Data, id, ts)? {
            return Ok(cell);
        }

        if let Some(cell) = self.get_cell_at_from_table(CellTable::Meta, id, ts)? {
            return Ok(cell);
        }

        Err(EngineError::NotFound)
    }

    fn close_active_version(&self, id: Uuid, now_ts: i64) -> Result<()> {
        let Some(table) = self.active_table_for_id(id, now_ts)? else {
            return Err(EngineError::NotFound);
        };

        let sql = format!(
            "UPDATE {}
             SET valid_to = $2
             WHERE id = $1 AND valid_from <= $2 AND valid_to > $2",
            table.as_str()
        );

        self.with_client(|client| {
            let id_str = id.to_string();
            let changed = client.execute(&sql, &[&id_str, &now_ts])?;
            if changed == 0 {
                return Err(EngineError::NotFound);
            }

            client.execute(
                "UPDATE fabric_cell
                 SET valid_to = $2
                 WHERE id = $1 AND valid_from <= $2 AND valid_to > $2",
                &[&id_str, &now_ts],
            )?;

            Ok(())
        })
    }

    fn insert_edge(&self, edge: &FabricEdge, now_ts: i64) -> Result<()> {
        self.with_client(|client| {
            let parent_id = edge.parent_id.to_string();
            let child_id = edge.child_id.to_string();
            let relation_type = serde_json::to_string(&edge.relation_type)?;

            if self.temporal_fabric_edges {
                client.execute(
                    "UPDATE fabric_edge
                     SET valid_to = $4
                     WHERE parent_id = $1
                       AND relation_type = $2
                       AND ordinal = $3
                       AND valid_from <= $4
                       AND valid_to > $4",
                    &[&parent_id, &relation_type, &edge.ordinal, &now_ts],
                )?;

                client.execute(
                    "INSERT INTO fabric_edge (parent_id, child_id, relation_type, ordinal, valid_from, valid_to)
                     VALUES ($1, $2, $3, $4, $5, $6)",
                    &[
                        &parent_id,
                        &child_id,
                        &relation_type,
                        &edge.ordinal,
                        &now_ts,
                        &POSTGRES_OPEN_ENDED_VALID_TO,
                    ],
                )?;
            } else {
                client.execute(
                    "DELETE FROM fabric_edge
                     WHERE parent_id = $1
                       AND relation_type = $2
                       AND ordinal = $3",
                    &[&parent_id, &relation_type, &edge.ordinal],
                )?;

                let zero: i64 = 0;
                client.execute(
                    "INSERT INTO fabric_edge (parent_id, child_id, relation_type, ordinal, valid_from, valid_to)
                     VALUES ($1, $2, $3, $4, $5, $6)",
                    &[
                        &parent_id,
                        &child_id,
                        &relation_type,
                        &edge.ordinal,
                        &zero,
                        &POSTGRES_OPEN_ENDED_VALID_TO,
                    ],
                )?;
            }

            Ok(())
        })
    }

    fn next_relation_ordinal(&self, parent_id: Uuid, relation_type: &RelationType) -> Result<i64> {
        self.with_client(|client| {
            let parent = parent_id.to_string();
            let relation = serde_json::to_string(relation_type)?;
            let row = client.query_one(
                "SELECT COALESCE(MAX(ordinal), -1) + 1
                 FROM fabric_edge
                 WHERE parent_id = $1
                   AND relation_type = $2
                   AND valid_to = $3",
                &[&parent, &relation, &POSTGRES_OPEN_ENDED_VALID_TO],
            )?;
            Ok(row.get(0))
        })
    }

    fn get_children_by_relation(
        &self,
        parent_id: Uuid,
        relation_type: &RelationType,
        ts: i64,
    ) -> Result<Vec<Uuid>> {
        self.with_client(|client| {
            let parent = parent_id.to_string();
            let relation = serde_json::to_string(relation_type)?;
            let rows = client.query(
                "SELECT child_id
                 FROM fabric_edge
                 WHERE parent_id = $1
                   AND relation_type = $2
                   AND valid_from <= $3
                   AND valid_to > $3
                 ORDER BY ordinal ASC",
                &[&parent, &relation, &ts],
            )?;

            let mut result = Vec::with_capacity(rows.len());
            for row in rows {
                let child_id_str: String = row.get(0);
                let child_id = Uuid::parse_str(&child_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid child UUID in fabric_edge: {err}"))
                })?;
                result.push(child_id);
            }

            Ok(result)
        })
    }

    fn get_edges_for_parent(&self, parent_id: Uuid, ts: i64) -> Result<Vec<FabricEdge>> {
        self.with_client(|client| {
            let parent = parent_id.to_string();
            let rows = client.query(
                "SELECT parent_id, child_id, relation_type, ordinal
                 FROM fabric_edge
                 WHERE parent_id = $1
                   AND valid_from <= $2
                   AND valid_to > $2
                 ORDER BY relation_type ASC, ordinal ASC",
                &[&parent, &ts],
            )?;

            let mut result = Vec::with_capacity(rows.len());
            for row in rows {
                let parent_id_str: String = row.get(0);
                let child_id_str: String = row.get(1);
                let relation_type_str: String = row.get(2);
                let ordinal: i64 = row.get(3);

                let parent_id = Uuid::parse_str(&parent_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid parent UUID in fabric_edge: {err}"))
                })?;
                let child_id = Uuid::parse_str(&child_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid child UUID in fabric_edge: {err}"))
                })?;
                let relation_type = serde_json::from_str(&relation_type_str)?;

                result.push(FabricEdge {
                    parent_id,
                    child_id,
                    relation_type,
                    ordinal,
                });
            }

            Ok(result)
        })
    }
}
