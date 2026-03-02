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

use mysql::{Conn, Opts, Row, params, prelude::Queryable};
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

pub struct MariaDbStorage {
    conn: Mutex<Conn>,
    temporal_fabric_edges: bool,
}

const MARIADB_OPEN_ENDED_VALID_TO: i64 = 4_102_444_800_000_000; // 2100-01-01T00:00:00Z in microseconds

impl MariaDbStorage {
    pub fn new(connection_str: &str, temporal_fabric_edges: bool) -> Result<Self> {
        let opts = Opts::from_url(connection_str)
            .map_err(|err| EngineError::InvalidData(format!("invalid MariaDB URL: {err}")))?;
        let conn = Conn::new(opts)?;
        let storage = Self {
            conn: Mutex::new(conn),
            temporal_fabric_edges,
        };
        storage.init()?;
        Ok(storage)
    }

    fn with_conn<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Conn) -> Result<T>,
    {
        let mut guard = self
            .conn
            .lock()
            .map_err(|_| EngineError::Internal("mariadb connection mutex poisoned".to_string()))?;
        f(&mut guard)
    }

    fn init(&self) -> Result<()> {
        self.with_conn(|conn| {
            conn.query_drop(
                "CREATE TABLE IF NOT EXISTS data_cell (
                    id VARCHAR(36) NOT NULL,
                    cell_type TEXT NOT NULL,
                    format TEXT NOT NULL,
                    content BLOB NOT NULL,
                    valid_from BIGINT NOT NULL DEFAULT (CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 1000000 AS SIGNED)),
                    valid_to BIGINT NOT NULL DEFAULT 4102444800000000,
                    children TEXT NOT NULL,
                    PRIMARY KEY (id, valid_to)
                )",
            )?;

            conn.query_drop(
                "CREATE TABLE IF NOT EXISTS meta_cell (
                    id VARCHAR(36) NOT NULL,
                    cell_type TEXT NOT NULL,
                    format TEXT NOT NULL,
                    content BLOB NOT NULL,
                    valid_from BIGINT NOT NULL DEFAULT (CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 1000000 AS SIGNED)),
                    valid_to BIGINT NOT NULL DEFAULT 4102444800000000,
                    children TEXT NOT NULL,
                    PRIMARY KEY (id, valid_to)
                )",
            )?;

            conn.query_drop(
                "CREATE TABLE IF NOT EXISTS fabric_cell (
                    id VARCHAR(36) NOT NULL,
                    valid_from BIGINT NOT NULL DEFAULT (CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 1000000 AS SIGNED)),
                    valid_to BIGINT NOT NULL DEFAULT 4102444800000000,
                    PRIMARY KEY (id, valid_to)
                )",
            )?;

            conn.query_drop(
                "CREATE TABLE IF NOT EXISTS fabric_edge (
                    parent_id VARCHAR(36) NOT NULL,
                    child_id VARCHAR(36) NOT NULL,
                    relation_type VARCHAR(255) NOT NULL,
                    ordinal BIGINT NOT NULL,
                    valid_from BIGINT NOT NULL DEFAULT (CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 1000000 AS SIGNED)),
                    valid_to BIGINT NOT NULL DEFAULT 4102444800000000,
                    PRIMARY KEY (parent_id, relation_type, ordinal, valid_to)
                )",
            )?;

            conn.query_drop("DROP TRIGGER IF EXISTS data_cell_no_overlap_before_insert")?;
            conn.query_drop(
                "CREATE TRIGGER data_cell_no_overlap_before_insert
                 BEFORE INSERT ON data_cell FOR EACH ROW
                 BEGIN
                    IF EXISTS (
                      SELECT 1 FROM data_cell
                      WHERE id = NEW.id
                        AND NEW.valid_from < valid_to
                        AND NEW.valid_to > valid_from
                    ) THEN
                      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'data_cell temporal overlap';
                    END IF;
                 END",
            )?;

            conn.query_drop("DROP TRIGGER IF EXISTS meta_cell_no_overlap_before_insert")?;
            conn.query_drop(
                "CREATE TRIGGER meta_cell_no_overlap_before_insert
                 BEFORE INSERT ON meta_cell FOR EACH ROW
                 BEGIN
                    IF EXISTS (
                      SELECT 1 FROM meta_cell
                      WHERE id = NEW.id
                        AND NEW.valid_from < valid_to
                        AND NEW.valid_to > valid_from
                    ) THEN
                      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'meta_cell temporal overlap';
                    END IF;
                 END",
            )?;
            conn.query_drop(
                "CREATE UNIQUE INDEX IF NOT EXISTS data_cell_one_active_per_id
                 ON data_cell(id, valid_to)",
            )?;
            conn.query_drop(
                "CREATE UNIQUE INDEX IF NOT EXISTS meta_cell_one_active_per_id
                 ON meta_cell(id, valid_to)",
            )?;
            conn.query_drop(
                "CREATE INDEX IF NOT EXISTS fabric_edge_parent_lookup
                 ON fabric_edge(parent_id, relation_type, valid_from, valid_to, ordinal)",
            )?;
            conn.query_drop(
                "CREATE INDEX IF NOT EXISTS data_cell_current_lookup
                 ON data_cell(id, valid_from, valid_to)",
            )?;
            conn.query_drop(
                "CREATE INDEX IF NOT EXISTS meta_cell_current_lookup
                 ON meta_cell(id, valid_from, valid_to)",
            )?;

            Ok(())
        })
    }

    fn table_for_cell_type(cell_type: &CellType) -> CellTable {
        match cell_type {
            CellType::Meta => CellTable::Meta,
            _ => CellTable::Data,
        }
    }

    fn insert_cell_in_table(&self, table: CellTable, cell: &Cell) -> Result<()> {
        let children = serde_json::to_string(&cell.children)?;
        let cell_type = serde_json::to_string(&cell.cell_type)?;
        let format = serde_json::to_string(&cell.format)?;

        self.with_conn(|conn| {
            if cell.valid_from > 0 {
                let sql = format!(
                    "INSERT INTO {} (id, cell_type, format, content, valid_from, valid_to, children)
                     VALUES (:id, :cell_type, :format, :content, :valid_from, :valid_to, :children)",
                    table.as_str()
                );
                conn.exec_drop(
                    sql,
                    params! {
                        "id" => cell.id.to_string(),
                        "cell_type" => cell_type.clone(),
                        "format" => format.clone(),
                        "content" => cell.content.clone(),
                        "valid_from" => cell.valid_from,
                        "valid_to" => cell.valid_to,
                        "children" => children.clone(),
                    },
                )?;
            } else {
                let sql = format!(
                    "INSERT INTO {} (id, cell_type, format, content, children)
                     VALUES (:id, :cell_type, :format, :content, :children)",
                    table.as_str()
                );
                conn.exec_drop(
                    sql,
                    params! {
                        "id" => cell.id.to_string(),
                        "cell_type" => cell_type,
                        "format" => format,
                        "content" => cell.content.clone(),
                        "children" => children,
                    },
                )?;
            }

            if matches!(cell.cell_type, CellType::Container) {
                if cell.valid_from > 0 {
                    conn.exec_drop(
                        "INSERT INTO fabric_cell (id, valid_from, valid_to)
                         VALUES (:id, :valid_from, :valid_to)
                         ON DUPLICATE KEY UPDATE valid_from = VALUES(valid_from)",
                        params! {
                            "id" => cell.id.to_string(),
                            "valid_from" => cell.valid_from,
                            "valid_to" => cell.valid_to,
                        },
                    )?;
                } else {
                    conn.exec_drop(
                        "INSERT INTO fabric_cell (id)
                         VALUES (:id)
                         ON DUPLICATE KEY UPDATE id = VALUES(id)",
                        params! {
                            "id" => cell.id.to_string(),
                        },
                    )?;
                }
            }

            Ok(())
        })
    }

    fn cell_from_row(row: Row) -> Result<Cell> {
        let (id_str, cell_type_str, format_str, content, valid_from, valid_to, children_str): (
            String,
            String,
            String,
            Vec<u8>,
            i64,
            i64,
            String,
        ) = mysql::from_row_opt(row)
            .map_err(|err| EngineError::InvalidData(format!("invalid cell row shape: {err}")))?;

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
             WHERE id = :id AND valid_from <= :ts AND valid_to > :ts
             ORDER BY valid_from DESC
             LIMIT 1",
            table.as_str()
        );

        self.with_conn(|conn| {
            let row = conn.exec_first(
                sql,
                params! {
                    "id" => id.to_string(),
                    "ts" => ts,
                },
            )?;

            row.map(Self::cell_from_row).transpose()
        })
    }

    fn latest_materialized_ts_from_table(&self, table: CellTable, id: Uuid) -> Result<Option<i64>> {
        let sql = format!(
            "WITH timeline AS (
                SELECT MAX(valid_from) AS ts FROM {table} WHERE id = :id
                UNION ALL
                SELECT MAX(valid_to) AS ts FROM {table} WHERE id = :id AND valid_to < :max_time
            )
            SELECT MAX(ts) AS latest_ts FROM timeline",
            table = table.as_str()
        );

        self.with_conn(|conn| {
            let latest: Option<i64> = conn.exec_first(
                sql,
                params! {
                    "id" => id.to_string(),
                    "max_time" => MARIADB_OPEN_ENDED_VALID_TO,
                },
            )?;
            Ok(latest)
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

impl Storage for MariaDbStorage {
    fn current_timestamp(&self) -> Result<i64> {
        self.with_conn(|conn| {
            let now: Option<i64> = conn.query_first(
                "SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 1000000 AS SIGNED)",
            )?;
            now.ok_or_else(|| {
                EngineError::Internal("failed to read MariaDB current time".to_string())
            })
        })
    }

    fn open_ended_valid_to(&self) -> i64 {
        MARIADB_OPEN_ENDED_VALID_TO
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
             SET valid_to = :now_ts
             WHERE id = :id AND valid_from <= :now_ts AND valid_to > :now_ts",
            table.as_str()
        );

        self.with_conn(|conn| {
            conn.exec_drop(
                sql,
                params! {
                    "id" => id.to_string(),
                    "now_ts" => now_ts,
                },
            )?;

            if conn.affected_rows() == 0 {
                return Err(EngineError::NotFound);
            }

            conn.exec_drop(
                "UPDATE fabric_cell
                 SET valid_to = :now_ts
                 WHERE id = :id AND valid_from <= :now_ts AND valid_to > :now_ts",
                params! {
                    "id" => id.to_string(),
                    "now_ts" => now_ts,
                },
            )?;

            Ok(())
        })
    }

    fn insert_edge(&self, edge: &FabricEdge, now_ts: i64) -> Result<()> {
        self.with_conn(|conn| {
            let relation_type = serde_json::to_string(&edge.relation_type)?;

            if self.temporal_fabric_edges {
                conn.exec_drop(
                    "UPDATE fabric_edge
                     SET valid_to = :now_ts
                     WHERE parent_id = :parent_id
                       AND relation_type = :relation_type
                       AND ordinal = :ordinal
                       AND valid_from <= :now_ts
                       AND valid_to > :now_ts",
                    params! {
                        "parent_id" => edge.parent_id.to_string(),
                        "relation_type" => relation_type.clone(),
                        "ordinal" => edge.ordinal,
                        "now_ts" => now_ts,
                    },
                )?;

                conn.exec_drop(
                    "INSERT INTO fabric_edge (parent_id, child_id, relation_type, ordinal, valid_from, valid_to)
                     VALUES (:parent_id, :child_id, :relation_type, :ordinal, :valid_from, :valid_to)",
                    params! {
                        "parent_id" => edge.parent_id.to_string(),
                        "child_id" => edge.child_id.to_string(),
                        "relation_type" => relation_type,
                        "ordinal" => edge.ordinal,
                        "valid_from" => now_ts,
                        "valid_to" => MARIADB_OPEN_ENDED_VALID_TO,
                    },
                )?;
            } else {
                conn.exec_drop(
                    "DELETE FROM fabric_edge
                     WHERE parent_id = :parent_id
                       AND relation_type = :relation_type
                       AND ordinal = :ordinal",
                    params! {
                        "parent_id" => edge.parent_id.to_string(),
                        "relation_type" => relation_type.clone(),
                        "ordinal" => edge.ordinal,
                    },
                )?;

                let zero: i64 = 0;
                conn.exec_drop(
                    "INSERT INTO fabric_edge (parent_id, child_id, relation_type, ordinal, valid_from, valid_to)
                     VALUES (:parent_id, :child_id, :relation_type, :ordinal, :valid_from, :valid_to)",
                    params! {
                        "parent_id" => edge.parent_id.to_string(),
                        "child_id" => edge.child_id.to_string(),
                        "relation_type" => relation_type,
                        "ordinal" => edge.ordinal,
                        "valid_from" => zero,
                        "valid_to" => MARIADB_OPEN_ENDED_VALID_TO,
                    },
                )?;
            }

            Ok(())
        })
    }

    fn next_relation_ordinal(&self, parent_id: Uuid, relation_type: &RelationType) -> Result<i64> {
        self.with_conn(|conn| {
            let relation = serde_json::to_string(relation_type)?;
            let ordinal: Option<i64> = conn.exec_first(
                "SELECT COALESCE(MAX(ordinal), -1) + 1
                 FROM fabric_edge
                 WHERE parent_id = :parent_id
                   AND relation_type = :relation_type
                   AND valid_to = :max_time",
                params! {
                    "parent_id" => parent_id.to_string(),
                    "relation_type" => relation,
                    "max_time" => MARIADB_OPEN_ENDED_VALID_TO,
                },
            )?;

            Ok(ordinal.unwrap_or(0))
        })
    }

    fn get_children_by_relation(
        &self,
        parent_id: Uuid,
        relation_type: &RelationType,
        ts: i64,
    ) -> Result<Vec<Uuid>> {
        self.with_conn(|conn| {
            let relation = serde_json::to_string(relation_type)?;
            let child_ids: Vec<String> = conn.exec(
                "SELECT child_id
                 FROM fabric_edge
                 WHERE parent_id = :parent_id
                   AND relation_type = :relation_type
                   AND valid_from <= :ts
                   AND valid_to > :ts
                 ORDER BY ordinal ASC",
                params! {
                    "parent_id" => parent_id.to_string(),
                    "relation_type" => relation,
                    "ts" => ts,
                },
            )?;

            let mut result = Vec::with_capacity(child_ids.len());
            for child_id_str in child_ids {
                let child_id = Uuid::parse_str(&child_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid child UUID in fabric_edge: {err}"))
                })?;
                result.push(child_id);
            }

            Ok(result)
        })
    }

    fn get_edges_for_parent(&self, parent_id: Uuid, ts: i64) -> Result<Vec<FabricEdge>> {
        self.with_conn(|conn| {
            let rows: Vec<(String, String, String, i64)> = conn.exec(
                "SELECT parent_id, child_id, relation_type, ordinal
                 FROM fabric_edge
                 WHERE parent_id = :parent_id
                   AND valid_from <= :ts
                   AND valid_to > :ts
                 ORDER BY relation_type ASC, ordinal ASC",
                params! {
                    "parent_id" => parent_id.to_string(),
                    "ts" => ts,
                },
            )?;

            let mut result = Vec::with_capacity(rows.len());
            for (parent_id_str, child_id_str, relation_type_str, ordinal) in rows {
                let parsed_parent_id = Uuid::parse_str(&parent_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid parent UUID in fabric_edge: {err}"))
                })?;
                let parsed_child_id = Uuid::parse_str(&child_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid child UUID in fabric_edge: {err}"))
                })?;
                let parsed_relation_type = serde_json::from_str(&relation_type_str)?;

                result.push(FabricEdge {
                    parent_id: parsed_parent_id,
                    child_id: parsed_child_id,
                    relation_type: parsed_relation_type,
                    ordinal,
                });
            }

            Ok(result)
        })
    }
}
