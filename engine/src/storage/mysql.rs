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
use crate::models::{Cell, CellType, FabricCell, RelationType, Timestamp};
use crate::storage::time::{FUTURE_SENTINEL_STR, MIN_TIMESTAMP_STR, format_db_time, parse_db_time, future_sentinel};

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

pub struct MySqlStorage {
    conn: Mutex<Conn>,
    temporal_fabric_cells: bool,
}

impl MySqlStorage {
    pub fn new(connection_str: &str, temporal_fabric_cells: bool) -> Result<Self> {
        let opts = Opts::from_url(connection_str)
            .map_err(|err| EngineError::InvalidData(format!("invalid MySQL URL: {err}")))?;
        let conn = Conn::new(opts)?;
        let storage = Self {
            conn: Mutex::new(conn),
            temporal_fabric_cells,
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
            .map_err(|_| EngineError::Internal("mysql connection mutex poisoned".to_string()))?;
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
                    valid_from DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                    valid_to DATETIME(6) NOT NULL DEFAULT '2100-01-01 00:00:00.000000',
                    fabric_id TEXT NULL,
                    PRIMARY KEY (id, valid_to)
                )",
            )?;

            conn.query_drop(
                "CREATE TABLE IF NOT EXISTS meta_cell (
                    id VARCHAR(36) NOT NULL,
                    cell_type TEXT NOT NULL,
                    format TEXT NOT NULL,
                    content BLOB NOT NULL,
                    valid_from DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                    valid_to DATETIME(6) NOT NULL DEFAULT '2100-01-01 00:00:00.000000',
                    fabric_id TEXT NULL,
                    PRIMARY KEY (id, valid_to)
                )",
            )?;

            conn.query_drop(
                "CREATE TABLE IF NOT EXISTS fabric_cell (
                    id VARCHAR(36) NOT NULL,
                    valid_from DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                    valid_to DATETIME(6) NOT NULL DEFAULT '2100-01-01 00:00:00.000000',
                    PRIMARY KEY (id, valid_to)
                )",
            )?;

            conn.query_drop(
                "CREATE TABLE IF NOT EXISTS fabric_cells (
                    fabric_id VARCHAR(36) NOT NULL,
                    cell_id VARCHAR(36) NOT NULL,
                    relation_type VARCHAR(255) NOT NULL,
                    ordinal BIGINT NOT NULL,
                    valid_from DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                    valid_to DATETIME(6) NOT NULL DEFAULT '2100-01-01 00:00:00.000000',
                    PRIMARY KEY (fabric_id, relation_type, ordinal, valid_to)
                )",
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
        let cell_type = serde_json::to_string(&cell.cell_type)?;
        let format = serde_json::to_string(&cell.format)?;
        let sql = format!(
            "INSERT INTO {} (id, cell_type, format, content, valid_from, valid_to, fabric_id)
             VALUES (:id, :cell_type, :format, :content, :valid_from, :valid_to, :fabric_id)",
            table.as_str()
        );

        self.with_conn(|conn| {
            conn.exec_drop(
                sql,
                params! {
                    "id" => cell.id.to_string(),
                    "cell_type" => cell_type,
                    "format" => format,
                    "content" => cell.content.clone(),
                    "valid_from" => format_db_time(&cell.valid_from),
                    "valid_to" => format_db_time(&cell.valid_to),
                    "fabric_id" => cell.fabric_id.map(|value| value.to_string()),
                },
            )?;

            if cell.fabric_id.is_some() {
                conn.exec_drop(
                    "INSERT INTO fabric_cell (id, valid_from, valid_to)
                     VALUES (:id, :valid_from, :valid_to)
                     ON DUPLICATE KEY UPDATE valid_from = VALUES(valid_from)",
                    params! {
                        "id" => cell.id.to_string(),
                        "valid_from" => format_db_time(&cell.valid_from),
                        "valid_to" => format_db_time(&cell.valid_to),
                    },
                )?;
            }

            Ok(())
        })
    }

    fn cell_from_row(row: Row) -> Result<Cell> {
        let (
            id_str,
            cell_type_str,
            format_str,
            content,
            valid_from_str,
            valid_to_str,
            fabric_id_str,
        ): (String, String, String, Vec<u8>, String, String, Option<String>) =
            mysql::from_row_opt(row)
                .map_err(|err| EngineError::InvalidData(format!("invalid cell row shape: {err}")))?;

        let id = Uuid::parse_str(&id_str)
            .map_err(|err| EngineError::InvalidData(format!("invalid UUID in row: {err}")))?;
        let cell_type = serde_json::from_str(&cell_type_str)?;
        let format = serde_json::from_str(&format_str)?;
        let fabric_id = fabric_id_str
            .map(|value| Uuid::parse_str(&value))
            .transpose()
            .map_err(|err| {
                EngineError::InvalidData(format!("invalid fabric UUID in row: {err}"))
            })?;

        let valid_from = parse_db_time(&valid_from_str)?;
        let valid_to = parse_db_time(&valid_to_str)?;

        Ok(Cell {
            id,
            cell_type,
            format,
            content,
            valid_from,
            valid_to,
            fabric_id,
        })
    }

    fn get_cell_at_from_table(
        &self,
        table: CellTable,
        id: Uuid,
        ts: Timestamp,
    ) -> Result<Option<Cell>> {
        let sql = format!(
            "SELECT id,
                    cell_type,
                    format,
                    content,
                    DATE_FORMAT(valid_from, '%Y-%m-%d %H:%i:%s.%f') AS valid_from,
                    DATE_FORMAT(valid_to, '%Y-%m-%d %H:%i:%s.%f') AS valid_to,
                    fabric_id
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
                    "ts" => format_db_time(&ts),
                },
            )?;

            row.map(Self::cell_from_row).transpose()
        })
    }

    fn latest_materialized_ts_from_table(
        &self,
        table: CellTable,
        id: Uuid,
    ) -> Result<Option<Timestamp>> {
        let sql = format!(
            "WITH timeline AS (
                SELECT MAX(valid_from) AS ts FROM {table} WHERE id = :id
                UNION ALL
                SELECT MAX(valid_to) AS ts FROM {table} WHERE id = :id AND valid_to < :max_time
            )
            SELECT DATE_FORMAT(MAX(ts), '%Y-%m-%d %H:%i:%s.%f') AS latest_ts FROM timeline",
            table = table.as_str()
        );

        self.with_conn(|conn| {
            let latest: Option<String> = conn.exec_first(
                sql,
                params! {
                    "id" => id.to_string(),
                    "max_time" => FUTURE_SENTINEL_STR,
                },
            )?;
            latest.map(|value| parse_db_time(&value)).transpose()
        })
    }

    fn active_table_for_id(&self, id: Uuid, ts: Timestamp) -> Result<Option<CellTable>> {
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

impl Storage for MySqlStorage {
    fn current_query_timestamp(&self) -> Result<Timestamp> {
        self.with_conn(|conn| {
            let now: Option<String> = conn.query_first(
                "SELECT DATE_FORMAT(CURRENT_TIMESTAMP(6), '%Y-%m-%d %H:%i:%s.%f')",
            )?;
            let value = now.ok_or_else(|| {
                EngineError::Internal("failed to read MySQL current time".to_string())
            })?;
            parse_db_time(&value)
        })
    }

    fn active_valid_to_sentinel(&self) -> Timestamp {
        future_sentinel()
    }

    fn insert_cell(&self, cell: &Cell) -> Result<()> {
        self.insert_cell_in_table(Self::table_for_cell_type(&cell.cell_type), cell)
    }

    fn reserve_next_version_timestamp(
        &self,
        id: Uuid,
        candidate_ts: Timestamp,
    ) -> Result<Timestamp> {
        let data_latest = self.latest_materialized_ts_from_table(CellTable::Data, id)?;
        let meta_latest = self.latest_materialized_ts_from_table(CellTable::Meta, id)?;
        let latest = match (data_latest, meta_latest) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        let next = match latest {
            Some(prev) if candidate_ts <= prev => prev + chrono::Duration::microseconds(1),
            _ => candidate_ts,
        };

        Ok(next)
    }

    fn get_cell_at(&self, id: Uuid, ts: Timestamp) -> Result<Cell> {
        if let Some(cell) = self.get_cell_at_from_table(CellTable::Data, id, ts)? {
            return Ok(cell);
        }

        if let Some(cell) = self.get_cell_at_from_table(CellTable::Meta, id, ts)? {
            return Ok(cell);
        }

        Err(EngineError::NotFound)
    }

    fn close_active_version(&self, id: Uuid, now_ts: Timestamp) -> Result<()> {
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
                    "now_ts" => format_db_time(&now_ts),
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
                    "now_ts" => format_db_time(&now_ts),
                },
            )?;

            Ok(())
        })
    }

    fn insert_fabric_cell(&self, fabric_cell: &FabricCell, now_ts: Timestamp) -> Result<()> {
        self.with_conn(|conn| {
            let relation_type = serde_json::to_string(&fabric_cell.relation_type)?;
            let now_str = format_db_time(&now_ts);

            if self.temporal_fabric_cells {
                conn.exec_drop(
                    "UPDATE fabric_cells
                     SET valid_to = :now_ts
                     WHERE fabric_id = :fabric_id
                       AND relation_type = :relation_type
                       AND ordinal = :ordinal
                       AND valid_from <= :now_ts
                       AND valid_to > :now_ts",
                    params! {
                        "fabric_id" => fabric_cell.fabric_id.to_string(),
                        "relation_type" => relation_type.clone(),
                        "ordinal" => fabric_cell.ordinal,
                        "now_ts" => now_str,
                    },
                )?;

                conn.exec_drop(
                    "INSERT INTO fabric_cells (fabric_id, cell_id, relation_type, ordinal, valid_from, valid_to)
                     VALUES (:fabric_id, :cell_id, :relation_type, :ordinal, :valid_from, :valid_to)",
                    params! {
                        "fabric_id" => fabric_cell.fabric_id.to_string(),
                        "cell_id" => fabric_cell.cell_id.to_string(),
                        "relation_type" => relation_type,
                        "ordinal" => fabric_cell.ordinal,
                        "valid_from" => now_str,
                        "valid_to" => FUTURE_SENTINEL_STR,
                    },
                )?;
            } else {
                conn.exec_drop(
                    "DELETE FROM fabric_cells
                     WHERE fabric_id = :fabric_id
                       AND relation_type = :relation_type
                       AND ordinal = :ordinal",
                    params! {
                        "fabric_id" => fabric_cell.fabric_id.to_string(),
                        "relation_type" => relation_type.clone(),
                        "ordinal" => fabric_cell.ordinal,
                    },
                )?;

                conn.exec_drop(
                    "INSERT INTO fabric_cells (fabric_id, cell_id, relation_type, ordinal, valid_from, valid_to)
                     VALUES (:fabric_id, :cell_id, :relation_type, :ordinal, :valid_from, :valid_to)",
                    params! {
                        "fabric_id" => fabric_cell.fabric_id.to_string(),
                        "cell_id" => fabric_cell.cell_id.to_string(),
                        "relation_type" => relation_type,
                        "ordinal" => fabric_cell.ordinal,
                        "valid_from" => MIN_TIMESTAMP_STR,
                        "valid_to" => FUTURE_SENTINEL_STR,
                    },
                )?;
            }

            Ok(())
        })
    }

    fn next_relation_ordinal(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
        ts: Timestamp,
    ) -> Result<i64> {
        self.with_conn(|conn| {
            let relation = serde_json::to_string(relation_type)?;
            let ordinal: Option<i64> = conn.exec_first(
                "SELECT COALESCE(MAX(ordinal), -1) + 1
                 FROM fabric_cells
                 WHERE fabric_id = :fabric_id
                   AND relation_type = :relation_type
                   AND valid_from <= :ts
                   AND valid_to > :ts",
                params! {
                    "fabric_id" => fabric_id.to_string(),
                    "relation_type" => relation,
                    "ts" => format_db_time(&ts),
                },
            )?;

            Ok(ordinal.unwrap_or(0))
        })
    }

    fn get_cells_by_relation(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
        ts: Timestamp,
    ) -> Result<Vec<Uuid>> {
        self.with_conn(|conn| {
            let relation = serde_json::to_string(relation_type)?;
            let cell_ids: Vec<String> = conn.exec(
                "SELECT cell_id
                 FROM fabric_cells
                 WHERE fabric_id = :fabric_id
                   AND relation_type = :relation_type
                   AND valid_from <= :ts
                   AND valid_to > :ts
                 ORDER BY ordinal ASC",
                params! {
                    "fabric_id" => fabric_id.to_string(),
                    "relation_type" => relation,
                    "ts" => format_db_time(&ts),
                },
            )?;

            let mut result = Vec::with_capacity(cell_ids.len());
            for cell_id_str in cell_ids {
                let cell_id = Uuid::parse_str(&cell_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid cell UUID in fabric_cells: {err}"))
                })?;
                result.push(cell_id);
            }

            Ok(result)
        })
    }

    fn get_fabric_cells(&self, fabric_id: Uuid, ts: Timestamp) -> Result<Vec<FabricCell>> {
        self.with_conn(|conn| {
            let rows: Vec<(String, String, String, i64)> = conn.exec(
                "SELECT fabric_id, cell_id, relation_type, ordinal
                 FROM fabric_cells
                 WHERE fabric_id = :fabric_id
                   AND valid_from <= :ts
                   AND valid_to > :ts
                 ORDER BY relation_type ASC, ordinal ASC",
                params! {
                    "fabric_id" => fabric_id.to_string(),
                    "ts" => format_db_time(&ts),
                },
            )?;

            let mut result = Vec::with_capacity(rows.len());
            for (fabric_id_str, cell_id_str, relation_type_str, ordinal) in rows {
                let parsed_fabric_id = Uuid::parse_str(&fabric_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid fabric UUID in fabric_cells: {err}"))
                })?;
                let parsed_cell_id = Uuid::parse_str(&cell_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid cell UUID in fabric_cells: {err}"))
                })?;
                let parsed_relation_type = serde_json::from_str(&relation_type_str)?;

                result.push(FabricCell {
                    fabric_id: parsed_fabric_id,
                    cell_id: parsed_cell_id,
                    relation_type: parsed_relation_type,
                    ordinal,
                });
            }

            Ok(result)
        })
    }
}
