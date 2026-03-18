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
use crate::models::{Cell, CellType, ContentFormat, FabricCell, RelationType, Timestamp};
use crate::storage::time::{FUTURE_SENTINEL_STR, MIN_TIMESTAMP_STR, format_db_time, parse_db_time};

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
    const NOW_EXPR: &'static str = "CURRENT_TIMESTAMP(6)";

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

    fn insert_cell_in_table(
        &self,
        table: CellTable,
        id: Uuid,
        cell_type: &CellType,
        format: &ContentFormat,
        content: &[u8],
        fabric_id: Option<Uuid>,
    ) -> Result<Cell> {
        let cell_type = serde_json::to_string(cell_type)?;
        let format = serde_json::to_string(format)?;
        let sql = format!(
            "INSERT INTO {} (id, cell_type, format, content, fabric_id)
             VALUES (:id, :cell_type, :format, :content, :fabric_id)",
            table.as_str()
        );

        self.with_conn(|conn| {
            conn.exec_drop(
                sql,
                params! {
                    "id" => id.to_string(),
                    "cell_type" => cell_type,
                    "format" => format,
                    "content" => content.to_vec(),
                    "fabric_id" => fabric_id.map(|value| value.to_string()),
                },
            )?;

            if fabric_id.is_some() {
                conn.exec_drop(
                    "INSERT INTO fabric_cell (id) VALUES (:id)",
                    params! {
                        "id" => id.to_string(),
                    },
                )?;
            }

            self.get_cell(id)
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
        ): (
            String,
            String,
            String,
            Vec<u8>,
            String,
            String,
            Option<String>,
        ) = mysql::from_row_opt(row)
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

    fn get_current_cell_from_table(&self, table: CellTable, id: Uuid) -> Result<Option<Cell>> {
        let sql = format!(
            "SELECT id,
                    cell_type,
                    format,
                    content,
                    DATE_FORMAT(valid_from, '%Y-%m-%d %H:%i:%s.%f') AS valid_from,
                    DATE_FORMAT(valid_to, '%Y-%m-%d %H:%i:%s.%f') AS valid_to,
                    fabric_id
             FROM {}
             WHERE id = :id AND valid_from <= {} AND valid_to > {}
             ORDER BY valid_from DESC
             LIMIT 1",
            table.as_str(),
            Self::NOW_EXPR,
            Self::NOW_EXPR
        );

        self.with_conn(|conn| {
            let row = conn.exec_first(sql, params! { "id" => id.to_string() })?;
            row.map(Self::cell_from_row).transpose()
        })
    }

    fn get_cell_history_from_table(&self, table: CellTable, id: Uuid) -> Result<Vec<Cell>> {
        let sql = format!(
            "SELECT id,
                    cell_type,
                    format,
                    content,
                    DATE_FORMAT(valid_from, '%Y-%m-%d %H:%i:%s.%f') AS valid_from,
                    DATE_FORMAT(valid_to, '%Y-%m-%d %H:%i:%s.%f') AS valid_to,
                    fabric_id
             FROM {}
             WHERE id = :id
             ORDER BY valid_from ASC, valid_to ASC",
            table.as_str()
        );
        self.with_conn(|conn| {
            let rows: Vec<Row> = conn.exec(sql, params! { "id" => id.to_string() })?;
            rows.into_iter().map(Self::cell_from_row).collect()
        })
    }

    fn get_cell(&self, id: Uuid) -> Result<Cell> {
        if let Some(cell) = self.get_current_cell_from_table(CellTable::Data, id)? {
            return Ok(cell);
        }
        if let Some(cell) = self.get_current_cell_from_table(CellTable::Meta, id)? {
            return Ok(cell);
        }
        Err(EngineError::NotFound)
    }

    fn active_table_for_id(&self, id: Uuid) -> Result<Option<CellTable>> {
        if self
            .get_current_cell_from_table(CellTable::Data, id)?
            .is_some()
        {
            return Ok(Some(CellTable::Data));
        }
        if self
            .get_current_cell_from_table(CellTable::Meta, id)?
            .is_some()
        {
            return Ok(Some(CellTable::Meta));
        }
        Ok(None)
    }

    fn close_active_version(&self, id: Uuid) -> Result<()> {
        let Some(table) = self.active_table_for_id(id)? else {
            return Err(EngineError::NotFound);
        };

        let sql = format!(
            "UPDATE {}
             SET valid_to = {}
             WHERE id = :id AND valid_from <= {} AND valid_to > {}",
            table.as_str(),
            Self::NOW_EXPR,
            Self::NOW_EXPR,
            Self::NOW_EXPR
        );

        self.with_conn(|conn| {
            conn.exec_drop(sql, params! { "id" => id.to_string() })?;
            if conn.affected_rows() == 0 {
                return Err(EngineError::NotFound);
            }
            conn.exec_drop(
                &format!(
                    "UPDATE fabric_cell
                     SET valid_to = {}
                     WHERE id = :id AND valid_from <= {} AND valid_to > {}",
                    Self::NOW_EXPR,
                    Self::NOW_EXPR,
                    Self::NOW_EXPR
                ),
                params! { "id" => id.to_string() },
            )?;
            Ok(())
        })
    }
}

impl Storage for MySqlStorage {
    fn insert_cell(
        &self,
        id: Uuid,
        cell_type: &CellType,
        format: &ContentFormat,
        content: &[u8],
        fabric_id: Option<Uuid>,
    ) -> Result<Cell> {
        self.insert_cell_in_table(
            Self::table_for_cell_type(cell_type),
            id,
            cell_type,
            format,
            content,
            fabric_id,
        )
    }

    fn replace_cell(
        &self,
        id: Uuid,
        cell_type: &CellType,
        format: &ContentFormat,
        content: &[u8],
        fabric_id: Option<Uuid>,
    ) -> Result<Cell> {
        self.close_active_version(id)?;
        self.insert_cell(id, cell_type, format, content, fabric_id)
    }

    fn delete_cell(&self, id: Uuid) -> Result<()> {
        self.close_active_version(id)
    }

    fn get_cell(&self, id: Uuid) -> Result<Cell> {
        MySqlStorage::get_cell(self, id)
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

    fn get_cell_history(&self, id: Uuid) -> Result<Vec<Cell>> {
        let mut rows = self.get_cell_history_from_table(CellTable::Data, id)?;
        rows.extend(self.get_cell_history_from_table(CellTable::Meta, id)?);
        rows.sort_by(|a, b| {
            a.valid_from
                .cmp(&b.valid_from)
                .then(a.valid_to.cmp(&b.valid_to))
        });
        Ok(rows)
    }

    fn insert_fabric_cell(&self, fabric_cell: &FabricCell) -> Result<()> {
        self.with_conn(|conn| {
            let relation_type = serde_json::to_string(&fabric_cell.relation_type)?;

            if self.temporal_fabric_cells {
                conn.exec_drop(
                    &format!(
                        "UPDATE fabric_cells
                     SET valid_to = {}
                     WHERE fabric_id = :fabric_id
                       AND relation_type = :relation_type
                       AND ordinal = :ordinal
                       AND valid_from <= {}
                       AND valid_to > {}",
                        Self::NOW_EXPR,
                        Self::NOW_EXPR,
                        Self::NOW_EXPR
                    ),
                    params! {
                        "fabric_id" => fabric_cell.fabric_id.to_string(),
                        "relation_type" => relation_type.clone(),
                        "ordinal" => fabric_cell.ordinal,
                    },
                )?;

                conn.exec_drop(
                    "INSERT INTO fabric_cells (fabric_id, cell_id, relation_type, ordinal)
                     VALUES (:fabric_id, :cell_id, :relation_type, :ordinal)",
                    params! {
                        "fabric_id" => fabric_cell.fabric_id.to_string(),
                        "cell_id" => fabric_cell.cell_id.to_string(),
                        "relation_type" => relation_type,
                        "ordinal" => fabric_cell.ordinal,
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

    fn next_relation_ordinal(&self, fabric_id: Uuid, relation_type: &RelationType) -> Result<i64> {
        self.with_conn(|conn| {
            let relation = serde_json::to_string(relation_type)?;
            let ordinal: Option<i64> = conn.exec_first(
                &format!(
                    "SELECT COALESCE(MAX(ordinal), -1) + 1
                     FROM fabric_cells
                     WHERE fabric_id = :fabric_id
                       AND relation_type = :relation_type
                       AND valid_from <= {}
                       AND valid_to > {}",
                    Self::NOW_EXPR,
                    Self::NOW_EXPR
                ),
                params! {
                    "fabric_id" => fabric_id.to_string(),
                    "relation_type" => relation,
                },
            )?;

            Ok(ordinal.unwrap_or(0))
        })
    }

    fn next_relation_ordinal_at(
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
    ) -> Result<Vec<Uuid>> {
        self.with_conn(|conn| {
            let relation = serde_json::to_string(relation_type)?;
            let cell_ids: Vec<String> = conn.exec(
                &format!(
                    "SELECT cell_id
                     FROM fabric_cells
                     WHERE fabric_id = :fabric_id
                       AND relation_type = :relation_type
                       AND valid_from <= {}
                       AND valid_to > {}
                     ORDER BY ordinal ASC",
                    Self::NOW_EXPR,
                    Self::NOW_EXPR
                ),
                params! {
                    "fabric_id" => fabric_id.to_string(),
                    "relation_type" => relation,
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

    fn get_cells_by_relation_at(
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

    fn get_fabric_cells(&self, fabric_id: Uuid) -> Result<Vec<FabricCell>> {
        self.with_conn(|conn| {
            let rows: Vec<(String, String, String, i64)> = conn.exec(
                &format!(
                    "SELECT fabric_id, cell_id, relation_type, ordinal
                     FROM fabric_cells
                     WHERE fabric_id = :fabric_id
                       AND valid_from <= {}
                       AND valid_to > {}
                     ORDER BY relation_type ASC, ordinal ASC",
                    Self::NOW_EXPR,
                    Self::NOW_EXPR
                ),
                params! {
                    "fabric_id" => fabric_id.to_string(),
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

    fn get_fabric_cells_at(&self, fabric_id: Uuid, ts: Timestamp) -> Result<Vec<FabricCell>> {
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
