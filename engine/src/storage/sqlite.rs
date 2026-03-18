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

use rusqlite::{Connection, OptionalExtension, params};
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

pub struct SqliteStorage {
    conn: Mutex<Connection>,
    temporal_fabric_cells: bool,
}

impl SqliteStorage {
    const SQLITE_NOW: &'static str = "STRFTIME('%Y-%m-%d %H:%M:%f', 'now')";

    pub fn new(path: &str, temporal_fabric_cells: bool) -> Result<Self> {
        let conn = Connection::open(path)?;
        let storage = Self {
            conn: Mutex::new(conn),
            temporal_fabric_cells,
        };
        storage.init()?;
        Ok(storage)
    }

    fn with_conn<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        let guard = self
            .conn
            .lock()
            .map_err(|_| EngineError::Internal("sqlite connection mutex poisoned".to_string()))?;
        f(&guard)
    }

    fn init(&self) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS data_cell (
                id TEXT NOT NULL,
                cell_type TEXT NOT NULL,
                format TEXT NOT NULL,
                content BLOB NOT NULL,
                valid_from TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'now')),
                valid_to TEXT NOT NULL DEFAULT '2100-01-01 00:00:00.000000',
                fabric_id TEXT,
                PRIMARY KEY (id, valid_to)
            )",
                params![],
            )?;

            conn.execute(
                "CREATE TABLE IF NOT EXISTS meta_cell (
                id TEXT NOT NULL,
                cell_type TEXT NOT NULL,
                format TEXT NOT NULL,
                content BLOB NOT NULL,
                valid_from TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'now')),
                valid_to TEXT NOT NULL DEFAULT '2100-01-01 00:00:00.000000',
                fabric_id TEXT,
                PRIMARY KEY (id, valid_to)
            )",
                params![],
            )?;

            conn.execute(
                "CREATE TABLE IF NOT EXISTS fabric_cell (
                id TEXT NOT NULL,
                valid_from TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'now')),
                valid_to TEXT NOT NULL DEFAULT '2100-01-01 00:00:00.000000',
                PRIMARY KEY (id, valid_to)
            )",
                params![],
            )?;

            conn.execute(
                "CREATE TABLE IF NOT EXISTS fabric_cells (
                fabric_id TEXT NOT NULL,
                cell_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                valid_from TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'now')),
                valid_to TEXT NOT NULL DEFAULT '2100-01-01 00:00:00.000000',
                PRIMARY KEY (fabric_id, relation_type, ordinal, valid_to)
            )",
                params![],
            )?;

            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS data_cell_one_active_per_id
             ON data_cell(id)
             WHERE valid_to = '2100-01-01 00:00:00.000000'",
                params![],
            )?;

            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS meta_cell_one_active_per_id
             ON meta_cell(id)
             WHERE valid_to = '2100-01-01 00:00:00.000000'",
                params![],
            )?;

            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS fabric_cells_one_active_per_slot
             ON fabric_cells(fabric_id, relation_type, ordinal)
             WHERE valid_to = '2100-01-01 00:00:00.000000'",
                params![],
            )?;

            Ok(())
        })
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
        let sql = format!(
            "INSERT INTO {} (id, cell_type, format, content, fabric_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            table.as_str()
        );

        self.with_conn(|conn| {
            conn.execute(
                &sql,
                params![
                    id.to_string(),
                    serde_json::to_string(cell_type)?,
                    serde_json::to_string(format)?,
                    content,
                    fabric_id.map(|value| value.to_string()),
                ],
            )?;

            if fabric_id.is_some() {
                conn.execute(
                    "INSERT INTO fabric_cell (id) VALUES (?1)",
                    params![id.to_string()],
                )?;
            }

            Self::get_current_cell_from_table(conn, table, id)?.ok_or(EngineError::NotFound)
        })
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
        let valid_from_str: String = row.get(4)?;
        let valid_to_str: String = row.get(5)?;
        let fabric_id_str: Option<String> = row.get(6)?;

        let id = Uuid::parse_str(&id_str).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
        })?;

        let cell_type = serde_json::from_str(&cell_type_str).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(err))
        })?;

        let format = serde_json::from_str(&format_str).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(err))
        })?;

        let fabric_id = fabric_id_str
            .map(|value| {
                Uuid::parse_str(&value).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        6,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })
            })
            .transpose()?;

        let valid_from = parse_db_time(&valid_from_str).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(err))
        })?;
        let valid_to = parse_db_time(&valid_to_str).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(err))
        })?;

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
            "SELECT id, cell_type, format, content, valid_from, valid_to, fabric_id
             FROM {}
             WHERE id = ?1 AND valid_from <= ?2 AND valid_to > ?2
             ORDER BY valid_from DESC
             LIMIT 1",
            table.as_str()
        );

        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&sql)?;
            let cell = stmt
                .query_row(
                    params![id.to_string(), format_db_time(&ts)],
                    Self::cell_from_row,
                )
                .optional()?;

            Ok(cell)
        })
    }

    fn get_current_cell_from_table(
        conn: &Connection,
        table: CellTable,
        id: Uuid,
    ) -> Result<Option<Cell>> {
        let sql = format!(
            "SELECT id, cell_type, format, content, valid_from, valid_to, fabric_id
             FROM {}
             WHERE id = ?1 AND valid_from <= {} AND valid_to > {}
             ORDER BY valid_from DESC
             LIMIT 1",
            table.as_str(),
            Self::SQLITE_NOW,
            Self::SQLITE_NOW
        );

        let mut stmt = conn.prepare(&sql)?;
        let cell = stmt
            .query_row(params![id.to_string()], Self::cell_from_row)
            .optional()?;

        Ok(cell)
    }

    fn current_db_time(conn: &Connection) -> Result<String> {
        let now: String = conn.query_row(
            "SELECT STRFTIME('%Y-%m-%d %H:%M:%f', 'now')",
            params![],
            |row| row.get(0),
        )?;
        Ok(now)
    }

    fn get_cell_history_from_table(&self, table: CellTable, id: Uuid) -> Result<Vec<Cell>> {
        let sql = format!(
            "SELECT id, cell_type, format, content, valid_from, valid_to, fabric_id
             FROM {}
             WHERE id = ?1
             ORDER BY valid_from ASC, valid_to ASC",
            table.as_str()
        );

        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&sql)?;
            let mapped = stmt.query_map(params![id.to_string()], Self::cell_from_row)?;
            let mut result = Vec::new();
            for row in mapped {
                result.push(row?);
            }
            Ok(result)
        })
    }

    fn get_cell(&self, id: Uuid) -> Result<Cell> {
        self.with_conn(|conn| {
            if let Some(cell) = Self::get_current_cell_from_table(conn, CellTable::Data, id)? {
                return Ok(cell);
            }
            if let Some(cell) = Self::get_current_cell_from_table(conn, CellTable::Meta, id)? {
                return Ok(cell);
            }
            Err(EngineError::NotFound)
        })
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

    fn active_table_for_id(&self, id: Uuid) -> Result<Option<CellTable>> {
        self.with_conn(|conn| {
            if Self::get_current_cell_from_table(conn, CellTable::Data, id)?.is_some() {
                return Ok(Some(CellTable::Data));
            }
            if Self::get_current_cell_from_table(conn, CellTable::Meta, id)?.is_some() {
                return Ok(Some(CellTable::Meta));
            }
            Ok(None)
        })
    }

    fn close_active_version(&self, id: Uuid) -> Result<()> {
        let Some(table) = self.active_table_for_id(id)? else {
            return Err(EngineError::NotFound);
        };

        let sql = format!(
            "UPDATE {}
             SET valid_to = {}
             WHERE id = ?1 AND valid_from <= {} AND valid_to > {}",
            table.as_str(),
            Self::SQLITE_NOW,
            Self::SQLITE_NOW,
            Self::SQLITE_NOW
        );

        self.with_conn(|conn| {
            let changed = conn.execute(&sql, params![id.to_string()])?;
            if changed == 0 {
                return Err(EngineError::NotFound);
            }

            conn.execute(
                &format!(
                    "UPDATE fabric_cell
                     SET valid_to = {}
                     WHERE id = ?1 AND valid_from <= {} AND valid_to > {}",
                    Self::SQLITE_NOW,
                    Self::SQLITE_NOW,
                    Self::SQLITE_NOW
                ),
                params![id.to_string()],
            )?;

            Ok(())
        })
    }

    fn replace_cell_in_table(
        &self,
        table: CellTable,
        id: Uuid,
        cell_type: &CellType,
        format: &ContentFormat,
        content: &[u8],
        fabric_id: Option<Uuid>,
    ) -> Result<Cell> {
        self.with_conn(|conn| {
            let now = Self::current_db_time(conn)?;
            let close_sql = format!(
                "UPDATE {}
                 SET valid_to = ?2
                 WHERE id = ?1 AND valid_from <= ?2 AND valid_to > ?2",
                table.as_str()
            );
            let changed = conn.execute(&close_sql, params![id.to_string(), now.clone()])?;
            if changed == 0 {
                return Err(EngineError::NotFound);
            }

            conn.execute(
                "UPDATE fabric_cell
                 SET valid_to = ?2
                 WHERE id = ?1 AND valid_from <= ?2 AND valid_to > ?2",
                params![id.to_string(), now.clone()],
            )?;

            let insert_sql = format!(
                "INSERT INTO {} (id, cell_type, format, content, valid_from, fabric_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                table.as_str()
            );
            conn.execute(
                &insert_sql,
                params![
                    id.to_string(),
                    serde_json::to_string(cell_type)?,
                    serde_json::to_string(format)?,
                    content,
                    now.clone(),
                    fabric_id.map(|value| value.to_string()),
                ],
            )?;

            if fabric_id.is_some() {
                conn.execute(
                    "INSERT INTO fabric_cell (id, valid_from) VALUES (?1, ?2)",
                    params![id.to_string(), now],
                )?;
            }

            Self::get_current_cell_from_table(conn, table, id)?.ok_or(EngineError::NotFound)
        })
    }

    fn insert_fabric_cell(&self, fabric_cell: &FabricCell) -> Result<()> {
        self.with_conn(|conn| {
            if self.temporal_fabric_cells {
                conn.execute(
                    &format!(
                        "UPDATE fabric_cells
                 SET valid_to = {}
                 WHERE fabric_id = ?1
                   AND relation_type = ?2
                   AND ordinal = ?3
                   AND valid_from <= {}
                   AND valid_to > {}",
                        Self::SQLITE_NOW,
                        Self::SQLITE_NOW,
                        Self::SQLITE_NOW
                    ),
                    params![
                        fabric_cell.fabric_id.to_string(),
                        serde_json::to_string(&fabric_cell.relation_type)?,
                        fabric_cell.ordinal,
                    ],
                )?;

                conn.execute(
                    "INSERT INTO fabric_cells (fabric_id, cell_id, relation_type, ordinal)
                 VALUES (?1, ?2, ?3, ?4)",
                    params![
                        fabric_cell.fabric_id.to_string(),
                        fabric_cell.cell_id.to_string(),
                        serde_json::to_string(&fabric_cell.relation_type)?,
                        fabric_cell.ordinal,
                    ],
                )?;
            } else {
                conn.execute(
                    "DELETE FROM fabric_cells
                 WHERE fabric_id = ?1
                   AND relation_type = ?2
                   AND ordinal = ?3",
                    params![
                        fabric_cell.fabric_id.to_string(),
                        serde_json::to_string(&fabric_cell.relation_type)?,
                        fabric_cell.ordinal,
                    ],
                )?;

                conn.execute(
                    "INSERT INTO fabric_cells (fabric_id, cell_id, relation_type, ordinal, valid_from, valid_to)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        fabric_cell.fabric_id.to_string(),
                        fabric_cell.cell_id.to_string(),
                        serde_json::to_string(&fabric_cell.relation_type)?,
                        fabric_cell.ordinal,
                        MIN_TIMESTAMP_STR,
                        FUTURE_SENTINEL_STR,
                    ],
                )?;
            }

            Ok(())
        })
    }

    fn next_relation_ordinal(&self, fabric_id: Uuid, relation_type: &RelationType) -> Result<i64> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT COALESCE(MAX(ordinal), -1) + 1
                 FROM fabric_cells
                 WHERE fabric_id = ?1
                   AND relation_type = ?2
                   AND valid_from <= {}
                   AND valid_to > {}",
                Self::SQLITE_NOW,
                Self::SQLITE_NOW
            ))?;

            let ordinal: i64 = stmt.query_row(
                params![fabric_id.to_string(), serde_json::to_string(relation_type)?],
                |row| row.get(0),
            )?;

            Ok(ordinal)
        })
    }

    fn next_relation_ordinal_at(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
        ts: Timestamp,
    ) -> Result<i64> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT COALESCE(MAX(ordinal), -1) + 1
             FROM fabric_cells
             WHERE fabric_id = ?1
               AND relation_type = ?2
               AND valid_from <= ?3
               AND valid_to > ?3",
            )?;

            let ordinal: i64 = stmt.query_row(
                params![
                    fabric_id.to_string(),
                    serde_json::to_string(relation_type)?,
                    format_db_time(&ts)
                ],
                |row| row.get(0),
            )?;

            Ok(ordinal)
        })
    }

    fn get_cells_by_relation(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
    ) -> Result<Vec<Uuid>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT cell_id
                 FROM fabric_cells
                 WHERE fabric_id = ?1
                   AND relation_type = ?2
                   AND valid_from <= {}
                   AND valid_to > {}
                 ORDER BY ordinal ASC",
                Self::SQLITE_NOW,
                Self::SQLITE_NOW
            ))?;

            let mapped = stmt.query_map(
                params![fabric_id.to_string(), serde_json::to_string(relation_type)?],
                |row| {
                    let cell_id_str: String = row.get(0)?;
                    Uuid::parse_str(&cell_id_str).map_err(|err| {
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
        })
    }

    fn get_cells_by_relation_at(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
        ts: Timestamp,
    ) -> Result<Vec<Uuid>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT cell_id
             FROM fabric_cells
             WHERE fabric_id = ?1
               AND relation_type = ?2
               AND valid_from <= ?3
               AND valid_to > ?3
             ORDER BY ordinal ASC",
            )?;

            let mapped = stmt.query_map(
                params![
                    fabric_id.to_string(),
                    serde_json::to_string(relation_type)?,
                    format_db_time(&ts),
                ],
                |row| {
                    let cell_id_str: String = row.get(0)?;
                    Uuid::parse_str(&cell_id_str).map_err(|err| {
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
        })
    }

    fn get_fabric_cells(&self, fabric_id: Uuid) -> Result<Vec<FabricCell>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT fabric_id, cell_id, relation_type, ordinal
                 FROM fabric_cells
                 WHERE fabric_id = ?1
                   AND valid_from <= {}
                   AND valid_to > {}
                 ORDER BY relation_type ASC, ordinal ASC",
                Self::SQLITE_NOW,
                Self::SQLITE_NOW
            ))?;

            let mapped = stmt.query_map(params![fabric_id.to_string()], |row| {
                let fabric_id_str: String = row.get(0)?;
                let cell_id_str: String = row.get(1)?;
                let relation_type_str: String = row.get(2)?;
                let ordinal: i64 = row.get(3)?;

                let fabric_id = Uuid::parse_str(&fabric_id_str).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?;
                let cell_id = Uuid::parse_str(&cell_id_str).map_err(|err| {
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

                Ok(FabricCell {
                    fabric_id,
                    cell_id,
                    relation_type,
                    ordinal,
                })
            })?;

            let mut result = Vec::new();
            for row in mapped {
                result.push(row?);
            }

            Ok(result)
        })
    }

    fn get_fabric_cells_at(&self, fabric_id: Uuid, ts: Timestamp) -> Result<Vec<FabricCell>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT fabric_id, cell_id, relation_type, ordinal
             FROM fabric_cells
             WHERE fabric_id = ?1
               AND valid_from <= ?2
               AND valid_to > ?2
             ORDER BY relation_type ASC, ordinal ASC",
            )?;

            let mapped =
                stmt.query_map(params![fabric_id.to_string(), format_db_time(&ts)], |row| {
                    let fabric_id_str: String = row.get(0)?;
                    let cell_id_str: String = row.get(1)?;
                    let relation_type_str: String = row.get(2)?;
                    let ordinal: i64 = row.get(3)?;

                    let fabric_id = Uuid::parse_str(&fabric_id_str).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?;

                    let cell_id = Uuid::parse_str(&cell_id_str).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?;

                    let relation_type =
                        serde_json::from_str(&relation_type_str).map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?;

                    Ok(FabricCell {
                        fabric_id,
                        cell_id,
                        relation_type,
                        ordinal,
                    })
                })?;

            let mut result = Vec::new();
            for row in mapped {
                result.push(row?);
            }

            Ok(result)
        })
    }
}

impl Storage for SqliteStorage {
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
        let table = Self::table_for_cell_type(cell_type);
        self.replace_cell_in_table(table, id, cell_type, format, content, fabric_id)
    }

    fn delete_cell(&self, id: Uuid) -> Result<()> {
        self.close_active_version(id)
    }

    fn get_cell(&self, id: Uuid) -> Result<Cell> {
        SqliteStorage::get_cell(self, id)
    }

    fn get_cell_at(&self, id: Uuid, ts: Timestamp) -> Result<Cell> {
        SqliteStorage::get_cell_at(self, id, ts)
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
        SqliteStorage::insert_fabric_cell(self, fabric_cell)
    }

    fn next_relation_ordinal(&self, fabric_id: Uuid, relation_type: &RelationType) -> Result<i64> {
        SqliteStorage::next_relation_ordinal(self, fabric_id, relation_type)
    }

    fn next_relation_ordinal_at(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
        ts: Timestamp,
    ) -> Result<i64> {
        SqliteStorage::next_relation_ordinal_at(self, fabric_id, relation_type, ts)
    }

    fn get_cells_by_relation(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
    ) -> Result<Vec<Uuid>> {
        SqliteStorage::get_cells_by_relation(self, fabric_id, relation_type)
    }

    fn get_cells_by_relation_at(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
        ts: Timestamp,
    ) -> Result<Vec<Uuid>> {
        SqliteStorage::get_cells_by_relation_at(self, fabric_id, relation_type, ts)
    }

    fn get_fabric_cells(&self, fabric_id: Uuid) -> Result<Vec<FabricCell>> {
        SqliteStorage::get_fabric_cells(self, fabric_id)
    }

    fn get_fabric_cells_at(&self, fabric_id: Uuid, ts: Timestamp) -> Result<Vec<FabricCell>> {
        SqliteStorage::get_fabric_cells_at(self, fabric_id, ts)
    }
}
