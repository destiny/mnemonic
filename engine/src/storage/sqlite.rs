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

use super::{FabricMemberRow, Storage};
use crate::error::{EngineError, Result};
use crate::models::{Cell, CellType, ContentFormat, RelationType, Timestamp};
use crate::storage::FabricRecord;
use crate::storage::time::{FUTURE_SENTINEL_STR, MIN_TIMESTAMP_STR, format_db_time, parse_db_time};

pub struct SqliteStorage {
    conn: Mutex<Connection>,
    temporal_fabric_cells: bool,
}

impl SqliteStorage {
    const SQLITE_NOW: &'static str = "STRFTIME('%Y-%m-%d %H:%M:%f', 'now')";

    fn root_relation_value() -> Result<String> {
        Ok(serde_json::to_string(&RelationType::Root)?)
    }

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
                "CREATE TABLE IF NOT EXISTS fabric (
                id TEXT NOT NULL,
                valid_from DATETIME NOT NULL DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'now')),
                valid_to DATETIME NOT NULL DEFAULT '2100-01-01 00:00:00.000000',
                PRIMARY KEY (id, valid_to)
            )",
                params![],
            )?;

            conn.execute(
                "CREATE TABLE IF NOT EXISTS cell (
                id TEXT NOT NULL,
                cell_type TEXT NOT NULL,
                format TEXT NOT NULL,
                content BLOB NOT NULL,
                valid_from DATETIME NOT NULL DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'now')),
                valid_to DATETIME NOT NULL DEFAULT '2100-01-01 00:00:00.000000',
                fabric_id TEXT,
                PRIMARY KEY (id, valid_to)
            )",
                params![],
            )?;

            conn.execute(
                "CREATE TABLE IF NOT EXISTS fabric_cells (
                fabric_id TEXT NOT NULL,
                cell_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                ordinal INTEGER,
                valid_from DATETIME NOT NULL DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'now')),
                valid_to DATETIME NOT NULL DEFAULT '2100-01-01 00:00:00.000000',
                PRIMARY KEY (fabric_id, relation_type, cell_id, valid_to)
            )",
                params![],
            )?;

            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS fabric_one_active_per_id
             ON fabric(id)
             WHERE valid_to = '2100-01-01 00:00:00.000000'",
                params![],
            )?;

            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS cell_one_active_per_id
             ON cell(id)
             WHERE valid_to = '2100-01-01 00:00:00.000000'",
                params![],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS cell_current_lookup
             ON cell(id, valid_from, valid_to)",
                params![],
            )?;

            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS fabric_cells_one_active_per_slot
             ON fabric_cells(fabric_id, relation_type, ordinal)
             WHERE valid_to = '2100-01-01 00:00:00.000000' AND ordinal IS NOT NULL",
                params![],
            )?;

            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS fabric_cells_one_active_root
             ON fabric_cells(fabric_id, relation_type)
             WHERE valid_to = '2100-01-01 00:00:00.000000' AND ordinal IS NULL",
                params![],
            )?;

            Ok(())
        })
    }

    fn insert_cell_record(
        conn: &Connection,
        id: Uuid,
        cell_type: &CellType,
        format: &ContentFormat,
        content: &[u8],
        fabric_id: Option<Uuid>,
        valid_from: Option<&str>,
    ) -> Result<()> {
        let (sql, params): (&str, Vec<rusqlite::types::Value>) = match valid_from {
            Some(valid_from) => (
                "INSERT INTO cell (id, cell_type, format, content, valid_from, fabric_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                vec![
                    id.to_string().into(),
                    serde_json::to_string(cell_type)?.into(),
                    serde_json::to_string(format)?.into(),
                    content.to_vec().into(),
                    valid_from.to_string().into(),
                    fabric_id.map(|value| value.to_string()).into(),
                ],
            ),
            None => (
                "INSERT INTO cell (id, cell_type, format, content, fabric_id)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                vec![
                    id.to_string().into(),
                    serde_json::to_string(cell_type)?.into(),
                    serde_json::to_string(format)?.into(),
                    content.to_vec().into(),
                    fabric_id.map(|value| value.to_string()).into(),
                ],
            ),
        };
        conn.execute(sql, rusqlite::params_from_iter(params))?;
        Ok(())
    }

    fn insert_root_membership(
        conn: &Connection,
        fabric_id: Uuid,
        cell_id: Uuid,
        valid_from: &str,
    ) -> Result<()> {
        conn.execute(
            "INSERT INTO fabric_cells (fabric_id, cell_id, relation_type, ordinal, valid_from, valid_to)
             VALUES (?1, ?2, ?3, NULL, ?4, ?5)",
            params![
                fabric_id.to_string(),
                cell_id.to_string(),
                Self::root_relation_value()?,
                valid_from,
                FUTURE_SENTINEL_STR,
            ],
        )?;
        Ok(())
    }

    fn insert_fabric_record(conn: &Connection, id: Uuid, valid_from: Option<&str>) -> Result<()> {
        match valid_from {
            Some(valid_from) => {
                conn.execute(
                    "INSERT INTO fabric (id, valid_from) VALUES (?1, ?2)",
                    params![id.to_string(), valid_from],
                )?;
            }
            None => {
                conn.execute(
                    "INSERT INTO fabric (id) VALUES (?1)",
                    params![id.to_string()],
                )?;
            }
        }
        Ok(())
    }

    fn insert_cell(
        &self,
        id: Uuid,
        cell_type: &CellType,
        format: &ContentFormat,
        content: &[u8],
        fabric_id: Option<Uuid>,
    ) -> Result<Cell> {
        self.with_conn(|conn| {
            let now = Self::current_db_time(conn)?;
            Self::insert_cell_record(conn, id, cell_type, format, content, fabric_id, Some(&now))?;

            if let Some(fabric_id) = fabric_id {
                Self::insert_fabric_record(conn, fabric_id, Some(&now))?;
                Self::insert_root_membership(conn, fabric_id, id, &now)?;
            }

            Self::get_current_cell(conn, id)?.ok_or(EngineError::NotFound)
        })
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

    fn fabric_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FabricRecord> {
        let id_str: String = row.get(0)?;
        let valid_from_str: String = row.get(1)?;
        let valid_to_str: String = row.get(2)?;

        let id = Uuid::parse_str(&id_str).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
        })?;
        let valid_from = parse_db_time(&valid_from_str).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(err))
        })?;
        let valid_to = parse_db_time(&valid_to_str).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(err))
        })?;

        Ok(FabricRecord {
            id,
            valid_from,
            valid_to,
        })
    }

    fn get_cell_at_internal(&self, id: Uuid, ts: Timestamp) -> Result<Option<Cell>> {
        let sql = "SELECT id, cell_type, format, content, valid_from, valid_to, fabric_id
             FROM cell
             WHERE id = ?1 AND valid_from <= ?2 AND valid_to > ?2
             ORDER BY valid_from DESC
             LIMIT 1";

        self.with_conn(|conn| {
            let mut stmt = conn.prepare(sql)?;
            let cell = stmt
                .query_row(
                    params![id.to_string(), format_db_time(&ts)],
                    Self::cell_from_row,
                )
                .optional()?;

            Ok(cell)
        })
    }

    fn get_current_cell(conn: &Connection, id: Uuid) -> Result<Option<Cell>> {
        let sql = format!(
            "SELECT id, cell_type, format, content, valid_from, valid_to, fabric_id
             FROM cell
             WHERE id = ?1 AND valid_from <= {} AND valid_to > {}
             ORDER BY valid_from DESC
             LIMIT 1",
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

    fn get_cell_history_internal(&self, id: Uuid) -> Result<Vec<Cell>> {
        let sql = "SELECT id, cell_type, format, content, valid_from, valid_to, fabric_id
             FROM cell
             WHERE id = ?1
             ORDER BY valid_from ASC, valid_to ASC";

        self.with_conn(|conn| {
            let mut stmt = conn.prepare(sql)?;
            let mapped = stmt.query_map(params![id.to_string()], Self::cell_from_row)?;
            let mut result = Vec::new();
            for row in mapped {
                result.push(row?);
            }
            Ok(result)
        })
    }

    fn get_fabric_at_internal(&self, id: Uuid, ts: Timestamp) -> Result<Option<FabricRecord>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, valid_from, valid_to
                 FROM fabric
                 WHERE id = ?1 AND valid_from <= ?2 AND valid_to > ?2
                 ORDER BY valid_from DESC
                 LIMIT 1",
            )?;
            let fabric = stmt
                .query_row(
                    params![id.to_string(), format_db_time(&ts)],
                    Self::fabric_from_row,
                )
                .optional()?;
            Ok(fabric)
        })
    }

    fn get_current_fabric(conn: &Connection, id: Uuid) -> Result<Option<FabricRecord>> {
        let mut stmt = conn.prepare(&format!(
            "SELECT id, valid_from, valid_to
             FROM fabric
             WHERE id = ?1 AND valid_from <= {} AND valid_to > {}
             ORDER BY valid_from DESC
             LIMIT 1",
            Self::SQLITE_NOW,
            Self::SQLITE_NOW
        ))?;
        let fabric = stmt
            .query_row(params![id.to_string()], Self::fabric_from_row)
            .optional()?;
        Ok(fabric)
    }

    fn get_fabric(&self, id: Uuid) -> Result<FabricRecord> {
        self.with_conn(|conn| Self::get_current_fabric(conn, id)?.ok_or(EngineError::NotFound))
    }

    fn get_fabric_at(&self, id: Uuid, ts: Timestamp) -> Result<FabricRecord> {
        self.get_fabric_at_internal(id, ts)?
            .ok_or(EngineError::NotFound)
    }

    fn list_fabrics(&self) -> Result<Vec<Uuid>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT id
                 FROM fabric
                 WHERE valid_from <= {} AND valid_to > {}
                 ORDER BY id ASC",
                Self::SQLITE_NOW,
                Self::SQLITE_NOW
            ))?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            let mut fabrics = Vec::new();
            for row in rows {
                fabrics.push(Uuid::parse_str(&row?).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?);
            }
            Ok(fabrics)
        })
    }

    fn insert_fabric_cell(&self, fabric_cell: &FabricMemberRow) -> Result<()> {
        self.with_conn(|conn| {
            let relation = serde_json::to_string(&fabric_cell.relation_type)?;

            if self.temporal_fabric_cells {
                conn.execute(
                    &format!(
                        "UPDATE fabric_cells
                     SET valid_to = {}
                     WHERE fabric_id = ?1
                       AND relation_type = ?2
                       AND cell_id = ?3
                       AND valid_from <= {}
                       AND valid_to > {}",
                        Self::SQLITE_NOW,
                        Self::SQLITE_NOW,
                        Self::SQLITE_NOW
                    ),
                    params![
                        fabric_cell.fabric_id.to_string(),
                        relation.clone(),
                        fabric_cell.cell_id.to_string(),
                    ],
                )?;

                if let Some(ordinal) = fabric_cell.ordinal {
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
                        params![fabric_cell.fabric_id.to_string(), relation.clone(), ordinal],
                    )?;
                } else {
                    conn.execute(
                        &format!(
                            "UPDATE fabric_cells
                     SET valid_to = {}
                     WHERE fabric_id = ?1
                       AND relation_type = ?2
                       AND ordinal IS NULL
                       AND valid_from <= {}
                       AND valid_to > {}",
                            Self::SQLITE_NOW,
                            Self::SQLITE_NOW,
                            Self::SQLITE_NOW
                        ),
                        params![fabric_cell.fabric_id.to_string(), relation.clone()],
                    )?;
                }

                conn.execute(
                    "INSERT INTO fabric_cells (fabric_id, cell_id, relation_type, ordinal)
                 VALUES (?1, ?2, ?3, ?4)",
                    params![
                        fabric_cell.fabric_id.to_string(),
                        fabric_cell.cell_id.to_string(),
                        relation,
                        fabric_cell.ordinal,
                    ],
                )?;
            } else {
                conn.execute(
                    "DELETE FROM fabric_cells
                     WHERE fabric_id = ?1
                       AND relation_type = ?2
                       AND cell_id = ?3",
                    params![
                        fabric_cell.fabric_id.to_string(),
                        relation.clone(),
                        fabric_cell.cell_id.to_string(),
                    ],
                )?;

                if let Some(ordinal) = fabric_cell.ordinal {
                    conn.execute(
                        "DELETE FROM fabric_cells
                     WHERE fabric_id = ?1
                       AND relation_type = ?2
                       AND ordinal = ?3",
                        params![fabric_cell.fabric_id.to_string(), relation.clone(), ordinal],
                    )?;
                } else {
                    conn.execute(
                        "DELETE FROM fabric_cells
                     WHERE fabric_id = ?1
                       AND relation_type = ?2
                       AND ordinal IS NULL",
                        params![fabric_cell.fabric_id.to_string(), relation.clone()],
                    )?;
                }

                conn.execute(
                    "INSERT INTO fabric_cells (fabric_id, cell_id, relation_type, ordinal, valid_from, valid_to)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        fabric_cell.fabric_id.to_string(),
                        fabric_cell.cell_id.to_string(),
                        relation,
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

    fn get_fabric_cells(&self, fabric_id: Uuid) -> Result<Vec<FabricMemberRow>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT fabric_id, cell_id, relation_type, ordinal
                 FROM fabric_cells
                 WHERE fabric_id = ?1
                   AND valid_from <= {}
                   AND valid_to > {}
                 ORDER BY CASE WHEN relation_type = ?2 THEN 0 ELSE 1 END,
                          ordinal IS NULL ASC,
                          ordinal ASC,
                          cell_id ASC",
                Self::SQLITE_NOW,
                Self::SQLITE_NOW
            ))?;

            let mapped = stmt.query_map(
                params![fabric_id.to_string(), Self::root_relation_value()?],
                |row| {
                    let fabric_id_str: String = row.get(0)?;
                    let cell_id_str: String = row.get(1)?;
                    let relation_type_str: String = row.get(2)?;
                    let ordinal: Option<i64> = row.get(3)?;

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

                    Ok(FabricMemberRow {
                        fabric_id,
                        cell_id,
                        relation_type,
                        ordinal,
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

    fn get_fabric_cells_at(&self, fabric_id: Uuid, ts: Timestamp) -> Result<Vec<FabricMemberRow>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT fabric_id, cell_id, relation_type, ordinal
             FROM fabric_cells
             WHERE fabric_id = ?1
               AND valid_from <= ?2
               AND valid_to > ?2
             ORDER BY CASE WHEN relation_type = ?3 THEN 0 ELSE 1 END,
                      ordinal IS NULL ASC,
                      ordinal ASC,
                      cell_id ASC",
            )?;

            let mapped = stmt.query_map(
                params![
                    fabric_id.to_string(),
                    format_db_time(&ts),
                    Self::root_relation_value()?
                ],
                |row| {
                    let fabric_id_str: String = row.get(0)?;
                    let cell_id_str: String = row.get(1)?;
                    let relation_type_str: String = row.get(2)?;
                    let ordinal: Option<i64> = row.get(3)?;

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

                    Ok(FabricMemberRow {
                        fabric_id,
                        cell_id,
                        relation_type,
                        ordinal,
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
}

impl Storage for SqliteStorage {
    fn insert_fabric(&self, id: Uuid) -> Result<FabricRecord> {
        self.with_conn(|conn| {
            let now = Self::current_db_time(conn)?;
            Self::insert_fabric_record(conn, id, Some(&now))?;
            Self::get_current_fabric(conn, id)?.ok_or(EngineError::NotFound)
        })
    }

    fn insert_cell(
        &self,
        id: Uuid,
        cell_type: &CellType,
        format: &ContentFormat,
        content: &[u8],
        fabric_id: Option<Uuid>,
    ) -> Result<Cell> {
        SqliteStorage::insert_cell(self, id, cell_type, format, content, fabric_id)
    }

    fn replace_cell(
        &self,
        id: Uuid,
        cell_type: &CellType,
        format: &ContentFormat,
        content: &[u8],
        fabric_id: Option<Uuid>,
    ) -> Result<Cell> {
        self.with_conn(|conn| {
            let now = Self::current_db_time(conn)?;
            let changed = conn.execute(
                "UPDATE cell
                 SET valid_to = ?2
                 WHERE id = ?1 AND valid_from <= ?2 AND valid_to > ?2",
                params![id.to_string(), now.clone()],
            )?;
            if changed == 0 {
                return Err(EngineError::NotFound);
            }
            Self::insert_cell_record(conn, id, cell_type, format, content, fabric_id, Some(&now))?;
            Self::get_current_cell(conn, id)?.ok_or(EngineError::NotFound)
        })
    }

    fn delete_cell(&self, id: Uuid) -> Result<()> {
        let cell = self.get_cell(id)?;
        self.with_conn(|conn| {
            let now = Self::current_db_time(conn)?;
            let changed = conn.execute(
                "UPDATE cell
                 SET valid_to = ?2
                 WHERE id = ?1 AND valid_from <= ?2 AND valid_to > ?2",
                params![id.to_string(), now.clone()],
            )?;
            if changed == 0 {
                return Err(EngineError::NotFound);
            }
            if let Some(fabric_id) = cell.fabric_id {
                conn.execute(
                    "UPDATE fabric
                     SET valid_to = ?2
                     WHERE id = ?1 AND valid_from <= ?2 AND valid_to > ?2",
                    params![fabric_id.to_string(), now],
                )?;
            }
            Ok(())
        })
    }

    fn get_fabric(&self, id: Uuid) -> Result<FabricRecord> {
        SqliteStorage::get_fabric(self, id)
    }

    fn get_fabric_at(&self, id: Uuid, ts: Timestamp) -> Result<FabricRecord> {
        SqliteStorage::get_fabric_at(self, id, ts)
    }

    fn list_fabrics(&self) -> Result<Vec<Uuid>> {
        SqliteStorage::list_fabrics(self)
    }

    fn get_cell(&self, id: Uuid) -> Result<Cell> {
        self.with_conn(|conn| Self::get_current_cell(conn, id)?.ok_or(EngineError::NotFound))
    }

    fn get_cell_at(&self, id: Uuid, ts: Timestamp) -> Result<Cell> {
        self.get_cell_at_internal(id, ts)?
            .ok_or(EngineError::NotFound)
    }

    fn get_cell_history(&self, id: Uuid) -> Result<Vec<Cell>> {
        self.get_cell_history_internal(id)
    }

    fn insert_fabric_cell(&self, fabric_cell: &FabricMemberRow) -> Result<()> {
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

    fn get_fabric_cells(&self, fabric_id: Uuid) -> Result<Vec<FabricMemberRow>> {
        SqliteStorage::get_fabric_cells(self, fabric_id)
    }

    fn get_fabric_cells_at(&self, fabric_id: Uuid, ts: Timestamp) -> Result<Vec<FabricMemberRow>> {
        SqliteStorage::get_fabric_cells_at(self, fabric_id, ts)
    }
}
