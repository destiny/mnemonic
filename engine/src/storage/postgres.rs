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

use super::{FabricMemberRow, FabricRecord, Storage};
use crate::error::{EngineError, Result};
use crate::models::{Cell, CellType, ContentFormat, RelationType, Timestamp};
use crate::storage::time::{FUTURE_SENTINEL_STR, MIN_TIMESTAMP_STR, format_db_time, parse_db_time};

#[derive(Debug, Clone, Copy)]
enum CellTable {
    Data,
    Meta,
}

impl CellTable {
    fn as_str(self) -> &'static str {
        "cell"
    }
}

pub struct PostgresStorage {
    client: Mutex<Client>,
    temporal_fabric_cells: bool,
}

impl PostgresStorage {
    const NOW_EXPR: &'static str = "clock_timestamp()";

    fn root_relation_value() -> Result<String> {
        Ok(serde_json::to_string(&RelationType::Root)?)
    }

    pub fn new(connection_str: &str, temporal_fabric_cells: bool) -> Result<Self> {
        let client = Client::connect(connection_str, NoTls)?;
        let storage = Self {
            client: Mutex::new(client),
            temporal_fabric_cells,
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
                CREATE TABLE IF NOT EXISTS fabric (
                    id TEXT NOT NULL,
                    valid_from TIMESTAMP NOT NULL DEFAULT (clock_timestamp()),
                    valid_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '2100-01-01 00:00:00',
                    PRIMARY KEY (id, valid_to)
                );

                CREATE TABLE IF NOT EXISTS cell (
                    id TEXT NOT NULL,
                    cell_type TEXT NOT NULL,
                    format TEXT NOT NULL,
                    content BYTEA NOT NULL,
                    valid_from TIMESTAMP NOT NULL DEFAULT (clock_timestamp()),
                    valid_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '2100-01-01 00:00:00',
                    fabric_id TEXT,
                    PRIMARY KEY (id, valid_to)
                );

                CREATE TABLE IF NOT EXISTS fabric_cells (
                    fabric_id TEXT NOT NULL,
                    cell_id TEXT NOT NULL,
                    relation_type TEXT NOT NULL,
                    ordinal BIGINT,
                    valid_from TIMESTAMP NOT NULL DEFAULT (clock_timestamp()),
                    valid_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '2100-01-01 00:00:00',
                    PRIMARY KEY (fabric_id, relation_type, cell_id, valid_to)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS fabric_one_active_per_id
                ON fabric(id)
                WHERE valid_to = TIMESTAMP '2100-01-01 00:00:00';

                CREATE UNIQUE INDEX IF NOT EXISTS cell_one_active_per_id
                ON cell(id)
                WHERE valid_to = TIMESTAMP '2100-01-01 00:00:00';

                CREATE INDEX IF NOT EXISTS cell_current_lookup
                ON cell(id, valid_from, valid_to);

                CREATE UNIQUE INDEX IF NOT EXISTS fabric_cells_one_active_per_slot
                ON fabric_cells(fabric_id, relation_type, ordinal)
                WHERE valid_to = TIMESTAMP '2100-01-01 00:00:00' AND ordinal IS NOT NULL;

                CREATE UNIQUE INDEX IF NOT EXISTS fabric_cells_one_active_root
                ON fabric_cells(fabric_id, relation_type)
                WHERE valid_to = TIMESTAMP '2100-01-01 00:00:00' AND ordinal IS NULL;
                ",
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
        let cell_type = serde_json::to_string(cell_type)?;
        let format = serde_json::to_string(format)?;
        let sql = format!(
            "INSERT INTO {} (id, cell_type, format, content, fabric_id)
             VALUES ($1, $2, $3, $4, $5)",
            table.as_str()
        );

        self.with_client(|client| {
            client.execute(
                &sql,
                &[
                    &id.to_string(),
                    &cell_type,
                    &format,
                    &content,
                    &fabric_id.map(|value| value.to_string()),
                ],
            )?;

            if let Some(fabric_id) = fabric_id {
                client.execute("INSERT INTO fabric (id) VALUES ($1)", &[&fabric_id.to_string()])?;
                client.execute(
                    "INSERT INTO fabric_cells (fabric_id, cell_id, relation_type, ordinal, valid_from, valid_to)
                     VALUES ($1, $2, $3, NULL, $4, $5)",
                    &[
                        &fabric_id.to_string(),
                        &id.to_string(),
                        &Self::root_relation_value()?,
                        &MIN_TIMESTAMP_STR,
                        &FUTURE_SENTINEL_STR,
                    ],
                )?;
            }

            Self::get_current_cell_from_table(client, table, id)?.ok_or(EngineError::NotFound)
        })
    }

    fn cell_from_row(row: &postgres::Row) -> Result<Cell> {
        let id_str: String = row.get(0);
        let cell_type_str: String = row.get(1);
        let format_str: String = row.get(2);
        let content: Vec<u8> = row.get(3);
        let valid_from_str: String = row.get(4);
        let valid_to_str: String = row.get(5);
        let fabric_id_str: Option<String> = row.get(6);

        let id = Uuid::parse_str(&id_str)
            .map_err(|err| EngineError::InvalidData(format!("invalid UUID in row: {err}")))?;
        let cell_type = serde_json::from_str(&cell_type_str)?;
        let format = serde_json::from_str(&format_str)?;
        let fabric_id = fabric_id_str
            .map(|value| Uuid::parse_str(&value))
            .transpose()
            .map_err(|err| {
                EngineError::InvalidData(format!("invalid fabric UUID in cell row: {err}"))
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

    fn fabric_from_row(row: &postgres::Row) -> Result<FabricRecord> {
        let id_str: String = row.get(0);
        let valid_from_str: String = row.get(1);
        let valid_to_str: String = row.get(2);
        Ok(FabricRecord {
            id: Uuid::parse_str(&id_str).map_err(|err| {
                EngineError::InvalidData(format!("invalid fabric UUID in row: {err}"))
            })?,
            valid_from: parse_db_time(&valid_from_str)?,
            valid_to: parse_db_time(&valid_to_str)?,
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
                    to_char(valid_from, 'YYYY-MM-DD HH24:MI:SS.US') as valid_from,
                    to_char(valid_to, 'YYYY-MM-DD HH24:MI:SS.US') as valid_to,
                    fabric_id
             FROM {}
             WHERE id = $1 AND valid_from <= $2 AND valid_to > $2
             ORDER BY valid_from DESC
             LIMIT 1",
            table.as_str()
        );

        self.with_client(|client| {
            let id_str = id.to_string();
            let row = client.query_opt(&sql, &[&id_str, &format_db_time(&ts)])?;
            row.map(|r| Self::cell_from_row(&r)).transpose()
        })
    }

    fn get_current_cell_from_table(
        client: &mut Client,
        table: CellTable,
        id: Uuid,
    ) -> Result<Option<Cell>> {
        let sql = format!(
            "SELECT id,
                    cell_type,
                    format,
                    content,
                    to_char(valid_from, 'YYYY-MM-DD HH24:MI:SS.US') as valid_from,
                    to_char(valid_to, 'YYYY-MM-DD HH24:MI:SS.US') as valid_to,
                    fabric_id
             FROM {}
             WHERE id = $1 AND valid_from <= {} AND valid_to > {}
             ORDER BY valid_from DESC
             LIMIT 1",
            table.as_str(),
            Self::NOW_EXPR,
            Self::NOW_EXPR
        );

        let row = client.query_opt(&sql, &[&id.to_string()])?;
        row.map(|r| Self::cell_from_row(&r)).transpose()
    }

    fn get_cell_history_from_table(&self, table: CellTable, id: Uuid) -> Result<Vec<Cell>> {
        let sql = format!(
            "SELECT id,
                    cell_type,
                    format,
                    content,
                    to_char(valid_from, 'YYYY-MM-DD HH24:MI:SS.US') as valid_from,
                    to_char(valid_to, 'YYYY-MM-DD HH24:MI:SS.US') as valid_to,
                    fabric_id
             FROM {}
             WHERE id = $1
             ORDER BY valid_from ASC, valid_to ASC",
            table.as_str()
        );
        self.with_client(|client| {
            let rows = client.query(&sql, &[&id.to_string()])?;
            rows.into_iter()
                .map(|row| Self::cell_from_row(&row))
                .collect()
        })
    }

    fn get_cell(&self, id: Uuid) -> Result<Cell> {
        self.with_client(|client| {
            if let Some(cell) = Self::get_current_cell_from_table(client, CellTable::Data, id)? {
                return Ok(cell);
            }
            if let Some(cell) = Self::get_current_cell_from_table(client, CellTable::Meta, id)? {
                return Ok(cell);
            }
            Err(EngineError::NotFound)
        })
    }

    fn active_table_for_id(&self, id: Uuid) -> Result<Option<CellTable>> {
        self.with_client(|client| {
            if Self::get_current_cell_from_table(client, CellTable::Data, id)?.is_some() {
                return Ok(Some(CellTable::Data));
            }
            if Self::get_current_cell_from_table(client, CellTable::Meta, id)?.is_some() {
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
             WHERE id = $1 AND valid_from <= {} AND valid_to > {}",
            table.as_str(),
            Self::NOW_EXPR,
            Self::NOW_EXPR,
            Self::NOW_EXPR
        );

        self.with_client(|client| {
            let id_str = id.to_string();
            let changed = client.execute(&sql, &[&id_str])?;
            if changed == 0 {
                return Err(EngineError::NotFound);
            }
            Ok(())
        })
    }
}

impl Storage for PostgresStorage {
    fn insert_fabric(&self, id: Uuid) -> Result<FabricRecord> {
        self.with_client(|client| {
            client.execute("INSERT INTO fabric (id) VALUES ($1)", &[&id.to_string()])?;
            let row = client.query_one(
                &format!(
                    "SELECT id, to_char(valid_from, 'YYYY-MM-DD HH24:MI:SS.US'), to_char(valid_to, 'YYYY-MM-DD HH24:MI:SS.US')
                     FROM fabric
                     WHERE id = $1 AND valid_from <= {} AND valid_to > {}
                     ORDER BY valid_from DESC
                     LIMIT 1",
                    Self::NOW_EXPR,
                    Self::NOW_EXPR
                ),
                &[&id.to_string()],
            )?;
            Self::fabric_from_row(&row)
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
        self.insert_cell_in_table(CellTable::Data, id, cell_type, format, content, fabric_id)
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

    fn get_fabric(&self, id: Uuid) -> Result<FabricRecord> {
        self.with_client(|client| {
            let row = client.query_opt(
                &format!(
                    "SELECT id, to_char(valid_from, 'YYYY-MM-DD HH24:MI:SS.US'), to_char(valid_to, 'YYYY-MM-DD HH24:MI:SS.US')
                     FROM fabric
                     WHERE id = $1 AND valid_from <= {} AND valid_to > {}
                     ORDER BY valid_from DESC
                     LIMIT 1",
                    Self::NOW_EXPR,
                    Self::NOW_EXPR
                ),
                &[&id.to_string()],
            )?;
            row.map(|row| Self::fabric_from_row(&row))
                .transpose()?
                .ok_or(EngineError::NotFound)
        })
    }

    fn get_fabric_at(&self, id: Uuid, ts: Timestamp) -> Result<FabricRecord> {
        self.with_client(|client| {
            let row = client.query_opt(
                "SELECT id, to_char(valid_from, 'YYYY-MM-DD HH24:MI:SS.US'), to_char(valid_to, 'YYYY-MM-DD HH24:MI:SS.US')
                 FROM fabric
                 WHERE id = $1 AND valid_from <= $2 AND valid_to > $2
                 ORDER BY valid_from DESC
                 LIMIT 1",
                &[&id.to_string(), &format_db_time(&ts)],
            )?;
            row.map(|row| Self::fabric_from_row(&row))
                .transpose()?
                .ok_or(EngineError::NotFound)
        })
    }

    fn list_fabrics(&self) -> Result<Vec<Uuid>> {
        self.with_client(|client| {
            let rows = client.query(
                &format!(
                    "SELECT id FROM fabric WHERE valid_from <= {} AND valid_to > {} ORDER BY id ASC",
                    Self::NOW_EXPR,
                    Self::NOW_EXPR
                ),
                &[],
            )?;
            rows.into_iter()
                .map(|row| {
                    let id_str: String = row.get(0);
                    Uuid::parse_str(&id_str)
                        .map_err(|err| EngineError::InvalidData(format!("invalid fabric UUID in row: {err}")))
                })
                .collect()
        })
    }

    fn get_cell(&self, id: Uuid) -> Result<Cell> {
        PostgresStorage::get_cell(self, id)
    }

    fn get_cell_at(&self, id: Uuid, ts: Timestamp) -> Result<Cell> {
        self.get_cell_at_from_table(CellTable::Data, id, ts)?
            .ok_or(EngineError::NotFound)
    }

    fn get_cell_history(&self, id: Uuid) -> Result<Vec<Cell>> {
        self.get_cell_history_from_table(CellTable::Data, id)
    }

    fn insert_fabric_cell(&self, fabric_cell: &FabricMemberRow) -> Result<()> {
        self.with_client(|client| {
            let fabric_id = fabric_cell.fabric_id.to_string();
            let cell_id = fabric_cell.cell_id.to_string();
            let relation_type = serde_json::to_string(&fabric_cell.relation_type)?;

            if self.temporal_fabric_cells {
                client.execute(
                    &format!(
                        "UPDATE fabric_cells
                     SET valid_to = {}
                     WHERE fabric_id = $1
                       AND relation_type = $2
                       AND cell_id = $3
                       AND valid_from <= {}
                       AND valid_to > {}",
                        Self::NOW_EXPR,
                        Self::NOW_EXPR,
                        Self::NOW_EXPR
                    ),
                    &[&fabric_id, &relation_type, &cell_id],
                )?;

                if let Some(ordinal) = fabric_cell.ordinal {
                    client.execute(
                        &format!(
                            "UPDATE fabric_cells
                     SET valid_to = {}
                     WHERE fabric_id = $1
                       AND relation_type = $2
                       AND ordinal = $3
                       AND valid_from <= {}
                       AND valid_to > {}",
                            Self::NOW_EXPR,
                            Self::NOW_EXPR,
                            Self::NOW_EXPR
                        ),
                        &[&fabric_id, &relation_type, &ordinal],
                    )?;
                } else {
                    client.execute(
                        &format!(
                            "UPDATE fabric_cells
                     SET valid_to = {}
                     WHERE fabric_id = $1
                       AND relation_type = $2
                       AND ordinal IS NULL
                       AND valid_from <= {}
                       AND valid_to > {}",
                            Self::NOW_EXPR,
                            Self::NOW_EXPR,
                            Self::NOW_EXPR
                        ),
                        &[&fabric_id, &relation_type],
                    )?;
                }

                client.execute(
                    "INSERT INTO fabric_cells (fabric_id, cell_id, relation_type, ordinal)
                     VALUES ($1, $2, $3, $4)",
                    &[&fabric_id, &cell_id, &relation_type, &fabric_cell.ordinal],
                )?;
            } else {
                client.execute(
                    "DELETE FROM fabric_cells
                     WHERE fabric_id = $1
                       AND relation_type = $2
                       AND cell_id = $3",
                    &[&fabric_id, &relation_type, &cell_id],
                )?;

                if let Some(ordinal) = fabric_cell.ordinal {
                    client.execute(
                        "DELETE FROM fabric_cells
                     WHERE fabric_id = $1
                       AND relation_type = $2
                       AND ordinal = $3",
                        &[&fabric_id, &relation_type, &ordinal],
                    )?;
                } else {
                    client.execute(
                        "DELETE FROM fabric_cells
                     WHERE fabric_id = $1
                       AND relation_type = $2
                       AND ordinal IS NULL",
                        &[&fabric_id, &relation_type],
                    )?;
                }

                client.execute(
                    "INSERT INTO fabric_cells (fabric_id, cell_id, relation_type, ordinal, valid_from, valid_to)
                     VALUES ($1, $2, $3, $4, $5, $6)",
                    &[
                        &fabric_id,
                        &cell_id,
                        &relation_type,
                        &fabric_cell.ordinal,
                        &MIN_TIMESTAMP_STR,
                        &FUTURE_SENTINEL_STR,
                    ],
                )?;
            }

            Ok(())
        })
    }

    fn next_relation_ordinal(&self, fabric_id: Uuid, relation_type: &RelationType) -> Result<i64> {
        self.with_client(|client| {
            let fabric = fabric_id.to_string();
            let relation = serde_json::to_string(relation_type)?;
            let row = client.query_one(
                &format!(
                    "SELECT COALESCE(MAX(ordinal), -1) + 1
                     FROM fabric_cells
                     WHERE fabric_id = $1
                       AND relation_type = $2
                       AND valid_from <= {}
                       AND valid_to > {}",
                    Self::NOW_EXPR,
                    Self::NOW_EXPR
                ),
                &[&fabric, &relation],
            )?;
            Ok(row.get(0))
        })
    }

    fn next_relation_ordinal_at(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
        ts: Timestamp,
    ) -> Result<i64> {
        self.with_client(|client| {
            let fabric = fabric_id.to_string();
            let relation = serde_json::to_string(relation_type)?;
            let row = client.query_one(
                "SELECT COALESCE(MAX(ordinal), -1) + 1
                 FROM fabric_cells
                 WHERE fabric_id = $1
                   AND relation_type = $2
                   AND valid_from <= $3
                   AND valid_to > $3",
                &[&fabric, &relation, &format_db_time(&ts)],
            )?;
            Ok(row.get(0))
        })
    }

    fn get_cells_by_relation(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
    ) -> Result<Vec<Uuid>> {
        self.with_client(|client| {
            let fabric = fabric_id.to_string();
            let relation = serde_json::to_string(relation_type)?;
            let rows = client.query(
                &format!(
                    "SELECT cell_id
                     FROM fabric_cells
                     WHERE fabric_id = $1
                       AND relation_type = $2
                       AND valid_from <= {}
                       AND valid_to > {}
                     ORDER BY ordinal ASC",
                    Self::NOW_EXPR,
                    Self::NOW_EXPR
                ),
                &[&fabric, &relation],
            )?;

            let mut result = Vec::with_capacity(rows.len());
            for row in rows {
                let cell_id_str: String = row.get(0);
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
        self.with_client(|client| {
            let fabric = fabric_id.to_string();
            let relation = serde_json::to_string(relation_type)?;
            let rows = client.query(
                "SELECT cell_id
                 FROM fabric_cells
                 WHERE fabric_id = $1
                   AND relation_type = $2
                   AND valid_from <= $3
                   AND valid_to > $3
                 ORDER BY ordinal ASC",
                &[&fabric, &relation, &format_db_time(&ts)],
            )?;

            let mut result = Vec::with_capacity(rows.len());
            for row in rows {
                let cell_id_str: String = row.get(0);
                let cell_id = Uuid::parse_str(&cell_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid cell UUID in fabric_cells: {err}"))
                })?;
                result.push(cell_id);
            }

            Ok(result)
        })
    }

    fn get_fabric_cells(&self, fabric_id: Uuid) -> Result<Vec<FabricMemberRow>> {
        self.with_client(|client| {
            let fabric = fabric_id.to_string();
            let rows = client.query(
                &format!(
                    "SELECT fabric_id, cell_id, relation_type, ordinal
                     FROM fabric_cells
                     WHERE fabric_id = $1
                       AND valid_from <= {}
                       AND valid_to > {}
                     ORDER BY CASE WHEN relation_type = $2 THEN 0 ELSE 1 END,
                              ordinal ASC NULLS LAST,
                              cell_id ASC",
                    Self::NOW_EXPR,
                    Self::NOW_EXPR
                ),
                &[&fabric, &Self::root_relation_value()?],
            )?;

            let mut result = Vec::with_capacity(rows.len());
            for row in rows {
                let fabric_id_str: String = row.get(0);
                let cell_id_str: String = row.get(1);
                let relation_type_str: String = row.get(2);
                let ordinal: Option<i64> = row.get(3);
                let fabric_id = Uuid::parse_str(&fabric_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid fabric UUID in fabric_cells: {err}"))
                })?;
                let cell_id = Uuid::parse_str(&cell_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid cell UUID in fabric_cells: {err}"))
                })?;
                let relation_type = serde_json::from_str(&relation_type_str)?;
                result.push(FabricMemberRow {
                    fabric_id,
                    cell_id,
                    relation_type,
                    ordinal,
                });
            }
            Ok(result)
        })
    }

    fn get_fabric_cells_at(&self, fabric_id: Uuid, ts: Timestamp) -> Result<Vec<FabricMemberRow>> {
        self.with_client(|client| {
            let fabric = fabric_id.to_string();
            let rows = client.query(
                "SELECT fabric_id, cell_id, relation_type, ordinal
                 FROM fabric_cells
                 WHERE fabric_id = $1
                   AND valid_from <= $2
                   AND valid_to > $2
                 ORDER BY CASE WHEN relation_type = $3 THEN 0 ELSE 1 END,
                          ordinal ASC NULLS LAST,
                          cell_id ASC",
                &[&fabric, &format_db_time(&ts), &Self::root_relation_value()?],
            )?;

            let mut result = Vec::with_capacity(rows.len());
            for row in rows {
                let fabric_id_str: String = row.get(0);
                let cell_id_str: String = row.get(1);
                let relation_type_str: String = row.get(2);
                let ordinal: Option<i64> = row.get(3);

                let fabric_id = Uuid::parse_str(&fabric_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid fabric UUID in fabric_cells: {err}"))
                })?;
                let cell_id = Uuid::parse_str(&cell_id_str).map_err(|err| {
                    EngineError::InvalidData(format!("invalid cell UUID in fabric_cells: {err}"))
                })?;
                let relation_type = serde_json::from_str(&relation_type_str)?;

                result.push(FabricMemberRow {
                    fabric_id,
                    cell_id,
                    relation_type,
                    ordinal,
                });
            }

            Ok(result)
        })
    }
}
