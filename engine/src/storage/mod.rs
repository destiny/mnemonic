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

use uuid::Uuid;

use crate::error::Result;
use crate::models::{Cell, CellType, ContentFormat, RelationType, Timestamp};

pub mod mariadb;
pub mod mysql;
pub mod postgres;
pub mod sqlite;
pub mod time;

pub use mariadb::MariaDbStorage;
pub use mysql::MySqlStorage;
pub use postgres::PostgresStorage;
pub use sqlite::SqliteStorage;

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct FabricRecord {
    pub id: Uuid,
    pub valid_from: Timestamp,
    pub valid_to: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct FabricMemberRow {
    pub fabric_id: Uuid,
    pub cell_id: Uuid,
    pub relation_type: RelationType,
    pub ordinal: Option<i64>,
}

pub trait Storage: Send + Sync {
    fn insert_fabric(&self, id: Uuid) -> Result<FabricRecord>;
    fn insert_cell(
        &self,
        id: Uuid,
        cell_type: &CellType,
        format: &ContentFormat,
        content: &[u8],
        fabric_id: Option<Uuid>,
    ) -> Result<Cell>;
    fn replace_cell(
        &self,
        id: Uuid,
        cell_type: &CellType,
        format: &ContentFormat,
        content: &[u8],
        fabric_id: Option<Uuid>,
    ) -> Result<Cell>;
    fn delete_cell(&self, id: Uuid) -> Result<()>;
    fn get_fabric(&self, id: Uuid) -> Result<FabricRecord>;
    fn get_fabric_at(&self, id: Uuid, ts: Timestamp) -> Result<FabricRecord>;
    fn list_fabrics(&self) -> Result<Vec<Uuid>>;
    fn get_cell(&self, id: Uuid) -> Result<Cell>;
    fn get_cell_at(&self, id: Uuid, ts: Timestamp) -> Result<Cell>;
    fn get_cell_history(&self, id: Uuid) -> Result<Vec<Cell>>;
    fn insert_fabric_cell(&self, fabric_cell: &FabricMemberRow) -> Result<()>;
    fn next_relation_ordinal(&self, fabric_id: Uuid, relation_type: &RelationType) -> Result<i64>;
    fn next_relation_ordinal_at(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
        ts: Timestamp,
    ) -> Result<i64>;
    fn get_cells_by_relation(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
    ) -> Result<Vec<Uuid>>;
    fn get_cells_by_relation_at(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
        ts: Timestamp,
    ) -> Result<Vec<Uuid>>;
    fn get_fabric_cells(&self, fabric_id: Uuid) -> Result<Vec<FabricMemberRow>>;
    fn get_fabric_cells_at(&self, fabric_id: Uuid, ts: Timestamp) -> Result<Vec<FabricMemberRow>>;
}
