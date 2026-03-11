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
use crate::models::{Cell, FabricCell, RelationType, Timestamp};

pub mod mariadb;
pub mod mysql;
pub mod postgres;
pub mod sqlite;
pub mod time;

pub use mariadb::MariaDbStorage;
pub use mysql::MySqlStorage;
pub use postgres::PostgresStorage;
pub use sqlite::SqliteStorage;

pub trait Storage: Send + Sync {
    fn current_query_timestamp(&self) -> Result<Timestamp>;

    fn active_valid_to_sentinel(&self) -> Timestamp;

    fn insert_cell(&self, cell: &Cell) -> Result<()>;

    fn reserve_next_version_timestamp(
        &self,
        id: Uuid,
        candidate_ts: Timestamp,
    ) -> Result<Timestamp>;

    fn get_cell_at(&self, id: Uuid, ts: Timestamp) -> Result<Cell>;

    fn close_active_version(&self, id: Uuid, now_ts: Timestamp) -> Result<()>;

    fn insert_fabric_cell(&self, fabric_cell: &FabricCell, now_ts: Timestamp) -> Result<()>;

    fn next_relation_ordinal(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
        ts: Timestamp,
    ) -> Result<i64>;

    fn get_cells_by_relation(
        &self,
        fabric_id: Uuid,
        relation_type: &RelationType,
        ts: Timestamp,
    ) -> Result<Vec<Uuid>>;

    fn get_fabric_cells(&self, fabric_id: Uuid, ts: Timestamp) -> Result<Vec<FabricCell>>;
}
