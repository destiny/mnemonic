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
use crate::models::{Cell, FabricEdge, RelationType};

pub mod mariadb;
pub mod mysql;
pub mod postgres;
pub mod sqlite;

pub use mariadb::MariaDbStorage;
pub use mysql::MySqlStorage;
pub use postgres::PostgresStorage;
pub use sqlite::SqliteStorage;

pub trait Storage {
    fn current_timestamp(&self) -> Result<i64>;

    fn open_ended_valid_to(&self) -> i64;

    fn insert_cell(&self, cell: &Cell) -> Result<()>;

    fn reserve_update_timestamp(&self, id: Uuid, candidate_ts: i64) -> Result<i64>;

    fn get_cell_at(&self, id: Uuid, ts: i64) -> Result<Cell>;

    fn close_active_version(&self, id: Uuid, now_ts: i64) -> Result<()>;

    fn insert_edge(&self, edge: &FabricEdge, now_ts: i64) -> Result<()>;

    fn next_relation_ordinal(&self, parent_id: Uuid, relation_type: &RelationType) -> Result<i64>;

    fn get_children_by_relation(
        &self,
        parent_id: Uuid,
        relation_type: &RelationType,
        ts: i64,
    ) -> Result<Vec<Uuid>>;

    fn get_edges_for_parent(&self, parent_id: Uuid, ts: i64) -> Result<Vec<FabricEdge>>;
}
