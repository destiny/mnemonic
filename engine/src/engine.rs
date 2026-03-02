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

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use uuid::Uuid;

use crate::error::{EngineError, Result};
use crate::models::{
    Cell, CellType, ConflictStrategy, ContentFormat, FabricContext, FabricEdge, RelationType,
    VersionCandidate,
};
use crate::storage::{MariaDbStorage, MySqlStorage, PostgresStorage, SqliteStorage, Storage};

#[derive(Debug, Clone, Copy)]
pub struct EngineConfig {
    pub temporal_fabric_edges: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            temporal_fabric_edges: false,
        }
    }
}

pub struct Engine {
    storage: Box<dyn Storage>,
}

impl Engine {
    pub fn new(db_path: &str) -> Result<Self> {
        Self::with_config(db_path, EngineConfig::default())
    }

    pub fn with_config(db_path: &str, config: EngineConfig) -> Result<Self> {
        let storage = SqliteStorage::new(db_path, config.temporal_fabric_edges)?;
        Ok(Self {
            storage: Box::new(storage),
        })
    }

    pub fn from_storage(storage: Box<dyn Storage>) -> Self {
        Self { storage }
    }

    pub fn with_postgres_config(connection_str: &str, config: EngineConfig) -> Result<Self> {
        let storage = PostgresStorage::new(connection_str, config.temporal_fabric_edges)?;
        Ok(Self {
            storage: Box::new(storage),
        })
    }

    pub fn with_mysql_config(connection_str: &str, config: EngineConfig) -> Result<Self> {
        let storage = MySqlStorage::new(connection_str, config.temporal_fabric_edges)?;
        Ok(Self {
            storage: Box::new(storage),
        })
    }

    pub fn with_mariadb_config(connection_str: &str, config: EngineConfig) -> Result<Self> {
        let storage = MariaDbStorage::new(connection_str, config.temporal_fabric_edges)?;
        Ok(Self {
            storage: Box::new(storage),
        })
    }

    fn now_ts(&self) -> Result<i64> {
        self.storage.current_timestamp()
    }

    fn reserve_now_for_cell(&self, cell_id: Uuid) -> Result<i64> {
        let candidate = self.now_ts()?;
        let now = self.storage.reserve_update_timestamp(cell_id, candidate)?;
        if now > candidate {
            std::thread::sleep(Duration::from_micros((now - candidate) as u64));
        }
        Ok(now)
    }

    pub fn create_cell(
        &self,
        cell_type: CellType,
        format: ContentFormat,
        content: Vec<u8>,
    ) -> Result<Cell> {
        let now = self.now_ts()?;

        let cell = Cell {
            id: Uuid::now_v7(),
            cell_type,
            format,
            content,
            valid_from: now,
            valid_to: self.storage.open_ended_valid_to(),
            children: Vec::new(),
        };

        self.storage.insert_cell(&cell)?;

        Ok(cell)
    }

    pub fn update_cell_content(&self, cell_id: Uuid, new_content: Vec<u8>) -> Result<Cell> {
        let mut cell = self.get_current(cell_id)?;
        let now = self.reserve_now_for_cell(cell_id)?;

        self.storage.close_active_version(cell_id, now)?;
        cell.content = new_content;
        cell.valid_from = now;
        cell.valid_to = self.storage.open_ended_valid_to();
        self.storage.insert_cell(&cell)?;

        Ok(cell)
    }

    pub fn resolve_conflict_and_update(
        &self,
        cell_id: Uuid,
        local: VersionCandidate,
        remote: VersionCandidate,
        strategy: ConflictStrategy,
    ) -> Result<Cell> {
        let mut cell = self.get_current(cell_id)?;

        let winner = match strategy {
            ConflictStrategy::LastWriteWins => {
                if local.timestamp >= remote.timestamp {
                    local
                } else {
                    remote
                }
            }
            ConflictStrategy::LogicalClock => {
                let local_clock = local.logical_clock.unwrap_or(0);
                let remote_clock = remote.logical_clock.unwrap_or(0);
                if local_clock > remote_clock {
                    local
                } else if remote_clock > local_clock {
                    remote
                } else if local.timestamp >= remote.timestamp {
                    local
                } else {
                    remote
                }
            }
        };

        if winner.content.is_empty() {
            return Err(EngineError::Conflict(
                "resolved winner has empty content".to_string(),
            ));
        }

        let now = self.reserve_now_for_cell(cell_id)?;
        self.storage.close_active_version(cell_id, now)?;

        cell.content = winner.content;
        cell.valid_from = now;
        cell.valid_to = self.storage.open_ended_valid_to();
        self.storage.insert_cell(&cell)?;

        Ok(cell)
    }

    pub fn add_child(&self, parent_id: Uuid, child_id: Uuid) -> Result<()> {
        let mut parent = self.get_current(parent_id)?;
        parent.children.push(child_id);
        self.update_cell(&parent)?;

        self.add_relation(
            parent_id,
            child_id,
            RelationType::Contains,
            Some(parent.children.len() as i64 - 1),
        )?;

        Ok(())
    }

    pub fn add_relation(
        &self,
        parent_id: Uuid,
        child_id: Uuid,
        relation_type: RelationType,
        ordinal: Option<i64>,
    ) -> Result<()> {
        let resolved_ordinal = match ordinal {
            Some(value) => value,
            None => self
                .storage
                .next_relation_ordinal(parent_id, &relation_type)?,
        };

        let edge = FabricEdge {
            parent_id,
            child_id,
            relation_type,
            ordinal: resolved_ordinal,
        };

        let now = self.now_ts()?;
        self.storage.insert_edge(&edge, now)?;

        Ok(())
    }

    pub fn get_children_by_relation(
        &self,
        parent_id: Uuid,
        relation_type: RelationType,
    ) -> Result<Vec<Uuid>> {
        self.storage
            .get_children_by_relation(parent_id, &relation_type, self.now_ts()?)
    }

    pub fn update_cell(&self, cell: &Cell) -> Result<()> {
        let now = self.reserve_now_for_cell(cell.id)?;

        self.storage.close_active_version(cell.id, now)?;
        let next = Cell {
            id: cell.id,
            cell_type: cell.cell_type.clone(),
            format: cell.format.clone(),
            content: cell.content.clone(),
            valid_from: now,
            valid_to: self.storage.open_ended_valid_to(),
            children: cell.children.clone(),
        };

        self.storage.insert_cell(&next)?;

        Ok(())
    }

    pub fn get_current(&self, id: Uuid) -> Result<Cell> {
        self.storage.get_cell_at(id, self.now_ts()?)
    }

    pub fn get_at_time(&self, id: Uuid, timestamp: i64) -> Result<Cell> {
        self.storage.get_cell_at(id, timestamp)
    }

    pub fn build_context(&self, fabric_id: Uuid) -> Result<FabricContext> {
        self.build_context_at_time(fabric_id, self.now_ts()?)
    }

    pub fn build_context_at_time(&self, fabric_id: Uuid, timestamp: i64) -> Result<FabricContext> {
        let root = self.get_at_time(fabric_id, timestamp)?;

        let mut seen: HashSet<Uuid> = HashSet::new();
        let mut queue = VecDeque::new();
        let mut edges = Vec::new();
        let mut cells = Vec::new();

        seen.insert(root.id);
        cells.push(root.clone());
        queue.push_back(root.id);

        while let Some(parent_id) = queue.pop_front() {
            let parent_edges = self.storage.get_edges_for_parent(parent_id, timestamp)?;
            for edge in parent_edges {
                if seen.insert(edge.child_id) {
                    let child = self.get_at_time(edge.child_id, timestamp)?;
                    cells.push(child);
                    queue.push_back(edge.child_id);
                }
                edges.push(edge);
            }
        }

        Ok(FabricContext { root, edges, cells })
    }

    pub fn get_cell(&self, id: Uuid) -> Result<Cell> {
        self.get_current(id)
    }
}
