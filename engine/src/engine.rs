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

use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use uuid::Uuid;

use crate::error::{EngineError, Result};
use crate::models::{
    Cell, CellType, ConflictStrategy, ContentFormat, FabricCell, FabricContext, RelationType,
    Timestamp, VersionCandidate,
};
use crate::storage::{MariaDbStorage, MySqlStorage, PostgresStorage, SqliteStorage, Storage};

#[derive(Debug, Clone, Copy)]
pub struct EngineConfig {
    pub temporal_fabric_cells: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            temporal_fabric_cells: false,
        }
    }
}

pub struct Engine {
    storage: Box<dyn Storage>,
    context_cache: RefCell<HashMap<Uuid, FabricContext>>,
}

pub struct DocumentContext<'a> {
    engine: &'a Engine,
    root_id: Uuid,
    cells: HashMap<Uuid, Cell>,
    dirty_cells: HashSet<Uuid>,
}

impl<'a> DocumentContext<'a> {
    pub fn root_id(&self) -> Uuid {
        self.root_id
    }

    pub fn get_cell(&self, id: Uuid) -> Option<&Cell> {
        self.cells.get(&id)
    }

    pub fn update_cell_content(&mut self, cell_id: Uuid, new_content: Vec<u8>) -> Result<()> {
        let Some(cell) = self.cells.get_mut(&cell_id) else {
            return Err(EngineError::NotFound);
        };

        cell.content = new_content;
        self.dirty_cells.insert(cell_id);
        Ok(())
    }

    pub fn save(&mut self) -> Result<()> {
        for cell_id in self.dirty_cells.clone() {
            let cell = self
                .cells
                .get(&cell_id)
                .cloned()
                .ok_or(EngineError::NotFound)?;
            let saved = self.engine.update_cell_and_refresh(&cell)?;
            self.cells.insert(cell_id, saved);
        }

        self.dirty_cells.clear();
        self.engine.refresh_context_cache(self.root_id)?;
        Ok(())
    }
}

impl Engine {
    pub fn new(db_path: &str) -> Result<Self> {
        Self::with_config(db_path, EngineConfig::default())
    }

    pub fn with_config(db_path: &str, config: EngineConfig) -> Result<Self> {
        let storage = SqliteStorage::new(db_path, config.temporal_fabric_cells)?;
        Ok(Self {
            storage: Box::new(storage),
            context_cache: RefCell::new(HashMap::new()),
        })
    }

    pub fn from_storage(storage: Box<dyn Storage>) -> Self {
        Self {
            storage,
            context_cache: RefCell::new(HashMap::new()),
        }
    }

    pub fn with_postgres_config(connection_str: &str, config: EngineConfig) -> Result<Self> {
        let storage = PostgresStorage::new(connection_str, config.temporal_fabric_cells)?;
        Ok(Self {
            storage: Box::new(storage),
            context_cache: RefCell::new(HashMap::new()),
        })
    }

    pub fn with_mysql_config(connection_str: &str, config: EngineConfig) -> Result<Self> {
        let storage = MySqlStorage::new(connection_str, config.temporal_fabric_cells)?;
        Ok(Self {
            storage: Box::new(storage),
            context_cache: RefCell::new(HashMap::new()),
        })
    }

    pub fn with_mariadb_config(connection_str: &str, config: EngineConfig) -> Result<Self> {
        let storage = MariaDbStorage::new(connection_str, config.temporal_fabric_cells)?;
        Ok(Self {
            storage: Box::new(storage),
            context_cache: RefCell::new(HashMap::new()),
        })
    }

    fn clear_context_cache(&self) {
        self.context_cache.borrow_mut().clear();
    }

    fn refresh_context_cache(&self, fabric_id: Uuid) -> Result<()> {
        let context = self.build_context_with_timestamp(fabric_id, None)?;
        self.context_cache.borrow_mut().insert(fabric_id, context);
        Ok(())
    }

    fn now_ts(&self) -> Result<Timestamp> {
        self.storage.current_query_timestamp()
    }

    fn reserve_next_version_ts(&self, cell_id: Uuid) -> Result<Timestamp> {
        let candidate = self.now_ts()?;
        let now = self
            .storage
            .reserve_next_version_timestamp(cell_id, candidate)?;
        let mut observed = candidate;
        while observed < now {
            let remaining = now.signed_duration_since(observed);
            let micros = remaining.num_microseconds().unwrap_or(0).max(0) as u64;
            if micros == 0 {
                break;
            }
            std::thread::sleep(Duration::from_micros(micros.min(100)));
            observed = self.now_ts()?;
        }
        Ok(now)
    }

    fn default_fabric_id_for_cell_type(cell_id: Uuid, cell_type: &CellType) -> Option<Uuid> {
        match cell_type {
            CellType::Container => Some(cell_id),
            CellType::Custom(value) if value.starts_with("document.") => Some(cell_id),
            _ => None,
        }
    }

    fn ensure_cell_fabric(&self, cell_id: Uuid) -> Result<Uuid> {
        let cell = self.get_current(cell_id)?;
        if let Some(fabric_id) = cell.fabric_id {
            return Ok(fabric_id);
        }

        let now = self.reserve_next_version_ts(cell_id)?;
        self.storage.close_active_version(cell_id, now)?;

        let next = Cell {
            id: cell.id,
            cell_type: cell.cell_type,
            format: cell.format,
            content: cell.content,
            valid_from: now,
            valid_to: self.storage.active_valid_to_sentinel(),
            fabric_id: Some(cell.id),
        };

        self.storage.insert_cell(&next)?;
        self.clear_context_cache();

        Ok(next.fabric_id.expect("fabric id assigned"))
    }

    pub fn create_cell(
        &self,
        cell_type: CellType,
        format: ContentFormat,
        content: Vec<u8>,
    ) -> Result<Cell> {
        let now = self.now_ts()?;
        let id = Uuid::now_v7();
        let fabric_id = Self::default_fabric_id_for_cell_type(id, &cell_type);

        let cell = Cell {
            id,
            cell_type,
            format,
            content,
            valid_from: now,
            valid_to: self.storage.active_valid_to_sentinel(),
            fabric_id,
        };

        self.storage.insert_cell(&cell)?;
        self.clear_context_cache();

        Ok(cell)
    }

    pub fn update_cell_content(&self, cell_id: Uuid, new_content: Vec<u8>) -> Result<Cell> {
        let mut cell = self.get_current(cell_id)?;
        let now = self.reserve_next_version_ts(cell_id)?;

        self.storage.close_active_version(cell_id, now)?;
        cell.content = new_content;
        cell.valid_from = now;
        cell.valid_to = self.storage.active_valid_to_sentinel();
        self.storage.insert_cell(&cell)?;
        self.clear_context_cache();

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

        let now = self.reserve_next_version_ts(cell_id)?;
        self.storage.close_active_version(cell_id, now)?;

        cell.content = winner.content;
        cell.valid_from = now;
        cell.valid_to = self.storage.active_valid_to_sentinel();
        self.storage.insert_cell(&cell)?;
        self.clear_context_cache();

        Ok(cell)
    }

    pub fn add_fabric_cell(
        &self,
        root_cell_id: Uuid,
        cell_id: Uuid,
        relation_type: RelationType,
        ordinal: Option<i64>,
    ) -> Result<()> {
        let fabric_id = self.ensure_cell_fabric(root_cell_id)?;
        let now = self.now_ts()?;
        let resolved_ordinal = match ordinal {
            Some(value) => value,
            None => self
                .storage
                .next_relation_ordinal(fabric_id, &relation_type, now)?,
        };

        let fabric_cell = FabricCell {
            fabric_id,
            cell_id,
            relation_type,
            ordinal: resolved_ordinal,
        };

        self.storage.insert_fabric_cell(&fabric_cell, now)?;
        self.clear_context_cache();

        Ok(())
    }

    pub fn get_cells_by_relation(
        &self,
        root_cell_id: Uuid,
        relation_type: RelationType,
    ) -> Result<Vec<Uuid>> {
        let root = self.get_current(root_cell_id)?;
        let Some(fabric_id) = root.fabric_id else {
            return Ok(Vec::new());
        };

        self.storage
            .get_cells_by_relation(fabric_id, &relation_type, self.now_ts()?)
    }

    pub fn update_cell(&self, cell: &Cell) -> Result<()> {
        self.update_cell_and_refresh(cell)?;
        Ok(())
    }

    fn update_cell_and_refresh(&self, cell: &Cell) -> Result<Cell> {
        let now = self.reserve_next_version_ts(cell.id)?;

        self.storage.close_active_version(cell.id, now)?;
        let next = Cell {
            id: cell.id,
            cell_type: cell.cell_type.clone(),
            format: cell.format.clone(),
            content: cell.content.clone(),
            valid_from: now,
            valid_to: self.storage.active_valid_to_sentinel(),
            fabric_id: cell.fabric_id,
        };

        self.storage.insert_cell(&next)?;
        self.clear_context_cache();

        Ok(next)
    }

    pub fn open_document_context(
        &self,
        root_cell_id: Uuid,
        timestamp: Option<Timestamp>,
    ) -> Result<DocumentContext<'_>> {
        let context = self.build_context_with_timestamp(root_cell_id, timestamp)?;
        let mut cells = HashMap::new();
        for cell in context.cells {
            cells.insert(cell.id, cell);
        }

        Ok(DocumentContext {
            engine: self,
            root_id: context.root.id,
            cells,
            dirty_cells: HashSet::new(),
        })
    }

    pub fn get_cell_at(&self, id: Uuid, timestamp: Option<Timestamp>) -> Result<Cell> {
        let resolved_ts = match timestamp {
            Some(value) => value,
            None => self.now_ts()?,
        };

        self.storage.get_cell_at(id, resolved_ts)
    }

    pub fn get_current(&self, id: Uuid) -> Result<Cell> {
        self.get_cell_at(id, None)
    }

    pub fn get_at_time(&self, id: Uuid, timestamp: Timestamp) -> Result<Cell> {
        self.get_cell_at(id, Some(timestamp))
    }

    pub fn build_context(&self, root_cell_id: Uuid) -> Result<FabricContext> {
        self.build_context_with_timestamp(root_cell_id, None)
    }

    pub fn build_context_at_time(
        &self,
        root_cell_id: Uuid,
        timestamp: Timestamp,
    ) -> Result<FabricContext> {
        self.build_context_with_timestamp(root_cell_id, Some(timestamp))
    }

    pub fn build_context_with_timestamp(
        &self,
        root_cell_id: Uuid,
        timestamp: Option<Timestamp>,
    ) -> Result<FabricContext> {
        if timestamp.is_none() {
            if let Some(cached) = self.context_cache.borrow().get(&root_cell_id).cloned() {
                return Ok(cached);
            }
        }

        let resolved_ts = match timestamp {
            Some(value) => value,
            None => self.now_ts()?,
        };

        let root = self.get_at_time(root_cell_id, resolved_ts)?;

        let mut seen: HashSet<Uuid> = HashSet::new();
        let mut queue = VecDeque::new();
        let mut fabric_cells = Vec::new();
        let mut cells = Vec::new();

        seen.insert(root.id);
        cells.push(root.clone());
        queue.push_back(root.id);

        while let Some(cell_id) = queue.pop_front() {
            let current = self.get_at_time(cell_id, resolved_ts)?;
            let Some(fabric_id) = current.fabric_id else {
                continue;
            };

            let current_fabric_cells = self.storage.get_fabric_cells(fabric_id, resolved_ts)?;
            for fabric_cell in current_fabric_cells {
                if seen.insert(fabric_cell.cell_id) {
                    let child = self.get_at_time(fabric_cell.cell_id, resolved_ts)?;
                    cells.push(child);
                    queue.push_back(fabric_cell.cell_id);
                }
                fabric_cells.push(fabric_cell);
            }
        }

        let context = FabricContext {
            root,
            fabric_cells,
            cells,
        };
        if timestamp.is_none() {
            self.context_cache
                .borrow_mut()
                .insert(root_cell_id, context.clone());
        }

        Ok(context)
    }

    pub fn get_cell(&self, id: Uuid) -> Result<Cell> {
        self.get_current(id)
    }
}
