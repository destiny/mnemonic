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

use uuid::Uuid;

use crate::error::{EngineError, Result};
use crate::models::{Cell, CellType, ContentFormat, Fabric, FabricCell, RelationType, Timestamp};
use crate::storage::{
    FabricMemberRow, MariaDbStorage, MySqlStorage, PostgresStorage, SqliteStorage, Storage,
};

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
    fabric_cache: RefCell<HashMap<Uuid, Fabric>>,
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
        self.engine.refresh_fabric_cache(self.root_id)?;
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
            fabric_cache: RefCell::new(HashMap::new()),
        })
    }

    pub fn from_storage(storage: Box<dyn Storage>) -> Self {
        Self {
            storage,
            fabric_cache: RefCell::new(HashMap::new()),
        }
    }

    pub fn with_postgres_config(connection_str: &str, config: EngineConfig) -> Result<Self> {
        let storage = PostgresStorage::new(connection_str, config.temporal_fabric_cells)?;
        Ok(Self {
            storage: Box::new(storage),
            fabric_cache: RefCell::new(HashMap::new()),
        })
    }

    pub fn with_mysql_config(connection_str: &str, config: EngineConfig) -> Result<Self> {
        let storage = MySqlStorage::new(connection_str, config.temporal_fabric_cells)?;
        Ok(Self {
            storage: Box::new(storage),
            fabric_cache: RefCell::new(HashMap::new()),
        })
    }

    pub fn with_mariadb_config(connection_str: &str, config: EngineConfig) -> Result<Self> {
        let storage = MariaDbStorage::new(connection_str, config.temporal_fabric_cells)?;
        Ok(Self {
            storage: Box::new(storage),
            fabric_cache: RefCell::new(HashMap::new()),
        })
    }

    fn clear_fabric_cache(&self) {
        self.fabric_cache.borrow_mut().clear();
    }

    fn refresh_fabric_cache(&self, fabric_id: Uuid) -> Result<()> {
        let fabric = self.build_fabric_for_timestamp(fabric_id, None)?;
        self.fabric_cache.borrow_mut().insert(fabric_id, fabric);
        Ok(())
    }

    fn require_cell_fabric(&self, cell_id: Uuid) -> Result<Uuid> {
        let cell = self.get_cell(cell_id)?;
        cell.fabric_id.ok_or_else(|| {
            EngineError::InvalidData(format!(
                "cell {cell_id} does not reference a fabric; create it with fabric ownership before adding fabric members"
            ))
        })
    }

    pub fn create_cell(
        &self,
        cell_type: CellType,
        format: ContentFormat,
        content: Vec<u8>,
    ) -> Result<Cell> {
        let id = Uuid::now_v7();
        let cell = self
            .storage
            .insert_cell(id, &cell_type, &format, &content, None)?;
        self.clear_fabric_cache();

        Ok(cell)
    }

    pub fn create_cell_with_fabric(
        &self,
        cell_type: CellType,
        format: ContentFormat,
        content: Vec<u8>,
    ) -> Result<Cell> {
        let id = Uuid::now_v7();
        let cell = self
            .storage
            .insert_cell(id, &cell_type, &format, &content, Some(id))?;
        self.clear_fabric_cache();

        Ok(cell)
    }

    pub fn update_cell_content(&self, cell_id: Uuid, new_content: Vec<u8>) -> Result<Cell> {
        let cell = self.get_cell(cell_id)?;
        let cell = self.storage.replace_cell(
            cell.id,
            &cell.cell_type,
            &cell.format,
            &new_content,
            cell.fabric_id,
        )?;
        self.clear_fabric_cache();

        Ok(cell)
    }

    pub fn add_fabric_cell(
        &self,
        root_cell_id: Uuid,
        cell_id: Uuid,
        relation_type: RelationType,
        ordinal: Option<i64>,
    ) -> Result<()> {
        let fabric_id = self.require_cell_fabric(root_cell_id)?;
        let resolved_ordinal = match ordinal {
            Some(value) => value,
            None => self
                .storage
                .next_relation_ordinal(fabric_id, &relation_type)?,
        };

        let fabric_cell = FabricMemberRow {
            fabric_id,
            cell_id,
            relation_type,
            ordinal: Some(resolved_ordinal),
        };

        self.storage.insert_fabric_cell(&fabric_cell)?;
        self.clear_fabric_cache();

        Ok(())
    }

    pub fn get_cells_by_relation(
        &self,
        root_cell_id: Uuid,
        relation_type: RelationType,
    ) -> Result<Vec<Uuid>> {
        let root = self.get_cell(root_cell_id)?;
        let Some(fabric_id) = root.fabric_id else {
            return Ok(Vec::new());
        };

        self.storage
            .get_cells_by_relation(fabric_id, &relation_type)
    }

    pub fn update_cell(&self, cell: &Cell) -> Result<()> {
        self.update_cell_and_refresh(cell)?;
        Ok(())
    }

    fn update_cell_and_refresh(&self, cell: &Cell) -> Result<Cell> {
        let next = self.storage.replace_cell(
            cell.id,
            &cell.cell_type,
            &cell.format,
            &cell.content,
            cell.fabric_id,
        )?;
        self.clear_fabric_cache();

        Ok(next)
    }

    pub fn open_document_context(
        &self,
        root_cell_id: Uuid,
        timestamp: Option<Timestamp>,
    ) -> Result<DocumentContext<'_>> {
        let fabric = self.build_fabric_for_timestamp(root_cell_id, timestamp)?;
        let mut cells = HashMap::new();
        for fabric_cell in fabric.cells {
            cells.insert(fabric_cell.cell.id, fabric_cell.cell);
        }

        Ok(DocumentContext {
            engine: self,
            root_id: root_cell_id,
            cells,
            dirty_cells: HashSet::new(),
        })
    }

    pub fn get_cell(&self, id: Uuid) -> Result<Cell> {
        self.storage.get_cell(id)
    }

    pub fn list_fabrics(&self) -> Result<Vec<Uuid>> {
        self.storage.list_fabrics()
    }

    pub fn get_cell_at(&self, id: Uuid, timestamp: Timestamp) -> Result<Cell> {
        self.storage.get_cell_at(id, timestamp)
    }

    pub fn get_cell_history(&self, id: Uuid) -> Result<Vec<Cell>> {
        self.storage.get_cell_history(id)
    }

    pub fn delete_cell(&self, id: Uuid) -> Result<()> {
        self.storage.delete_cell(id)?;
        self.clear_fabric_cache();
        Ok(())
    }

    pub fn build_fabric(&self, root_cell_id: Uuid) -> Result<Fabric> {
        self.build_fabric_for_timestamp(root_cell_id, None)
    }

    pub fn build_fabric_at_time(&self, root_cell_id: Uuid, timestamp: Timestamp) -> Result<Fabric> {
        self.build_fabric_for_timestamp(root_cell_id, Some(timestamp))
    }

    fn build_fabric_for_timestamp(
        &self,
        root_cell_id: Uuid,
        timestamp: Option<Timestamp>,
    ) -> Result<Fabric> {
        if timestamp.is_none() {
            if let Some(cached) = self.fabric_cache.borrow().get(&root_cell_id).cloned() {
                return Ok(cached);
            }
        }

        let root = match timestamp {
            Some(value) => self.get_cell_at(root_cell_id, value)?,
            None => self.get_cell(root_cell_id)?,
        };
        let fabric_id = root.fabric_id.ok_or_else(|| {
            EngineError::InvalidData(format!(
                "cell {root_cell_id} does not reference a fabric; build_fabric requires a fabric-backed root cell"
            ))
        })?;
        match timestamp {
            Some(value) => {
                self.storage.get_fabric_at(fabric_id, value)?;
            }
            None => {
                self.storage.get_fabric(fabric_id)?;
            }
        }

        let mut seen: HashSet<Uuid> = HashSet::new();
        let mut queue = VecDeque::new();
        let mut fabric_cells = Vec::new();

        seen.insert(root.id);
        queue.push_back(root.id);

        while let Some(cell_id) = queue.pop_front() {
            let current = match timestamp {
                Some(value) => self.get_cell_at(cell_id, value)?,
                None => self.get_cell(cell_id)?,
            };
            let Some(fabric_id) = current.fabric_id else {
                continue;
            };

            let current_fabric_cells = match timestamp {
                Some(value) => self.storage.get_fabric_cells_at(fabric_id, value)?,
                None => self.storage.get_fabric_cells(fabric_id)?,
            };
            for fabric_cell in current_fabric_cells {
                let resolved = match timestamp {
                    Some(value) => self.get_cell_at(fabric_cell.cell_id, value)?,
                    None => self.get_cell(fabric_cell.cell_id)?,
                };

                if seen.insert(fabric_cell.cell_id) {
                    let child = match timestamp {
                        Some(value) => self.get_cell_at(fabric_cell.cell_id, value)?,
                        None => self.get_cell(fabric_cell.cell_id)?,
                    };
                    if child.fabric_id.is_some() {
                        queue.push_back(fabric_cell.cell_id);
                    }
                }
                fabric_cells.push(FabricCell {
                    cell: resolved,
                    relation_type: fabric_cell.relation_type,
                    ordinal: fabric_cell.ordinal,
                });
            }
        }

        let fabric = Fabric {
            id: fabric_id,
            cells: fabric_cells,
        };
        if timestamp.is_none() {
            self.fabric_cache
                .borrow_mut()
                .insert(root_cell_id, fabric.clone());
        }

        Ok(fabric)
    }
}
