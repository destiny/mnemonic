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

use crate::models::{Cell, CellType, ContentFormat};
use crate::storage::SqliteStorage;
use crate::error::Result;
use std::time::SystemTime;
use uuid::Uuid;
pub struct Engine {
    storage: SqliteStorage,
}

impl Engine {
    pub fn new(db_path: &str) -> Result<Self> {
        let storage = SqliteStorage::new(db_path)?;
        Ok(Self { storage })
    }

    pub fn create_cell(
        &self,
        cell_type: CellType,
        format: ContentFormat,
        content: Vec<u8>,
    ) -> Result<Cell> {
        let now = SystemTime::now();
        // Set valid_to to a very far future date (approx. 100 years from now)
        let far_future = now + std::time::Duration::from_secs(100 * 365 * 24 * 3600);
        
        let cell = Cell {
            id: Uuid::now_v7(),
            cell_type,
            format,
            content,
            valid_from: now,
            valid_to: far_future,
            children: Vec::new(),
        };
        self.storage.insert_cell(&cell)?;
        Ok(cell)
    }

    /// Creates a new version of an existing cell by updating its valid_from and valid_to timestamps.
    pub fn update_cell_content(&self, cell_id: Uuid, new_content: Vec<u8>) -> Result<Cell> {
        let mut cell = self.storage.get_cell(cell_id)?;
        let now = SystemTime::now();
        
        // 1. Update the old version's valid_to to 'now'
        let mut old_version = cell.clone();
        old_version.valid_to = now;
        self.storage.insert_cell_with_replace(&old_version)?;
        
        // Ensure new version's valid_to is different from old one's valid_to if 'now' happened to be the same.
        // But since we are creating a new version with a far-future valid_to, it should be fine.
        
        // 2. Create the new version with same ID starting from 'now'
        cell.content = new_content;
        cell.valid_from = now;
        let far_future = now + std::time::Duration::from_secs(100 * 365 * 24 * 3600);
        cell.valid_to = far_future;
        
        self.storage.insert_cell(&cell)?;
        Ok(cell)
    }

    pub fn add_child(&self, parent_id: Uuid, child_id: Uuid) -> Result<()> {
        let mut parent = self.storage.get_cell(parent_id)?;
        parent.children.push(child_id);
        // We need an update method in storage, or just re-insert if we handle conflicts
        // For now let's assume we need an update method.
        self.update_cell(&parent)?;
        Ok(())
    }

    pub fn update_cell(&self, cell: &Cell) -> Result<()> {
        // We use insert_cell_with_replace because (id, valid_to) is the PK.
        // If valid_to changed, it will be a new row. If not, it will replace.
        self.storage.insert_cell_with_replace(cell)?;
        Ok(())
    }

    pub fn get_cell(&self, id: Uuid) -> Result<Cell> {
        self.storage.get_cell(id)
    }
}
