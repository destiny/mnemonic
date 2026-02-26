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

use crate::error::Result;
use crate::models::{Cell, CellType, ContentFormat, FabricEdge, RelationType};
use crate::storage::SqliteStorage;
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
        self.storage.insert_edge(&edge)
    }

    pub fn get_children_by_relation(
        &self,
        parent_id: Uuid,
        relation_type: RelationType,
    ) -> Result<Vec<Uuid>> {
        self.storage
            .get_children_by_relation(parent_id, &relation_type)
    }

    pub fn update_cell(&self, cell: &Cell) -> Result<()> {
        self.storage.insert_cell_with_replace(cell)?;
        Ok(())
    }

    pub fn get_cell(&self, id: Uuid) -> Result<Cell> {
        self.storage.get_cell(id)
    }
}
