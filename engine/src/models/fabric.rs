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

use crate::models::cell::Cell;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RelationType {
    Contains,
    References,
    DerivesFrom,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FabricCell {
    pub fabric_id: Uuid,
    pub cell_id: Uuid,
    pub relation_type: RelationType,
    pub ordinal: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FabricContext {
    pub root: Cell,
    pub fabric_cells: Vec<FabricCell>,
    pub cells: Vec<Cell>,
}
