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

use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cell {
    pub id: Uuid,
    pub cell_type: CellType,
    pub format: ContentFormat,
    pub content: Vec<u8>, // Use Vec<u8> to store both text and binary data
    pub valid_from: SystemTime,
    pub valid_to: SystemTime,
    pub children: Vec<Uuid>, // Maintains sequence of child cells
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RelationType {
    Contains,
    References,
    DerivesFrom,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FabricEdge {
    pub parent_id: Uuid,
    pub child_id: Uuid,
    pub relation_type: RelationType,
    pub ordinal: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CellType {
    Meta,
    Data,
    Raw,      // Dedicated type for raw content
    Digested, // Dedicated type for digested/processed content
    Container,
    Tag,
    Keyword,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ContentFormat {
    Text,
    Binary,
    Json,
    Markdown,
    Other(String),
}
