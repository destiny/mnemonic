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

use mnemonic_engine::{CellType, ContentFormat, Engine, RelationType};

#[test]
fn test_cell_lifecycle() {
    let engine = Engine::new(":memory:").unwrap();

    // Create a raw data cell
    let raw_cell = engine
        .create_cell(
            CellType::Raw,
            ContentFormat::Text,
            "Hello world!".as_bytes().to_vec(),
        )
        .unwrap();

    // Create a digested cell
    let digested_cell = engine
        .create_cell(
            CellType::Digested,
            ContentFormat::Text,
            "HELLO WORLD!".as_bytes().to_vec(),
        )
        .unwrap();

    // Create a container cell to group them
    let container = engine
        .create_cell(CellType::Container, ContentFormat::Json, vec![])
        .unwrap();

    // Add both to container
    engine.add_child(container.id, raw_cell.id).unwrap();
    engine.add_child(container.id, digested_cell.id).unwrap();

    // Fetch and verify legacy children list remains usable
    let fetched_container = engine.get_cell(container.id).unwrap();
    assert_eq!(fetched_container.children.len(), 2);
    assert_eq!(fetched_container.children[0], raw_cell.id);
    assert_eq!(fetched_container.children[1], digested_cell.id);

    // Verify relation/fabric-based links
    let contains_children = engine
        .get_children_by_relation(container.id, RelationType::Contains)
        .unwrap();
    assert_eq!(contains_children, vec![raw_cell.id, digested_cell.id]);

    // Add another semantic relationship
    engine
        .add_relation(
            digested_cell.id,
            raw_cell.id,
            RelationType::DerivesFrom,
            None,
        )
        .unwrap();
    let derived_from = engine
        .get_children_by_relation(digested_cell.id, RelationType::DerivesFrom)
        .unwrap();
    assert_eq!(derived_from, vec![raw_cell.id]);

    let fetched_raw = engine.get_cell(raw_cell.id).unwrap();
    assert_eq!(fetched_raw.content, "Hello world!".as_bytes());

    // Wait a bit to ensure SystemTime::now() advances
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Test versioning: update content
    let updated_raw = engine
        .update_cell_content(raw_cell.id, "Updated content".as_bytes().to_vec())
        .unwrap();

    // Fetching by ID should return the latest version
    let latest_raw = engine.get_cell(raw_cell.id).unwrap();
    assert_eq!(latest_raw.content, "Updated content".as_bytes());

    // Compare timestamps with millisecond precision
    let latest_valid_to_ms = latest_raw
        .valid_to
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let updated_valid_to_ms = updated_raw
        .valid_to
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    assert_eq!(latest_valid_to_ms, updated_valid_to_ms);
}
