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

use std::time::{SystemTime, UNIX_EPOCH};

use mnemonic_engine::{
    CellType, ConflictStrategy, ContentFormat, Engine, EngineConfig, MAX_TIME, RelationType,
    VersionCandidate,
};

// Black-box specification baseline derived from AGENTS.md.
const SPEC_RULES: &[&str] = &[
    "Open-ended active rows use valid_to = MAX_TIME",
    "Temporal validity window is valid_from <= T < valid_to",
    "Update algorithm: close active row, then insert new active row",
    "Current reads are time-window based, not hard-coded to MAX_TIME",
    "Fabric context reconstruction is deterministic at any timestamp",
    "Conflicts are resolved before promoting a new active version",
];

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

#[test]
fn spec_sanity_rules_exist() {
    assert_eq!(SPEC_RULES.len(), 6);
}

#[test]
fn spec_cell_uses_max_time_and_current_lookup() {
    let engine = Engine::new(":memory:").unwrap();
    let cell = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"hello".to_vec())
        .unwrap();

    assert_eq!(cell.valid_to, MAX_TIME);

    let current = engine.get_current(cell.id).unwrap();
    assert_eq!(current.content, b"hello".to_vec());
    assert_eq!(current.valid_to, MAX_TIME);
}

#[test]
fn spec_temporal_window_boundary_on_update() {
    let engine = Engine::new(":memory:").unwrap();
    let cell = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"v1".to_vec())
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(2));
    let before_update = now_ts();

    let updated = engine.update_cell_content(cell.id, b"v2".to_vec()).unwrap();

    // AGENTS window semantics: valid_from <= T < valid_to.
    let old_at_before = engine.get_at_time(cell.id, before_update).unwrap();
    assert_eq!(old_at_before.content, b"v1".to_vec());

    let at_boundary = engine.get_at_time(cell.id, updated.valid_from).unwrap();
    assert_eq!(at_boundary.content, b"v2".to_vec());
    assert_eq!(at_boundary.valid_to, MAX_TIME);
}

#[test]
fn spec_conflict_resolution_deterministic() {
    let engine = Engine::new(":memory:").unwrap();
    let cell = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"base".to_vec())
        .unwrap();

    let local = VersionCandidate {
        content: b"local".to_vec(),
        timestamp: 100,
        logical_clock: Some(3),
    };
    let remote = VersionCandidate {
        content: b"remote".to_vec(),
        timestamp: 200,
        logical_clock: Some(2),
    };

    let lww = engine
        .resolve_conflict_and_update(
            cell.id,
            local.clone(),
            remote.clone(),
            ConflictStrategy::LastWriteWins,
        )
        .unwrap();
    assert_eq!(lww.content, b"remote".to_vec());

    let logical = engine
        .resolve_conflict_and_update(cell.id, local, remote, ConflictStrategy::LogicalClock)
        .unwrap();
    assert_eq!(logical.content, b"local".to_vec());
}

#[test]
fn spec_temporal_fabric_edges_when_enabled() {
    let engine = Engine::with_config(
        ":memory:",
        EngineConfig {
            temporal_fabric_edges: true,
        },
    )
    .unwrap();

    let parent = engine
        .create_cell(CellType::Container, ContentFormat::Json, vec![])
        .unwrap();
    let first = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"first".to_vec())
        .unwrap();
    let second = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"second".to_vec())
        .unwrap();

    engine
        .add_relation(parent.id, first.id, RelationType::Contains, Some(0))
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(2));
    let t_before_switch = now_ts();
    std::thread::sleep(std::time::Duration::from_millis(2));

    engine
        .add_relation(parent.id, second.id, RelationType::Contains, Some(0))
        .unwrap();

    let historical = engine
        .build_context_at_time(parent.id, t_before_switch)
        .unwrap();
    assert!(historical.edges.iter().any(|e| e.child_id == first.id));

    let current = engine.build_context(parent.id).unwrap();
    assert!(current.edges.iter().any(|e| e.child_id == second.id));
}

#[test]
fn e2e_lifecycle_black_box_acceptance() {
    let engine = Engine::new(":memory:").unwrap();

    let raw = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"Hello world!".to_vec())
        .unwrap();
    let digested = engine
        .create_cell(
            CellType::Digested,
            ContentFormat::Text,
            b"HELLO WORLD!".to_vec(),
        )
        .unwrap();
    let container = engine
        .create_cell(CellType::Container, ContentFormat::Json, vec![])
        .unwrap();

    engine.add_child(container.id, raw.id).unwrap();
    engine.add_child(container.id, digested.id).unwrap();
    engine
        .add_relation(digested.id, raw.id, RelationType::DerivesFrom, None)
        .unwrap();

    let contains = engine
        .get_children_by_relation(container.id, RelationType::Contains)
        .unwrap();
    assert_eq!(contains, vec![raw.id, digested.id]);

    let before_update = now_ts();
    std::thread::sleep(std::time::Duration::from_millis(2));

    engine
        .update_cell_content(raw.id, b"Updated content".to_vec())
        .unwrap();

    let current_raw = engine.get_current(raw.id).unwrap();
    let historical_raw = engine.get_at_time(raw.id, before_update).unwrap();
    assert_eq!(current_raw.content, b"Updated content".to_vec());
    assert_eq!(historical_raw.content, b"Hello world!".to_vec());

    let context_now = engine.build_context(container.id).unwrap();
    assert_eq!(context_now.root.id, container.id);
    assert!(context_now.cells.iter().any(|c| c.id == raw.id));
    assert!(context_now.cells.iter().any(|c| c.id == digested.id));
}
