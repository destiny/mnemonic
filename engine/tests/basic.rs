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

use chrono::Utc;
use rusqlite::Connection;

use mnemonic_engine::{CellType, ContentFormat, Engine, EngineConfig, RelationType, Timestamp};

// Black-box specification baseline derived from AGENTS.md.
const SPEC_RULES: &[&str] = &[
    "Open-ended active rows are selected via time-window semantics",
    "Temporal validity window is valid_from <= T < valid_to",
    "Update algorithm: close active row, then insert new active row",
    "Current reads are time-window based, not hard-coded to a sentinel",
    "Fabric reconstruction is deterministic at any timestamp",
];

fn now_ts() -> Timestamp {
    Utc::now()
}

#[test]
fn spec_sanity_rules_exist() {
    assert_eq!(SPEC_RULES.len(), 5);
}

#[test]
fn spec_cell_uses_max_time_and_current_lookup() {
    let engine = Engine::new(":memory:").unwrap();
    let cell = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"hello".to_vec())
        .unwrap();

    let now = now_ts();
    assert!(cell.valid_from <= now);
    assert!(cell.valid_to > now);

    let current = engine.get_cell(cell.id).unwrap();
    assert_eq!(current.content, b"hello".to_vec());
    assert!(current.valid_from <= now_ts());
    assert!(current.valid_to > now_ts());
}

#[test]
fn spec_temporal_window_boundary_on_update() {
    let engine = Engine::new(":memory:").unwrap();
    let cell = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"v1".to_vec())
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(2));
    let before_update = cell.valid_from;

    let updated = engine.update_cell_content(cell.id, b"v2".to_vec()).unwrap();

    // AGENTS window semantics: valid_from <= T < valid_to.
    let old_at_before = engine.get_cell_at(cell.id, before_update).unwrap();
    assert_eq!(old_at_before.content, b"v1".to_vec());

    let at_boundary = engine.get_cell_at(cell.id, updated.valid_from).unwrap();
    assert_eq!(at_boundary.content, b"v2".to_vec());
    assert!(at_boundary.valid_to > at_boundary.valid_from);
}

#[test]
fn spec_temporal_fabric_cells_when_enabled() {
    let engine = Engine::with_config(
        ":memory:",
        EngineConfig {
            temporal_fabric_cells: true,
        },
    )
    .unwrap();

    let parent = engine
        .create_cell_with_fabric(CellType::Container, ContentFormat::Json, vec![])
        .unwrap();
    let first = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"first".to_vec())
        .unwrap();
    let second = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"second".to_vec())
        .unwrap();

    engine
        .add_fabric_cell(parent.id, first.id, RelationType::Contains, Some(0))
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(2));
    let t_before_switch = now_ts();
    std::thread::sleep(std::time::Duration::from_millis(2));

    engine
        .add_fabric_cell(parent.id, second.id, RelationType::Contains, Some(0))
        .unwrap();

    let historical = engine
        .build_fabric_at_time(parent.id, t_before_switch)
        .unwrap();
    assert_eq!(historical.id, parent.fabric_id.unwrap());
    assert!(
        historical
            .cells
            .iter()
            .any(|entry| entry.cell.id == first.id && entry.ordinal == Some(0))
    );

    let current = engine.build_fabric(parent.id).unwrap();
    assert_eq!(current.id, parent.fabric_id.unwrap());
    assert!(
        current
            .cells
            .iter()
            .any(|entry| entry.cell.id == second.id && entry.ordinal == Some(0))
    );
}

#[test]
fn temporal_fabric_order_history_preserves_previous_ordinals() {
    let engine = Engine::with_config(
        ":memory:",
        EngineConfig {
            temporal_fabric_cells: true,
        },
    )
    .unwrap();

    let parent = engine
        .create_cell_with_fabric(CellType::Container, ContentFormat::Json, vec![])
        .unwrap();
    let first = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"first".to_vec())
        .unwrap();
    let second = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"second".to_vec())
        .unwrap();

    engine
        .add_fabric_cell(parent.id, first.id, RelationType::Contains, Some(0))
        .unwrap();
    engine
        .add_fabric_cell(parent.id, second.id, RelationType::Contains, Some(1))
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(2));
    let before_reorder = now_ts();
    std::thread::sleep(std::time::Duration::from_millis(2));

    engine
        .add_fabric_cell(parent.id, first.id, RelationType::Contains, Some(1))
        .unwrap();

    let historical = engine
        .build_fabric_at_time(parent.id, before_reorder)
        .unwrap();
    let current = engine.build_fabric(parent.id).unwrap();

    let historical_first = historical
        .cells
        .iter()
        .find(|entry| entry.cell.id == first.id && entry.relation_type == RelationType::Contains)
        .unwrap();
    let current_first = current
        .cells
        .iter()
        .find(|entry| entry.cell.id == first.id && entry.relation_type == RelationType::Contains)
        .unwrap();

    assert_eq!(historical_first.ordinal, Some(0));
    assert_eq!(current_first.ordinal, Some(1));
}

#[test]
fn non_temporal_fabric_membership_only_guarantees_current_order() {
    let engine = Engine::new(":memory:").unwrap();

    let parent = engine
        .create_cell_with_fabric(CellType::Container, ContentFormat::Json, vec![])
        .unwrap();
    let first = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"first".to_vec())
        .unwrap();
    let second = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"second".to_vec())
        .unwrap();

    engine
        .add_fabric_cell(parent.id, first.id, RelationType::Contains, Some(0))
        .unwrap();
    engine
        .add_fabric_cell(parent.id, second.id, RelationType::Contains, Some(1))
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(2));
    let before_reorder = now_ts();
    std::thread::sleep(std::time::Duration::from_millis(2));

    engine
        .add_fabric_cell(parent.id, first.id, RelationType::Contains, Some(1))
        .unwrap();

    let historical = engine
        .build_fabric_at_time(parent.id, before_reorder)
        .unwrap();
    let current = engine.build_fabric(parent.id).unwrap();

    let historical_first = historical
        .cells
        .iter()
        .find(|entry| entry.cell.id == first.id && entry.relation_type == RelationType::Contains)
        .unwrap();
    let current_first = current
        .cells
        .iter()
        .find(|entry| entry.cell.id == first.id && entry.relation_type == RelationType::Contains)
        .unwrap();

    assert_eq!(historical_first.ordinal, current_first.ordinal);
    assert_eq!(current_first.ordinal, Some(1));
}

#[test]
fn e2e_lifecycle_black_box_acceptance() {
    let engine = Engine::new(":memory:").unwrap();

    let raw = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"Hello world!".to_vec())
        .unwrap();
    let digested = engine
        .create_cell_with_fabric(
            CellType::Digested,
            ContentFormat::Text,
            b"HELLO WORLD!".to_vec(),
        )
        .unwrap();
    let container = engine
        .create_cell_with_fabric(CellType::Container, ContentFormat::Json, vec![])
        .unwrap();

    engine
        .add_fabric_cell(container.id, raw.id, RelationType::Contains, None)
        .unwrap();
    engine
        .add_fabric_cell(container.id, digested.id, RelationType::Contains, None)
        .unwrap();
    engine
        .add_fabric_cell(digested.id, raw.id, RelationType::DerivesFrom, None)
        .unwrap();

    let contains = engine
        .get_cells_by_relation(container.id, RelationType::Contains)
        .unwrap();
    assert_eq!(contains, vec![raw.id, digested.id]);

    let before_update = now_ts();
    std::thread::sleep(std::time::Duration::from_millis(2));

    engine
        .update_cell_content(raw.id, b"Updated content".to_vec())
        .unwrap();

    let current_raw = engine.get_cell(raw.id).unwrap();
    let historical_raw = engine.get_cell_at(raw.id, before_update).unwrap();
    assert_eq!(current_raw.content, b"Updated content".to_vec());
    assert_eq!(historical_raw.content, b"Hello world!".to_vec());

    let fabric_now = engine.build_fabric(container.id).unwrap();
    assert_eq!(fabric_now.id, container.fabric_id.unwrap());
    assert!(fabric_now.cells.iter().any(|entry| {
        entry.relation_type == RelationType::Root
            && entry.ordinal.is_none()
            && entry.cell.id == container.id
    }));
    assert!(fabric_now.cells.iter().any(|entry| entry.cell.id == raw.id));
    assert!(
        fabric_now
            .cells
            .iter()
            .any(|entry| entry.cell.id == digested.id)
    );
    let mut expected = vec![container.fabric_id.unwrap(), digested.fabric_id.unwrap()];
    expected.sort();
    assert_eq!(engine.list_fabrics().unwrap(), expected);
}

#[test]
fn context_defers_writes_until_save() {
    let engine = Engine::new(":memory:").unwrap();

    let root = engine
        .create_cell_with_fabric(CellType::Container, ContentFormat::Json, vec![])
        .unwrap();
    let child = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"original".to_vec())
        .unwrap();
    engine
        .add_fabric_cell(root.id, child.id, RelationType::Contains, None)
        .unwrap();

    let mut context = engine.open_document_context(root.id, None).unwrap();
    context
        .update_cell_content(child.id, b"draft".to_vec())
        .unwrap();

    let db_before_save = engine.get_cell(child.id).unwrap();
    assert_eq!(db_before_save.content, b"original".to_vec());

    context.save().unwrap();

    let db_after_save = engine.get_cell(child.id).unwrap();
    assert_eq!(db_after_save.content, b"draft".to_vec());
}

#[test]
fn optional_timestamp_uses_current_when_missing_and_history_when_present() {
    let engine = Engine::new(":memory:").unwrap();

    let cell = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"v1".to_vec())
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(2));
    let ts_before_update = now_ts();
    std::thread::sleep(std::time::Duration::from_millis(2));

    engine.update_cell_content(cell.id, b"v2".to_vec()).unwrap();

    let current_via_optional = engine.get_cell(cell.id).unwrap();
    let historical_via_optional = engine.get_cell_at(cell.id, ts_before_update).unwrap();

    assert_eq!(current_via_optional.content, b"v2".to_vec());
    assert_eq!(historical_via_optional.content, b"v1".to_vec());
}

#[test]
fn delete_closes_live_row_but_preserves_history() {
    let engine = Engine::new(":memory:").unwrap();

    let cell = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"alive".to_vec())
        .unwrap();
    let created_at = cell.valid_from;

    std::thread::sleep(std::time::Duration::from_millis(2));
    engine.delete_cell(cell.id).unwrap();

    assert!(matches!(
        engine.get_cell(cell.id),
        Err(mnemonic_engine::EngineError::NotFound)
    ));

    let historical = engine.get_cell_at(cell.id, created_at).unwrap();
    assert_eq!(historical.content, b"alive".to_vec());
}

#[test]
fn full_history_returns_all_versions_in_order() {
    let engine = Engine::new(":memory:").unwrap();

    let cell = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"v1".to_vec())
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    engine.update_cell_content(cell.id, b"v2".to_vec()).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    engine.update_cell_content(cell.id, b"v3".to_vec()).unwrap();

    let history = engine.get_cell_history(cell.id).unwrap();
    let contents: Vec<Vec<u8>> = history.into_iter().map(|cell| cell.content).collect();
    assert_eq!(
        contents,
        vec![b"v1".to_vec(), b"v2".to_vec(), b"v3".to_vec()]
    );
}

#[test]
fn adding_fabric_members_requires_a_fabric_backed_owner_cell() {
    let engine = Engine::new(":memory:").unwrap();

    let owner = engine
        .create_cell(CellType::Data, ContentFormat::Json, vec![])
        .unwrap();
    let child = engine
        .create_cell(CellType::Raw, ContentFormat::Text, b"child".to_vec())
        .unwrap();

    let err = engine
        .add_fabric_cell(owner.id, child.id, RelationType::Contains, None)
        .unwrap_err();

    assert!(matches!(err, mnemonic_engine::EngineError::InvalidData(_)));
}

#[test]
fn sqlite_schema_uses_datetime_and_nullable_ordinals() {
    let db_path =
        std::env::temp_dir().join(format!("mnemonic_schema_{}.sqlite", uuid::Uuid::now_v7()));
    let engine = Engine::new(db_path.to_str().unwrap()).unwrap();
    drop(engine);

    let conn = Connection::open(&db_path).unwrap();

    let active_fabrics = Engine::new(db_path.to_str().unwrap())
        .unwrap()
        .list_fabrics()
        .unwrap_or_default();
    assert!(active_fabrics.is_empty());

    let mut table_stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name ASC")
        .unwrap();
    let tables = table_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(tables.iter().any(|name| name == "fabric"));
    assert!(tables.iter().any(|name| name == "cell"));
    assert!(!tables.iter().any(|name| name == "data_cell"));
    assert!(!tables.iter().any(|name| name == "meta_cell"));

    let mut stmt = conn.prepare("PRAGMA table_info('fabric_cells')").unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(rows.iter().any(|(name, ty, notnull)| {
        name == "ordinal" && ty.eq_ignore_ascii_case("INTEGER") && *notnull == 0
    }));
    assert!(
        rows.iter()
            .any(|(name, ty, _)| { name == "valid_from" && ty.eq_ignore_ascii_case("DATETIME") })
    );
    assert!(
        rows.iter()
            .any(|(name, ty, _)| { name == "valid_to" && ty.eq_ignore_ascii_case("DATETIME") })
    );

    std::fs::remove_file(db_path).unwrap();
}

#[test]
fn listing_fabrics_reads_from_dedicated_fabric_table() {
    let db_path =
        std::env::temp_dir().join(format!("mnemonic_fabrics_{}.sqlite", uuid::Uuid::now_v7()));
    let engine = Engine::new(db_path.to_str().unwrap()).unwrap();

    let first = engine
        .create_cell_with_fabric(CellType::Container, ContentFormat::Json, vec![])
        .unwrap();
    let second = engine
        .create_cell_with_fabric(CellType::Container, ContentFormat::Json, vec![])
        .unwrap();

    let mut expected = vec![first.fabric_id.unwrap(), second.fabric_id.unwrap()];
    expected.sort();
    assert_eq!(engine.list_fabrics().unwrap(), expected);

    drop(engine);
    std::fs::remove_file(db_path).unwrap();
}
