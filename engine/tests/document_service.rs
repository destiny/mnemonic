// Copyright 2026 Arion Yau

use mnemonic_engine::{
    DictionaryEntryInput, DocumentKind, Engine, RelationType, Service, ServiceApi,
};

#[test]
fn context_and_kind_are_distinguishable_for_routing() {
    let engine = Engine::new(":memory:").unwrap();
    let service = Service::new(&engine);

    let text = service.create_document_context(DocumentKind::Text).unwrap();
    let table = service
        .create_document_context(DocumentKind::Table)
        .unwrap();
    let dictionary = service
        .create_document_context(DocumentKind::Dictionary)
        .unwrap();

    assert_eq!(
        service.detect_kind(text.document.root_cell_id).unwrap(),
        DocumentKind::Text
    );
    assert_eq!(
        service.detect_kind(table.document.root_cell_id).unwrap(),
        DocumentKind::Table
    );
    assert_eq!(
        service
            .detect_kind(dictionary.document.root_cell_id)
            .unwrap(),
        DocumentKind::Dictionary
    );
}

#[test]
fn text_document_keeps_segment_order_via_context() {
    let engine = Engine::new(":memory:").unwrap();
    let service = Service::new(&engine);

    let context = service.create_document_context(DocumentKind::Text).unwrap();
    service.append_text_segment(&context, "alpha").unwrap();
    service.append_text_segment(&context, "beta").unwrap();
    service.append_text_segment(&context, "gamma").unwrap();

    let children = engine
        .get_cells_by_relation(context.document.root_cell_id, RelationType::Contains)
        .unwrap();
    assert_eq!(children.len(), 3);
}

#[test]
fn dictionary_transforms_to_text_with_lineage() {
    let engine = Engine::new(":memory:").unwrap();
    let service = Service::new(&engine);

    let dictionary_ctx = service
        .create_document_context(DocumentKind::Dictionary)
        .unwrap();

    service
        .add_dictionary_entry(
            &dictionary_ctx,
            &DictionaryEntryInput {
                key: "language".to_string(),
                values: vec!["rust".to_string(), "zig".to_string()],
            },
        )
        .unwrap();

    service
        .add_dictionary_entry(
            &dictionary_ctx,
            &DictionaryEntryInput {
                key: "storage".to_string(),
                values: vec!["sqlite".to_string()],
            },
        )
        .unwrap();

    let text_ctx = service
        .transform_dictionary_to_text(&dictionary_ctx)
        .unwrap();
    assert_eq!(text_ctx.document.kind, DocumentKind::Text);

    let derives_from = engine
        .get_cells_by_relation(text_ctx.document.root_cell_id, RelationType::DerivesFrom)
        .unwrap();
    assert_eq!(derives_from, vec![dictionary_ctx.document.root_cell_id]);
}
