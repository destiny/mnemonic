use std::time::Instant;

use mnemonic_engine::{CellType, ContentFormat, Engine, RelationType};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct DocumentSeed {
    title: &'static str,
    doc_type: &'static str,
    section_count: usize,
    reference_count: usize,
    target_words: usize,
}

#[derive(Debug)]
struct StoredDocument {
    root_id: Uuid,
    section_ids: Vec<Uuid>,
    reference_ids: Vec<Uuid>,
}

fn repeated_words(label: &str, count: usize) -> String {
    (0..count)
        .map(|idx| format!("{}_{}", label, idx))
        .collect::<Vec<_>>()
        .join(" ")
}

fn build_sections(seed: &DocumentSeed) -> Vec<String> {
    let base = seed.target_words / seed.section_count;
    let remainder = seed.target_words % seed.section_count;

    (0..seed.section_count)
        .map(|idx| {
            let words = base + usize::from(idx < remainder);
            repeated_words(&format!("{}_section_{}", seed.doc_type, idx), words)
        })
        .collect()
}

fn build_references(seed: &DocumentSeed) -> Vec<String> {
    (0..seed.reference_count)
        .map(|idx| format!("{}::reference::{}", seed.doc_type, idx))
        .collect()
}

fn seed_document(engine: &Engine, seed: &DocumentSeed) -> StoredDocument {
    let root_payload = format!(
        "{{\"title\":\"{}\",\"document_type\":\"{}\",\"target_words\":{}}}",
        seed.title, seed.doc_type, seed.target_words
    );

    let root = engine
        .create_cell(
            CellType::Container,
            ContentFormat::Json,
            root_payload.into_bytes(),
        )
        .unwrap();

    let sections = build_sections(seed);
    let references = build_references(seed);

    let mut section_ids = Vec::new();
    for (idx, section) in sections.iter().enumerate() {
        let section_cell = engine
            .create_cell(
                CellType::Data,
                ContentFormat::Markdown,
                section.as_bytes().to_vec(),
            )
            .unwrap();

        engine
            .add_relation(
                root.id,
                section_cell.id,
                RelationType::Contains,
                Some(idx as i64),
            )
            .unwrap();
        section_ids.push(section_cell.id);
    }

    let mut reference_ids = Vec::new();
    for (idx, reference) in references.iter().enumerate() {
        let reference_cell = engine
            .create_cell(
                CellType::Meta,
                ContentFormat::Text,
                reference.as_bytes().to_vec(),
            )
            .unwrap();

        engine
            .add_relation(
                root.id,
                reference_cell.id,
                RelationType::References,
                Some(idx as i64),
            )
            .unwrap();
        reference_ids.push(reference_cell.id);
    }

    StoredDocument {
        root_id: root.id,
        section_ids,
        reference_ids,
    }
}

#[test]
fn generates_and_roundtrips_large_use_case_documents() {
    let engine = Engine::new(":memory:").unwrap();

    let seeds = vec![
        DocumentSeed {
            title: "Distributed Cache Invalidation Guide",
            doc_type: "technical_document",
            section_count: 5,
            reference_count: 40,
            target_words: 10_000,
        },
        DocumentSeed {
            title: "Q4 Expansion Proposal",
            doc_type: "business_proposal",
            section_count: 5,
            reference_count: 50,
            target_words: 10_000,
        },
        DocumentSeed {
            title: "Adaptive Planning Under Resource Constraints",
            doc_type: "academic_paper",
            section_count: 6,
            reference_count: 60,
            target_words: 10_000,
        },
        DocumentSeed {
            title: "LLM Tool-Use Behavior Study",
            doc_type: "research_document",
            section_count: 6,
            reference_count: 80,
            target_words: 10_000,
        },
        DocumentSeed {
            title: "The Last Orbit - Full Draft",
            doc_type: "novel_script",
            section_count: 20,
            reference_count: 20,
            target_words: 100_000,
        },
        DocumentSeed {
            title: "Broken Vows (Drama Script)",
            doc_type: "drama_script",
            section_count: 12,
            reference_count: 20,
            target_words: 25_000,
        },
        DocumentSeed {
            title: "Novel Translation Workspace: Northwind",
            doc_type: "translation_workspace",
            section_count: 10,
            reference_count: 120,
            target_words: 30_000,
        },
        DocumentSeed {
            title: "Systems Review with External References",
            doc_type: "document_with_external_references",
            section_count: 5,
            reference_count: 300,
            target_words: 12_000,
        },
    ];

    let mut stored = Vec::new();
    let mut total_words_seeded = 0usize;
    let mut total_reference_cells = 0usize;

    for seed in &seeds {
        let start = Instant::now();
        let doc = seed_document(&engine, seed);
        let elapsed_ms = start.elapsed().as_millis();

        total_words_seeded += seed.target_words;
        total_reference_cells += seed.reference_count;

        println!(
            "stored title=\"{}\" root_id={} target_words={} section_count={} reference_count={} write_ms={}",
            seed.title,
            doc.root_id,
            seed.target_words,
            seed.section_count,
            seed.reference_count,
            elapsed_ms
        );

        stored.push((seed, doc));
    }

    for (seed, doc) in &stored {
        let fetch_start = Instant::now();
        let context = engine.build_context(doc.root_id).unwrap();
        let fetch_ms = fetch_start.elapsed().as_millis();

        let contains = context
            .edges
            .iter()
            .filter(|e| e.relation_type == RelationType::Contains)
            .count();
        let references = context
            .edges
            .iter()
            .filter(|e| e.relation_type == RelationType::References)
            .count();

        let stored_words: usize = context
            .cells
            .iter()
            .filter(|c| c.cell_type == CellType::Data)
            .map(|c| {
                String::from_utf8_lossy(&c.content)
                    .split_whitespace()
                    .count()
            })
            .sum();

        assert_eq!(contains, seed.section_count);
        assert_eq!(references, seed.reference_count);
        assert_eq!(doc.section_ids.len(), seed.section_count);
        assert_eq!(doc.reference_ids.len(), seed.reference_count);
        assert_eq!(stored_words, seed.target_words);

        println!(
            "fetched title=\"{}\" root_id={} words={} fetch_ms={}",
            seed.title, doc.root_id, stored_words, fetch_ms
        );
    }

    assert!(total_words_seeded >= 197_000);
    assert!(total_reference_cells >= 690);
}
