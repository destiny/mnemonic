use chrono::Utc;
use std::fs;
use std::path::{Path, PathBuf};

use mnemonic_engine::storage::time::format_db_time;
use mnemonic_engine::{CellType, ContentFormat, Engine, RelationType, Timestamp};
use rusqlite::{Connection, params};
use uuid::Uuid;

const TAGGED_AS_RELATION: &str = "TaggedAs";
const SHARED_SCENARIO_DB: &str = "general_daily_cases.sqlite";

#[derive(Debug, Clone)]
struct DocumentScenario {
    case_id: &'static str,
    title: &'static str,
    document_type: &'static str,
    purpose: &'static str,
    section_titles: &'static [&'static str],
    target_words: usize,
    reference_count: usize,
    tag_candidates: &'static [&'static str],
    min_tag_occurrence: usize,
    actors: &'static [&'static str],
    events: &'static [&'static str],
    signals: &'static [&'static str],
    safeguards: &'static [&'static str],
    outcomes: &'static [&'static str],
    locations: &'static [&'static str],
    metrics: &'static [&'static str],
}

#[derive(Debug)]
struct GeneratedDocument {
    sections: Vec<String>,
    references: Vec<String>,
    tags_with_counts: Vec<(String, usize)>,
    source_markdown: String,
}

#[derive(Debug)]
struct StoredDocument {
    root_id: Uuid,
    purpose_id: Uuid,
    section_ids: Vec<Uuid>,
    reference_ids: Vec<Uuid>,
    tag_ids: Vec<Uuid>,
}

#[derive(Debug)]
struct CasePaths {
    source_doc_path: PathBuf,
    rendered_doc_path: PathBuf,
    snapshot_path: PathBuf,
}

fn word_count(text: &str) -> usize {
    text.split_whitespace().count()
}

fn now_ts() -> Timestamp {
    Utc::now()
}

fn scenario_output_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_output")
        .join("scenario_documents")
}

fn shared_db_path() -> PathBuf {
    scenario_output_dir().join(SHARED_SCENARIO_DB)
}

fn case_paths(scenario: &DocumentScenario) -> CasePaths {
    let base = format!("{}_{}", scenario.case_id, scenario.document_type);
    let out_dir = scenario_output_dir();
    CasePaths {
        source_doc_path: out_dir.join(format!("{base}_source.md")),
        rendered_doc_path: out_dir.join(format!("{base}_rendered.md")),
        snapshot_path: out_dir.join(format!("{base}_db_snapshot.txt")),
    }
}

fn scenarios() -> Vec<DocumentScenario> {
    vec![
        DocumentScenario {
            case_id: "case_01",
            title: "Distributed Cache Invalidation Guide",
            document_type: "technical_document",
            purpose: "Provide an implementable and auditable invalidation strategy so product, pricing, and order views remain consistent under routine load and incident conditions.",
            section_titles: &[
                "Scope and Constraints",
                "Consistency Model and Guarantees",
                "Invalidation Triggers and Pipelines",
                "Failure Modes and Recovery",
                "Operational Runbook",
            ],
            target_words: 10_000,
            reference_count: 40,
            tag_candidates: &[
                "cache invalidation",
                "event bus",
                "stale read",
                "retry queue",
                "runbook",
                "lease token",
                "write-through cache",
                "incident commander",
            ],
            min_tag_occurrence: 8,
            actors: &[
                "catalog service",
                "pricing service",
                "order service",
                "inventory service",
                "eligibility service",
            ],
            events: &[
                "item publish",
                "price override",
                "promotion activation",
                "inventory adjustment",
                "order cancellation",
            ],
            signals: &[
                "domain event",
                "change-data-capture message",
                "transaction hook",
                "workflow checkpoint",
            ],
            safeguards: &[
                "idempotent consumers",
                "partition-aware retries",
                "schema version guards",
                "bounded replay windows",
            ],
            outcomes: &[
                "users see current prices at checkout",
                "availability stays aligned with warehouse truth",
                "support tickets carry reproducible timeline evidence",
            ],
            locations: &["Singapore", "Tokyo", "Frankfurt", "Virginia", "Sydney"],
            metrics: &[
                "invalidation latency",
                "stale read rate",
                "duplicate event rate",
                "retry queue depth",
            ],
        },
        DocumentScenario {
            case_id: "case_02",
            title: "Q4 Expansion Proposal",
            document_type: "business_proposal",
            purpose: "Present a staged expansion plan that aligns market entry sequencing, budget governance, and operating risk controls across regions.",
            section_titles: &[
                "Executive Context",
                "Market Opportunity",
                "Operating Model",
                "Financial Plan",
                "Risk and Mitigation",
            ],
            target_words: 10_000,
            reference_count: 50,
            tag_candidates: &[
                "unit economics",
                "market entry",
                "go-to-market",
                "forecast variance",
                "operating margin",
                "payback period",
                "risk register",
            ],
            min_tag_occurrence: 8,
            actors: &[
                "regional gm",
                "finance lead",
                "sales director",
                "operations manager",
                "partnership team",
            ],
            events: &[
                "pilot launch",
                "channel onboarding",
                "price test",
                "territory rollout",
                "vendor consolidation",
            ],
            signals: &[
                "board update",
                "monthly forecast",
                "pipeline review",
                "market intelligence memo",
            ],
            safeguards: &[
                "stage-gated funding",
                "contractual controls",
                "compliance checks",
                "quarterly portfolio review",
            ],
            outcomes: &[
                "new revenue matures without margin collapse",
                "cross-functional teams execute predictable handoffs",
                "leadership decisions are evidence-backed",
            ],
            locations: &["Seoul", "Jakarta", "Bangkok", "Manila", "Kuala Lumpur"],
            metrics: &[
                "operating margin",
                "payback period",
                "forecast variance",
                "conversion rate",
            ],
        },
        DocumentScenario {
            case_id: "case_03",
            title: "Adaptive Planning Under Resource Constraints",
            document_type: "academic_paper",
            purpose: "Document a reproducible research method for adaptive planning where compute budgets and uncertain demand interact over time.",
            section_titles: &[
                "Problem Formulation",
                "Methodology",
                "Experimental Setup",
                "Results and Analysis",
                "Threats to Validity",
                "Conclusion",
            ],
            target_words: 10_000,
            reference_count: 60,
            tag_candidates: &[
                "resource constraints",
                "adaptive planning",
                "ablation study",
                "baseline model",
                "confidence interval",
                "reproducibility",
                "error bounds",
            ],
            min_tag_occurrence: 8,
            actors: &[
                "research team",
                "dataset curator",
                "evaluation pipeline",
                "statistical reviewer",
            ],
            events: &[
                "dataset refresh",
                "ablation run",
                "parameter sweep",
                "replication trial",
            ],
            signals: &[
                "confidence report",
                "experimental log",
                "peer feedback memo",
                "review artifact",
            ],
            safeguards: &[
                "pre-registered hypotheses",
                "seed control",
                "cross-validation protocol",
                "error bar reporting",
            ],
            outcomes: &[
                "method comparisons remain interpretable",
                "reported gains survive replication",
                "decision thresholds are statistically defensible",
            ],
            locations: &["Lab A", "Lab B", "Cluster North", "Cluster South"],
            metrics: &[
                "confidence interval",
                "error bounds",
                "effect size",
                "replication success rate",
            ],
        },
        DocumentScenario {
            case_id: "case_04",
            title: "LLM Tool-Use Behavior Study",
            document_type: "research_document",
            purpose: "Analyze agentic tool-use behavior, failure modes, and recovery patterns under realistic production-style tasks.",
            section_titles: &[
                "Study Scope",
                "Protocol Design",
                "Observed Behaviors",
                "Failure Taxonomy",
                "Intervention Strategies",
                "Findings Summary",
            ],
            target_words: 10_000,
            reference_count: 80,
            tag_candidates: &[
                "tool call",
                "grounding check",
                "hallucination risk",
                "recovery strategy",
                "traceability",
                "evaluation harness",
                "failure mode",
            ],
            min_tag_occurrence: 8,
            actors: &[
                "model runner",
                "evaluation harness",
                "human annotator",
                "orchestrator",
                "safety reviewer",
            ],
            events: &[
                "tool selection",
                "context refresh",
                "fallback trigger",
                "multi-step recovery",
            ],
            signals: &[
                "trace log",
                "confidence marker",
                "annotation note",
                "evaluation checkpoint",
            ],
            safeguards: &[
                "grounding checks",
                "tool budget limits",
                "result verification",
                "post-run triage",
            ],
            outcomes: &[
                "responses stay attributable to evidence",
                "error recovery cost remains bounded",
                "operator trust improves with traceability",
            ],
            locations: &["Eval batch 1", "Eval batch 2", "Prod-like sandbox"],
            metrics: &[
                "tool success rate",
                "hallucination risk",
                "recovery latency",
                "trace coverage",
            ],
        },
        DocumentScenario {
            case_id: "case_05",
            title: "The Last Orbit - Full Draft",
            document_type: "novel_script",
            purpose: "Develop a coherent full-length draft with stable character arcs, setting continuity, and event chronology for revision planning.",
            section_titles: &[
                "Act I - Departure",
                "Act II - Fracture",
                "Act III - Return",
                "Character Notes",
                "Worldbuilding Ledger",
                "Revision Notes",
            ],
            target_words: 100_000,
            reference_count: 20,
            tag_candidates: &[
                "Captain Mira",
                "Orion Station",
                "Echo Gate",
                "Helios Archive",
                "Ashfall Event",
                "Council Accord",
                "Navigator Rune",
            ],
            min_tag_occurrence: 12,
            actors: &[
                "Captain Mira",
                "Navigator Rune",
                "Archivist Sol",
                "Commander Vale",
                "Council envoy",
            ],
            events: &[
                "docking dispute",
                "jump sequence",
                "signal blackout",
                "Ashfall Event",
                "Council hearing",
            ],
            signals: &[
                "distress beacon",
                "sealed transcript",
                "flight recorder",
                "Helios Archive extract",
            ],
            safeguards: &[
                "continuity pass",
                "timeline checks",
                "character voice guardrails",
                "scene dependency mapping",
            ],
            outcomes: &[
                "character motivations stay consistent",
                "plot twists remain foreshadowed",
                "world rules stay internally coherent",
            ],
            locations: &["Orion Station", "Echo Gate", "Broken Ring", "Delta Harbor"],
            metrics: &[
                "scene continuity score",
                "character arc coverage",
                "event chronology overlap",
            ],
        },
        DocumentScenario {
            case_id: "case_06",
            title: "Broken Vows (Drama Script)",
            document_type: "drama_script",
            purpose: "Build a stage-ready script draft with emotional progression, scene blocking intent, and repeatable motif tracking.",
            section_titles: &[
                "Opening Tableau",
                "Act I Conflict",
                "Act II Escalation",
                "Act III Confrontation",
                "Stage Direction Notes",
                "Dialogue Revision Pass",
            ],
            target_words: 25_000,
            reference_count: 20,
            tag_candidates: &[
                "Elena",
                "Marcus",
                "Old Chapel",
                "Winter Gala",
                "red ledger",
                "family oath",
            ],
            min_tag_occurrence: 8,
            actors: &[
                "Elena",
                "Marcus",
                "Aunt Viola",
                "Judge Carver",
                "chorus lead",
            ],
            events: &[
                "public accusation",
                "private confession",
                "inheritance reveal",
                "vow renewal",
            ],
            signals: &[
                "lighting cue",
                "orchestra motif",
                "backstage note",
                "blocking revision",
            ],
            safeguards: &[
                "dialogue pacing checks",
                "scene transition mapping",
                "motif consistency review",
                "cast readability pass",
            ],
            outcomes: &[
                "emotional beats land on cue",
                "audience comprehension remains high",
                "the final reveal feels earned",
            ],
            locations: &["Old Chapel", "Ballroom", "Courtyard", "Green Room"],
            metrics: &[
                "dialogue pacing",
                "scene transition latency",
                "motif recurrence",
            ],
        },
        DocumentScenario {
            case_id: "case_07",
            title: "Novel Translation Workspace: Northwind",
            document_type: "translation_workspace",
            purpose: "Track translation choices, terminology reuse, and context-preserving rewrites across a long-form narrative workspace.",
            section_titles: &[
                "Source Context Notes",
                "Terminology Register",
                "Style Guide Decisions",
                "Chapter Translation Draft",
                "Ambiguity Resolution Log",
                "Reviewer Feedback Integration",
            ],
            target_words: 30_000,
            reference_count: 120,
            tag_candidates: &[
                "Northwind Harbor",
                "Talia",
                "glass orchard",
                "winter oath",
                "idiom mapping",
                "term consistency",
                "voice preservation",
            ],
            min_tag_occurrence: 10,
            actors: &[
                "lead translator",
                "review editor",
                "term curator",
                "context reviewer",
            ],
            events: &[
                "idiom rewrite",
                "term lock",
                "voice conflict",
                "context rollback",
                "chapter merge",
            ],
            signals: &[
                "review annotation",
                "term alert",
                "ambiguity marker",
                "parallel draft note",
            ],
            safeguards: &[
                "term consistency checks",
                "voice preservation review",
                "context window validation",
                "cross-chapter terminology diff",
            ],
            outcomes: &[
                "translated tone matches source intent",
                "repeated terms remain stable across chapters",
                "review cycles converge faster",
            ],
            locations: &["Northwind Harbor", "Salt Market", "Glass Orchard"],
            metrics: &[
                "term consistency",
                "review turnaround",
                "ambiguity resolution rate",
            ],
        },
        DocumentScenario {
            case_id: "case_08",
            title: "Systems Review with External References",
            document_type: "document_with_external_references",
            purpose: "Create a verifiable systems review package with dense external references and traceable claims across architecture, operations, and governance.",
            section_titles: &[
                "Review Scope",
                "Architecture Findings",
                "Operational Findings",
                "Compliance and Policy Mapping",
                "Reference Matrix",
                "Action Plan",
            ],
            target_words: 12_000,
            reference_count: 300,
            tag_candidates: &[
                "evidence link",
                "control mapping",
                "audit trail",
                "external reference",
                "remediation action",
                "policy exception",
                "verification note",
            ],
            min_tag_occurrence: 10,
            actors: &[
                "review lead",
                "security architect",
                "platform owner",
                "compliance manager",
                "audit coordinator",
            ],
            events: &[
                "control assessment",
                "evidence intake",
                "exception review",
                "remediation tracking",
            ],
            signals: &[
                "evidence link",
                "verification note",
                "control register update",
                "risk escalation memo",
            ],
            safeguards: &[
                "source attribution checks",
                "control mapping reviews",
                "evidence integrity validation",
                "remediation acceptance criteria",
            ],
            outcomes: &[
                "findings are independently verifiable",
                "owners can execute remediation quickly",
                "audit scope and operational scope stay aligned",
            ],
            locations: &[
                "Platform A",
                "Platform B",
                "Data Zone C",
                "Control Domain D",
            ],
            metrics: &[
                "evidence coverage",
                "control compliance ratio",
                "remediation closure rate",
            ],
        },
    ]
}

fn section_sentence(scenario: &DocumentScenario, section_title: &str, idx: usize) -> String {
    let actor = scenario.actors[idx % scenario.actors.len()];
    let event = scenario.events[idx % scenario.events.len()];
    let signal = scenario.signals[idx % scenario.signals.len()];
    let safeguard = scenario.safeguards[idx % scenario.safeguards.len()];
    let outcome = scenario.outcomes[idx % scenario.outcomes.len()];
    let location = scenario.locations[idx % scenario.locations.len()];
    let metric = scenario.metrics[idx % scenario.metrics.len()];
    let term = scenario.tag_candidates[idx % scenario.tag_candidates.len()];

    let hour = [2usize, 5, 9, 12, 15, 18, 21, 23][idx % 8];
    let minute = [4usize, 11, 19, 26, 34, 43, 51, 58][idx % 8];
    let case_num = idx + 1;
    let scope = 600 + (idx % 31) * 73;

    match idx % 10 {
        0 => format!(
            "In {section_title}, case {case_num} tracks how {actor} handled {event} at {location}, emitted a {signal}, and used {safeguard} so {outcome}."
        ),
        1 => format!(
            "At {hour:02}:{minute:02} UTC, reviewers checked {metric}, recorded an audit trail entry, and confirmed that the {term} decision remained consistent with prior sections."
        ),
        2 => format!(
            "The team reconciled {scope} affected units, compared timeline order against business order, and wrote a verification note linking the incident to {term} criteria."
        ),
        3 => format!(
            "A focused rehearsal evaluated fallback behavior, then documented how {safeguard} prevented regressions while keeping the narrative and operational intent readable."
        ),
        4 => format!(
            "Stakeholders accepted this checkpoint after evidence review showed that {signal} and owner actions were traceable, attributable, and aligned with the stated purpose."
        ),
        5 => format!(
            "Cross-functional discussion highlighted repeated phrases and entities, including {term}, so they could be tagged, searched, and reused in downstream verification steps."
        ),
        6 => format!(
            "During validation, the group compared local findings with external references, then updated run notes to preserve continuity across edits and future reruns."
        ),
        7 => format!(
            "This passage captures a concrete decision boundary: when uncertainty rose, teams escalated early, preserved context, and logged the exact rationale behind each change."
        ),
        8 => format!(
            "By tying narrative details to machine-verifiable structure, the scenario proves that readable prose can coexist with deterministic storage and reconstruction rules."
        ),
        _ => format!(
            "The closing line for case {case_num} summarizes accountability, links evidence to outcomes, and leaves a stable trail for inspection from the shared SQLite artifact."
        ),
    }
}

fn synthesize_meaningful_section(
    scenario: &DocumentScenario,
    section_title: &str,
    target_words: usize,
) -> String {
    let mut words = Vec::new();
    let mut i = 0usize;

    while words.len() < target_words {
        let sentence = section_sentence(scenario, section_title, i);
        words.extend(sentence.split_whitespace().map(str::to_string));
        i += 1;
    }

    words.truncate(target_words);

    let mut out = String::new();
    let mut idx = 0usize;
    while idx < words.len() {
        let end = (idx + 120).min(words.len());
        out.push_str(&words[idx..end].join(" "));
        out.push('\n');
        out.push('\n');
        idx = end;
    }
    out.trim().to_string()
}

fn build_references(scenario: &DocumentScenario) -> Vec<String> {
    (0..scenario.reference_count)
        .map(|idx| {
            let location = scenario.locations[idx % scenario.locations.len()];
            let metric = scenario.metrics[idx % scenario.metrics.len()];
            format!(
                "REF-{idx:03}: {} / {} / {} / {}",
                scenario.document_type, location, metric, scenario.case_id
            )
        })
        .collect()
}

fn count_term_occurrences(text: &str, term: &str) -> usize {
    text.match_indices(term).count()
}

fn collect_repeated_tags(
    text: &str,
    candidates: &[&str],
    min_occurrence: usize,
) -> Vec<(String, usize)> {
    let lower = text.to_lowercase();
    let mut tags = candidates
        .iter()
        .filter_map(|term| {
            let count = count_term_occurrences(&lower, &term.to_lowercase());
            if count >= min_occurrence {
                Some(((*term).to_string(), count))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    tags.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    tags
}

fn build_generated_document(scenario: &DocumentScenario) -> GeneratedDocument {
    let section_count = scenario.section_titles.len();
    let base_words = scenario.target_words / section_count;
    let remainder = scenario.target_words % section_count;

    let sections = scenario
        .section_titles
        .iter()
        .enumerate()
        .map(|(idx, title)| {
            let words_for_section = base_words + usize::from(idx < remainder);
            synthesize_meaningful_section(scenario, title, words_for_section)
        })
        .collect::<Vec<_>>();

    let references = build_references(scenario);
    let combined_sections = sections.join("\n\n");
    let tags_with_counts = collect_repeated_tags(
        &combined_sections,
        scenario.tag_candidates,
        scenario.min_tag_occurrence,
    );

    let mut source = String::new();
    source.push_str(&format!("# {}\n\n", scenario.title));
    source.push_str(&format!("Document Type: {}\n\n", scenario.document_type));
    source.push_str("## Purpose\n");
    source.push_str(scenario.purpose);
    source.push_str("\n\n## Sections\n");
    for (title, section) in scenario.section_titles.iter().zip(sections.iter()) {
        source.push_str(&format!("### {title}\n"));
        source.push_str(section);
        source.push_str("\n\n");
    }
    source.push_str("## References\n");
    for reference in &references {
        source.push_str(&format!("- {reference}\n"));
    }
    source.push_str("\n## Tags (Extracted Repeated Terms)\n");
    for (term, count) in &tags_with_counts {
        source.push_str(&format!("- {term} (occurrences: {count})\n"));
    }

    let total_words = sections.iter().map(|s| word_count(s)).sum::<usize>();
    assert_eq!(total_words, scenario.target_words);
    assert!(!tags_with_counts.is_empty());

    GeneratedDocument {
        sections,
        references,
        tags_with_counts,
        source_markdown: source,
    }
}

fn relation_type_json(relation_type: RelationType) -> String {
    serde_json::to_string(&relation_type).unwrap()
}

fn store_scenario_into_engine(
    engine: &Engine,
    scenario: &DocumentScenario,
    generated: &GeneratedDocument,
) -> StoredDocument {
    let root_payload = format!(
        "{{\"case_id\":\"{}\",\"title\":\"{}\",\"document_type\":\"{}\",\"target_words\":{},\"section_count\":{},\"reference_count\":{}}}",
        scenario.case_id,
        scenario.title,
        scenario.document_type,
        scenario.target_words,
        scenario.section_titles.len(),
        scenario.reference_count
    );

    let root = engine
        .create_cell_with_fabric(
            CellType::Container,
            ContentFormat::Json,
            root_payload.into_bytes(),
        )
        .unwrap();
    let purpose = engine
        .create_cell(
            CellType::Meta,
            ContentFormat::Text,
            scenario.purpose.as_bytes().to_vec(),
        )
        .unwrap();
    engine
        .add_fabric_cell(root.id, purpose.id, RelationType::Contains, Some(0))
        .unwrap();

    let mut section_ids = Vec::new();
    for (idx, section) in generated.sections.iter().enumerate() {
        let section_cell = engine
            .create_cell(
                CellType::Data,
                ContentFormat::Markdown,
                section.as_bytes().to_vec(),
            )
            .unwrap();
        engine
            .add_fabric_cell(
                root.id,
                section_cell.id,
                RelationType::Contains,
                Some((idx + 1) as i64),
            )
            .unwrap();
        section_ids.push(section_cell.id);
    }

    let mut reference_ids = Vec::new();
    for (idx, reference) in generated.references.iter().enumerate() {
        let reference_cell = engine
            .create_cell(
                CellType::Meta,
                ContentFormat::Text,
                reference.as_bytes().to_vec(),
            )
            .unwrap();
        engine
            .add_fabric_cell(
                root.id,
                reference_cell.id,
                RelationType::References,
                Some(idx as i64),
            )
            .unwrap();
        reference_ids.push(reference_cell.id);
    }

    let mut tag_ids = Vec::new();
    for (idx, (term, count)) in generated.tags_with_counts.iter().enumerate() {
        let tag_payload = format!("{term} | occurrences={count}");
        let tag_cell = engine
            .create_cell(CellType::Tag, ContentFormat::Text, tag_payload.into_bytes())
            .unwrap();
        engine
            .add_fabric_cell(
                root.id,
                tag_cell.id,
                RelationType::Custom(TAGGED_AS_RELATION.to_string()),
                Some(idx as i64),
            )
            .unwrap();
        tag_ids.push(tag_cell.id);
    }

    StoredDocument {
        root_id: root.id,
        purpose_id: purpose.id,
        section_ids,
        reference_ids,
        tag_ids,
    }
}

fn get_active_content(conn: &Connection, id: Uuid) -> String {
    let now = now_ts();
    let bytes: Vec<u8> = conn
        .query_row(
            "SELECT content FROM cell
             WHERE id = ?1 AND valid_from <= ?2 AND valid_to > ?2
             LIMIT 1",
            params![id.to_string(), format_db_time(&now)],
            |row| row.get(0),
        )
        .unwrap();
    String::from_utf8_lossy(&bytes).into_owned()
}

fn query_children_by_relation(
    conn: &Connection,
    root_id: Uuid,
    relation: RelationType,
) -> Vec<Uuid> {
    let now = now_ts();
    let mut stmt = conn
        .prepare(
            "SELECT cell_id
             FROM fabric_cells
             WHERE fabric_id = ?1
               AND relation_type = ?2
               AND valid_from <= ?3
               AND valid_to > ?3
             ORDER BY ordinal ASC",
        )
        .unwrap();
    let relation_json = relation_type_json(relation);
    stmt.query_map(
        params![root_id.to_string(), relation_json, format_db_time(&now)],
        |row| {
            let cell_id: String = row.get(0)?;
            Ok(Uuid::parse_str(&cell_id).unwrap())
        },
    )
    .unwrap()
    .map(|row| row.unwrap())
    .collect::<Vec<_>>()
}

fn render_document_from_database(
    db_path: &Path,
    scenario: &DocumentScenario,
    root_id: Uuid,
) -> String {
    let conn = Connection::open(db_path).unwrap();

    let contains_ids = query_children_by_relation(&conn, root_id, RelationType::Contains);
    let purpose_id = contains_ids.first().copied().unwrap();
    let section_ids = contains_ids.into_iter().skip(1).collect::<Vec<_>>();
    let reference_ids = query_children_by_relation(&conn, root_id, RelationType::References);
    let tag_ids = query_children_by_relation(
        &conn,
        root_id,
        RelationType::Custom(TAGGED_AS_RELATION.to_string()),
    );

    let purpose = get_active_content(&conn, purpose_id);
    let sections = section_ids
        .into_iter()
        .map(|id| get_active_content(&conn, id))
        .collect::<Vec<_>>();
    let references = reference_ids
        .into_iter()
        .map(|id| get_active_content(&conn, id))
        .collect::<Vec<_>>();
    let tags = tag_ids
        .into_iter()
        .map(|id| get_active_content(&conn, id))
        .collect::<Vec<_>>();

    let mut rendered = String::new();
    rendered.push_str(&format!("# {}\n\n", scenario.title));
    rendered.push_str(&format!("Document Type: {}\n\n", scenario.document_type));
    rendered.push_str("## Purpose\n");
    rendered.push_str(&purpose);
    rendered.push_str("\n\n## Sections\n");
    for (title, section) in scenario.section_titles.iter().zip(sections.iter()) {
        rendered.push_str(&format!("### {title}\n"));
        rendered.push_str(section);
        rendered.push_str("\n\n");
    }
    rendered.push_str("## References\n");
    for reference in references {
        rendered.push_str(&format!("- {reference}\n"));
    }
    rendered.push_str("\n## Tags (Extracted Repeated Terms)\n");
    for tag in tags {
        let mut pieces = tag.split(" | occurrences=");
        let term = pieces.next().unwrap_or_default();
        let count = pieces.next().unwrap_or_default();
        rendered.push_str(&format!("- {term} (occurrences: {count})\n"));
    }
    rendered
}

fn write_db_snapshot(
    db_path: &Path,
    snapshot_path: &Path,
    scenario: &DocumentScenario,
    stored: &StoredDocument,
    generated: &GeneratedDocument,
) {
    let conn = Connection::open(db_path).unwrap();
    let now = now_ts();
    let active_fabric_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM fabric WHERE valid_from <= ?1 AND valid_to > ?1",
            params![format_db_time(&now)],
            |row| row.get(0),
        )
        .unwrap();
    let active_cell_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM cell WHERE valid_from <= ?1 AND valid_to > ?1",
            params![format_db_time(&now)],
            |row| row.get(0),
        )
        .unwrap();
    let active_edge_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM fabric_cells WHERE valid_from <= ?1 AND valid_to > ?1",
            params![format_db_time(&now)],
            |row| row.get(0),
        )
        .unwrap();

    let mut snapshot = String::new();
    snapshot.push_str("Scenario Snapshot\n");
    snapshot.push_str(&format!("case_id={}\n", scenario.case_id));
    snapshot.push_str(&format!("document_type={}\n", scenario.document_type));
    snapshot.push_str(&format!("shared_sqlite_file={SHARED_SCENARIO_DB}\n"));
    snapshot.push_str(&format!("root_id={}\n", stored.root_id));
    snapshot.push_str(&format!("purpose_id={}\n", stored.purpose_id));
    snapshot.push_str(&format!("section_count={}\n", stored.section_ids.len()));
    snapshot.push_str(&format!("reference_count={}\n", stored.reference_ids.len()));
    snapshot.push_str(&format!("tag_count={}\n", stored.tag_ids.len()));
    snapshot.push_str(&format!("target_words={}\n", scenario.target_words));
    snapshot.push_str(&format!(
        "actual_words={}\n",
        word_count(&generated.sections.join(" "))
    ));
    snapshot.push_str(&format!(
        "active_fabric_rows={} active_cell_rows={} active_edge_rows={}\n",
        active_fabric_count, active_cell_count, active_edge_count
    ));
    snapshot.push_str("\nExtracted repeated terms:\n");
    for (term, count) in &generated.tags_with_counts {
        snapshot.push_str(&format!("- {term}: {count}\n"));
    }

    fs::write(snapshot_path, snapshot).unwrap();
}

#[test]
fn scenario_all_cases_meaningful_roundtrip_with_shared_sqlite() {
    let scenarios = scenarios();

    let out_dir = scenario_output_dir();
    fs::create_dir_all(&out_dir).unwrap();

    let db_path = shared_db_path();
    if db_path.exists() {
        fs::remove_file(&db_path).unwrap();
    }

    let engine = Engine::new(db_path.to_str().unwrap()).unwrap();

    for scenario in &scenarios {
        let paths = case_paths(scenario);
        for artifact in [
            &paths.source_doc_path,
            &paths.rendered_doc_path,
            &paths.snapshot_path,
        ] {
            if artifact.exists() {
                fs::remove_file(artifact).unwrap();
            }
        }

        let generated = build_generated_document(scenario);
        fs::write(&paths.source_doc_path, &generated.source_markdown).unwrap();

        let stored = store_scenario_into_engine(&engine, scenario, &generated);
        write_db_snapshot(
            &db_path,
            &paths.snapshot_path,
            scenario,
            &stored,
            &generated,
        );

        let rendered = render_document_from_database(&db_path, scenario, stored.root_id);
        fs::write(&paths.rendered_doc_path, &rendered).unwrap();

        assert_eq!(
            word_count(&generated.sections.join(" ")),
            scenario.target_words
        );
        assert_eq!(generated.references.len(), scenario.reference_count);
        assert!(!generated.tags_with_counts.is_empty());
        assert_eq!(rendered, generated.source_markdown);
        assert!(paths.source_doc_path.exists());
        assert!(paths.rendered_doc_path.exists());
        assert!(paths.snapshot_path.exists());
    }

    assert!(db_path.exists());
}
