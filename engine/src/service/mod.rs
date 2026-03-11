// Copyright 2026 Arion Yau
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

pub mod context;

use uuid::Uuid;

use crate::document::extensions::{dictionary, table, text};
use crate::document::{Document, DocumentKind};
use crate::engine::Engine;
use crate::error::{EngineError, Result};
use crate::models::{CellType, ContentFormat, RelationType, Timestamp};
use context::ServiceContext;

#[derive(Debug, Clone)]
pub struct DictionaryEntryInput {
    pub key: String,
    pub values: Vec<String>,
}

pub trait ServiceApi {
    fn create_document_context(&self, kind: DocumentKind) -> Result<ServiceContext>;
    fn open_context(&self, document_id: Uuid, timestamp: Option<Timestamp>) -> Result<ServiceContext>;
    fn detect_kind(&self, document_id: Uuid) -> Result<DocumentKind>;
}

pub struct Service<'a> {
    engine: &'a Engine,
}

impl<'a> Service<'a> {
    pub fn new(engine: &'a Engine) -> Self {
        Self { engine }
    }

    pub fn append_text_segment(&self, context: &ServiceContext, segment: &str) -> Result<Uuid> {
        self.ensure_kind(context, DocumentKind::Text)?;
        let cell = self.engine.create_cell(
            CellType::Data,
            ContentFormat::Text,
            segment.as_bytes().to_vec(),
        )?;
        self.engine.add_fabric_cell(
            context.document.root_cell_id,
            cell.id,
            text::contains_relation(),
            None,
        )?;
        Ok(cell.id)
    }

    pub fn add_table_cell(
        &self,
        context: &ServiceContext,
        row: i64,
        col: i64,
        value: &str,
    ) -> Result<Uuid> {
        self.ensure_kind(context, DocumentKind::Table)?;
        if row < 0 || col < 0 {
            return Err(EngineError::InvalidData(
                "table row and column must be non-negative".to_string(),
            ));
        }

        let ordinal = table::encode_ordinal(row, col)
            .ok_or_else(|| EngineError::InvalidData("table row/column overflow".to_string()))?;

        let cell = self.engine.create_cell(
            CellType::Data,
            ContentFormat::Text,
            value.as_bytes().to_vec(),
        )?;
        self.engine.add_fabric_cell(
            context.document.root_cell_id,
            cell.id,
            table::table_cell_relation(),
            Some(ordinal),
        )?;
        Ok(cell.id)
    }

    pub fn add_dictionary_entry(
        &self,
        context: &ServiceContext,
        entry: &DictionaryEntryInput,
    ) -> Result<Uuid> {
        self.ensure_kind(context, DocumentKind::Dictionary)?;

        let key_cell = self.engine.create_cell(
            CellType::Keyword,
            ContentFormat::Text,
            entry.key.as_bytes().to_vec(),
        )?;
        self.engine.add_fabric_cell(
            context.document.root_cell_id,
            key_cell.id,
            dictionary::key_relation(),
            None,
        )?;

        for value in &entry.values {
            let value_cell = self.engine.create_cell(
                CellType::Data,
                ContentFormat::Text,
                value.as_bytes().to_vec(),
            )?;
            self.engine.add_fabric_cell(
                key_cell.id,
                value_cell.id,
                dictionary::value_relation(),
                None,
            )?;
        }

        Ok(key_cell.id)
    }

    pub fn transform_dictionary_to_text(&self, context: &ServiceContext) -> Result<ServiceContext> {
        self.ensure_kind(context, DocumentKind::Dictionary)?;

        let text_ctx = self.create_document_context(DocumentKind::Text)?;
        let key_ids = self
            .engine
            .get_cells_by_relation(context.document.root_cell_id, dictionary::key_relation())?;

        for key_id in key_ids {
            let key = self.engine.get_current(key_id)?;
            let values = self
                .engine
                .get_cells_by_relation(key_id, dictionary::value_relation())?;

            for value_id in values {
                let value = self.engine.get_current(value_id)?;
                let line = format!(
                    "{}: {}",
                    String::from_utf8_lossy(&key.content),
                    String::from_utf8_lossy(&value.content)
                );
                self.append_text_segment(&text_ctx, &line)?;
            }
        }

        self.engine.add_fabric_cell(
            text_ctx.document.root_cell_id,
            context.document.root_cell_id,
            RelationType::DerivesFrom,
            None,
        )?;
        self.engine.add_fabric_cell(
            context.document.root_cell_id,
            text_ctx.document.root_cell_id,
            dictionary::derived_text_relation(),
            None,
        )?;

        Ok(text_ctx)
    }

    fn ensure_kind(&self, context: &ServiceContext, expected: DocumentKind) -> Result<()> {
        if context.document.kind == expected {
            return Ok(());
        }
        Err(EngineError::InvalidData(format!(
            "document {} is kind {:?}, expected {:?}",
            context.document.root_cell_id, context.document.kind, expected
        )))
    }
}

impl ServiceApi for Service<'_> {
    fn create_document_context(&self, kind: DocumentKind) -> Result<ServiceContext> {
        let root = self
            .engine
            .create_cell(kind.as_cell_type(), ContentFormat::Json, vec![])?;
        let document = Document::from_cell(&root).ok_or_else(|| {
            EngineError::InvalidData("root does not use a document.<kind> cell type".to_string())
        })?;

        Ok(ServiceContext::new(document, None))
    }

    fn open_context(&self, document_id: Uuid, timestamp: Option<Timestamp>) -> Result<ServiceContext> {
        let root = self.engine.get_cell_at(document_id, timestamp)?;
        let document = Document::from_cell(&root).ok_or_else(|| {
            EngineError::InvalidData(format!(
                "document {document_id} does not use a supported document.<kind> cell_type"
            ))
        })?;

        Ok(ServiceContext::new(document, timestamp))
    }

    fn detect_kind(&self, document_id: Uuid) -> Result<DocumentKind> {
        Ok(self.open_context(document_id, None)?.document.kind)
    }
}
