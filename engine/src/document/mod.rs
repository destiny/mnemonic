// Copyright 2026 Arion Yau
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use crate::models::{Cell, CellType};
use uuid::Uuid;

pub mod extensions;

const DOCUMENT_PREFIX: &str = "document.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentKind {
    Generic,
    Text,
    Table,
    Dictionary,
    List,
    Map,
}

impl DocumentKind {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::Text => "text",
            Self::Table => "table",
            Self::Dictionary => "dictionary",
            Self::List => "list",
            Self::Map => "map",
        }
    }

    pub fn as_cell_type(self) -> CellType {
        CellType::Custom(format!("{DOCUMENT_PREFIX}{}", self.as_label()))
    }

    pub fn parse(cell_type: &CellType) -> Option<Self> {
        let CellType::Custom(value) = cell_type else {
            return None;
        };

        match value.as_str() {
            "document.generic" => Some(Self::Generic),
            "document.text" => Some(Self::Text),
            "document.table" => Some(Self::Table),
            "document.dictionary" => Some(Self::Dictionary),
            "document.list" => Some(Self::List),
            "document.map" => Some(Self::Map),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Document {
    pub root_cell_id: Uuid,
    pub kind: DocumentKind,
}

impl Document {
    pub fn from_cell(cell: &Cell) -> Option<Self> {
        Some(Self {
            root_cell_id: cell.id,
            kind: DocumentKind::parse(&cell.cell_type)?,
        })
    }
}
