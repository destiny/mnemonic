use crate::document::Document;

#[derive(Debug, Clone)]
pub struct ServiceContext {
    pub document: Document,
    pub timestamp: Option<i64>,
}

impl ServiceContext {
    pub fn new(document: Document, timestamp: Option<i64>) -> Self {
        Self {
            document,
            timestamp,
        }
    }
}
