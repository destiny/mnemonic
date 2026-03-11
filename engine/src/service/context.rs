use crate::document::Document;
use crate::models::Timestamp;

#[derive(Debug, Clone)]
pub struct ServiceContext {
    pub document: Document,
    pub timestamp: Option<Timestamp>,
}

impl ServiceContext {
    pub fn new(document: Document, timestamp: Option<Timestamp>) -> Self {
        Self {
            document,
            timestamp,
        }
    }
}
