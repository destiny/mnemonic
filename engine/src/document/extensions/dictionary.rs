use crate::models::RelationType;

pub const DICTIONARY_KEY_RELATION: &str = "dictionary.key";
pub const DICTIONARY_VALUE_RELATION: &str = "dictionary.value";
pub const DERIVED_TEXT_RELATION: &str = "document.derived_text";

pub fn key_relation() -> RelationType {
    RelationType::Custom(DICTIONARY_KEY_RELATION.to_string())
}

pub fn value_relation() -> RelationType {
    RelationType::Custom(DICTIONARY_VALUE_RELATION.to_string())
}

pub fn derived_text_relation() -> RelationType {
    RelationType::Custom(DERIVED_TEXT_RELATION.to_string())
}
