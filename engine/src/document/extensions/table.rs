use crate::models::RelationType;

pub const TABLE_CELL_RELATION: &str = "table.cell";

pub fn table_cell_relation() -> RelationType {
    RelationType::Custom(TABLE_CELL_RELATION.to_string())
}

pub fn encode_ordinal(row: i64, col: i64) -> Option<i64> {
    row.checked_mul(1_000_000)
        .and_then(|base| base.checked_add(col))
}
