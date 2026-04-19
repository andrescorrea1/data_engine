use std::collections::HashMap;

// Remove rows where height <= 180.0
pub fn filter_height(row: &HashMap<String, String>) -> bool {
    row.get("height").
    and_then(|v| v.parse::<f64>().ok())
    .map(|h| h > 180.0)
    .unwrap_or(false)
}

// Remove rows where weight <= 160.0
pub fn filter_weight(row: &HashMap<String, String>) -> bool {
    row.get("weight")
    .and_then(|v| v.parse::<f64>().ok())
    .map(|w| w > 160.0)
    .unwrap_or(false)
}

// Rmove rows where the player's first name doesn't starts with "A"
pub fn filter_name(row: &HashMap<String, String>) -> bool {
    row.get("player_name")
    .map(|name| name.starts_with("A"))
    .unwrap_or(false)
}