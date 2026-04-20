use std::collections::HashMap;
//These functions were originally written by Copilot, and were then translated into code I could better understand/explain.

// Remove rows where height <= 180.0
pub fn filter_height(row: &HashMap<String, String>) -> bool {
    let mut height = row.get("height");
    if let Some(h) = height {
        let h: f64 = h.parse::<f64>().expect("Unable to convert height to floating point");
        if h > 180.0 {
                true 
        } else {
            false
        }
    } else {
        panic!("Unable to get height value")
    }
    
}

// Remove rows where weight <= 160.0
pub fn filter_weight(row: &HashMap<String, String>) -> bool {
    let mut weight = row.get("weight");
    if let Some(w) = weight {
        let w = w.parse::<f64>().expect("Unable to convert height to floating point");
        if w > 160.0 {
            true
        } else {
            false
        }
    } else {
        panic!("Unable to get weight value");
    }
}

// Remove rows where the player's first name doesn't start with "A"
pub fn filter_name(row: &HashMap<String, String>) -> bool {
    let name = row.get("player_name");
    if let Some(n) = name {
        if n.starts_with("A") {
            false
        } else {
            true
        }
    } else {
        panic!("Unable to get name value");
    }
}

// Keep only players born after 1990
pub fn filter_birth_year(row: &HashMap<String, String>) -> bool {
    let birthday = row.get("birthday");
    if let Some(b) = birthday {
        // Birthday format is "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS"
        let year: i32 = b[..4].parse::<i32>().expect("Unable to parse birth year");
        year > 1990
    } else {
        panic!("Unable to get birthday value");
    }
}