use std::collections::HashMap;
//These functions were originally written by Copilot, and were then translated into code I could better understand/explain.

// Height filter with user-defined threshold
pub fn filter_height(min_height: f64) -> impl Fn(&HashMap<String, String>) -> bool {
    move |row| {
        if let Some(h) = row.get("height") {
            if let Ok(h) = h.parse::<f64>() {
                return h > min_height;
            }
        }
        false
    }
}

// Weight filter with user-defined threshold
pub fn filter_weight(min_weight: f64) -> impl Fn(&HashMap<String, String>) -> bool {
    move |row| {
        if let Some(w) = row.get("weight") {
            if let Ok(w) = w.parse::<f64>() {
                return w > min_weight;
            }
        }
        false
    }
}

// Name filter with user-defined starting letter
pub fn filter_name(starts_with: String) -> impl Fn(&HashMap<String, String>) -> bool {
    move |row| {
        if let Some(name) = row.get("player_name") {
            return !name.starts_with(&starts_with);
        }
        false
    }
}

// Birth year filter with user-defined cutoff
pub fn filter_birth_year(min_year: i32) -> impl Fn(&HashMap<String, String>) -> bool {
    move |row| {
        if let Some(b) = row.get("birthday") {
            if let Ok(year) = b[..4].parse::<i32>() {
                return year > min_year;
            }
        }
        false
    }
}