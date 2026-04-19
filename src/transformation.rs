use std::collections::HashMap;

// Convert height from cm to inches
pub fn pipeline_height_to_inches(mut row: HashMap<String, String>) -> Option<HashMap<String, String>> {
    if let Some(value) = row.get_mut("height") {
        let height: f64 = value.parse::<f64>().expect("Failed to parse number");
        let height: f64 = height * 0.393701;
        *value = height.to_string();
    }
    Some(row)
}

//Convert weight from lbs to kg
pub fn pipeline_weight_to_kg(mut row: HashMap<String, String>) -> Option<HashMap<String, String>> {
    if let Some(value) = row.get_mut("weight") {
        let weight: f64 = value.parse::<f64>().expect("Failed to parse number");
        let weight: f64 = weight * 0.453582;
        *value = weight.to_string();
    }
    Some(row)
}

// Remove timestamp from birthday column
pub fn pipeline_remove_birthday_timestamp(mut row: HashMap<String, String>) -> Option<HashMap<String, String>> {
    if let Some(value) = row.get_mut("birthday") {
           let data = value.split(' ').next().unwrap_or("");
            *value = data.to_string();
    }
    Some(row)
} 