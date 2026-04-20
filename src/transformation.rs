use std::collections::HashMap;
//These functions were originally written by Copilot, and were then translated into code I could better understand/explain.

// Convert height from cm to inches
pub fn pipeline_height_to_inches(mut row: HashMap<String, String>) -> Option<HashMap<String, String>> {
    if let Some(value) = row.get_mut("height") {
        let height: f64 = value.parse::<f64>().expect("Failed to parse number");
        let height: f64 = height * 0.393701; //Variable shadowing
        *value = format!("{:.2}", height)//* is necessary because value is a mutable reference
    } else {
        return None;
    }
    Some(row)
}

//Convert weight from lbs to kg
pub fn pipeline_weight_to_kg(mut row: HashMap<String, String>) -> Option<HashMap<String, String>> {
    if let Some(value) = row.get_mut("weight") {
        let weight: f64 = value.parse::<f64>().expect("Failed to parse number");
        let weight: f64 = weight * 0.453582;
        *value = format!("{:.2}", weight);
    } else {
        return None;
    }
    Some(row)
}

// Remove timestamp from birthday column
pub fn pipeline_remove_birthday_timestamp(mut row: HashMap<String, String>) -> Option<HashMap<String, String>> {
    if let Some(value) = row.get_mut("birthday") {
            let date = value.split(' ').next().unwrap_or(""); //Next takes the first item from the iterator created by .split(' ')
            *value = date.to_string();
    } else {
        return None;
    }
    Some(row)
}

// Calculate BMI using height (inches) and weight (kg) after conversions have been applied
// BMI formula with these units: (weight_kg / (height_inches * 0.0254)^2)
pub fn pipeline_calculate_bmi(mut row: HashMap<String, String>) -> Option<HashMap<String, String>> {
    let height = row.get("height")?.parse::<f64>().ok()?;
    let weight = row.get("weight")?.parse::<f64>().ok()?;
    let height_m = height * 0.0254; // convert inches to meters
    let bmi = weight / (height_m * height_m);
    row.insert("bmi".to_string(), format!("{:.2}", bmi));
    Some(row)
}