use std::collections::HashMap;
use csv::ReaderBuilder;

#[derive(Debug, Clone)]
pub struct DataFrame {
    pub columns: Vec<String>,
    pub rows: Vec<HashMap<String, String>>,
}

impl DataFrame {
    pub fn new() -> Self {
        DataFrame {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub fn read_csv(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_path(path)?;

        let headers: Vec<String> = rdr.headers()?
            .iter()
            .map(|h| h.to_string())
            .collect();

        let mut rows = Vec::new();

        for result in rdr.records() {
            let record = result?;
            let row: HashMap<String, String> = headers
                .iter()
                .zip(record.iter())
                .map(|(k, v)| (k.clone(), v.to_string()))
                .collect();
            rows.push(row);
        }

        Ok(DataFrame { columns: headers, rows })
    }

    pub fn remove_empty_rows(&self) -> Self {
        let rows = self.rows
            .iter()
            .filter(|row| {
                row.values().all(|v| !v.trim().is_empty())
            })
            .cloned()
            .collect();

        DataFrame {
            columns: self.columns.clone(),
            rows,
        }
    }

    pub fn filter<F>(&self, predicate: F) -> Self
    where
        F: Fn(&HashMap<String, String>) -> bool,
    {
        let rows = self.rows
            .iter()
            .filter(|row| predicate(row))
            .cloned()
            .collect();

        DataFrame {
            columns: self.columns.clone(),
            rows,
        }
    }

    pub fn shape(&self) -> (usize, usize) {
        (self.rows.len(), self.columns.len())
    }

    pub fn head(&self, n: usize) {
        println!("Columns: {:?}", self.columns);
        for row in self.rows.iter().take(n) {
            println!("{:?}", row);
        }
    }
}