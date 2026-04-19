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
            .filter(|row| predicate(row)) //Keeps only rows where predicate returns true
            .cloned() //Clones the result
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
        println!("Printing the first {n} rows of the DataFrame:");
        for column in self.columns.iter() {
            print!("{:?}: ", column);
            for rown in self.rows.iter().take(n) {
                print!("{:?}, ", rown.get(column).unwrap_or(&"".to_string()));
            }
            println!();
        }
    }

    
    // Copilot assisted with the following function
    pub fn get_column(&self, name: &str) -> Option<Vec<String>> {
        if self.columns.contains(&name.to_string()) {
            Some (
                self.rows.iter().map(|row| row.get(name).cloned().unwrap_or_default()).collect()
            )
        } else {
            None
        }
    } 

    pub fn get_cell(&self, row_idx: usize, column: &str) ->Option<&String> {
        self.rows.get(row_idx)?.get(column)
    }
}