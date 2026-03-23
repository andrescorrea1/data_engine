use std::thread;
use crossbeam::channel::{self, Sender, Receiver};
use crate::dataframe::DataFrame;
use std::collections::HashMap;

pub type Row = HashMap<String, String>;

pub struct Pipeline {
    pub df: DataFrame,
}

impl Pipeline {
    pub fn new(df: DataFrame) -> Self {
        Pipeline { df }
    }

    /// Splits DataFrame into chunks and processes each concurrently
    pub fn run<F>(&self, chunk_size: usize, process_fn: F) -> Vec<Row>
    where
        F: Fn(Row) -> Option<Row> + Send + Sync + 'static,
    {
        let (tx, rx): (Sender<Row>, Receiver<Row>) = channel::unbounded();
        let process_fn = std::sync::Arc::new(process_fn);

        let chunks: Vec<Vec<Row>> = self.df.rows
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();

        let mut handles = vec![];

        for chunk in chunks {
            let tx = tx.clone();
            let process_fn = process_fn.clone();

            let handle = thread::spawn(move || {
                for row in chunk {
                    if let Some(processed) = process_fn(row) {
                        tx.send(processed).expect("Failed to send row");
                    }
                }
            });

            handles.push(handle);
        }

        // Drop original sender so receiver knows when all workers are done
        drop(tx);

        for handle in handles {
            handle.join().expect("Worker thread panicked");
        }

        rx.iter().collect()
    }
}