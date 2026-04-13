use crate::dataframe::DataFrame;
use crossbeam::channel::{self, Receiver, Sender};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub type Row = HashMap<String, String>;

#[derive(Clone)]
struct ThreadEvent {
    thread_id: usize,
    rows_done: usize,
    chunk_size: usize,
    finished: bool,
}

pub struct Pipeline {
    pub df: DataFrame,
}

impl Pipeline {
    pub fn new(df: DataFrame) -> Self {
        Pipeline { df }
    }

    pub fn run<F>(&self, chunk_size: usize, process_fn: F) -> DataFrame
    where
        F: Fn(Row) -> Option<Row> + Send + Sync + 'static,
    {
        let (tx, rx): (Sender<Row>, Receiver<Row>) = channel::unbounded();
        let process_fn = Arc::new(process_fn);

        let chunks: Vec<Vec<Row>> = self
            .df
            .rows
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();

        let num_threads = chunks.len();
        let log: Arc<Mutex<Vec<ThreadEvent>>> = Arc::new(Mutex::new(
            (0..num_threads)
                .map(|i| ThreadEvent {
                    thread_id: i,
                    rows_done: 0,
                    chunk_size: chunks.get(i).map(|c| c.len()).unwrap_or(0),
                    finished: false,
                })
                .collect(),
        ));

        // Print header
        println!("\n  Concurrent pipeline — {} threads\n", num_threads);
        for i in 0..num_threads {
            println!("  T{i}  [                                        ] 0%");
        }

        let log_printer = log.clone();
        let start = Instant::now();

        // Printer thread: redraws progress bars every 50ms
        let printer = thread::spawn(move || {
            loop {
                let events = log_printer.lock().unwrap().clone();
                let all_done = events.iter().all(|e| e.finished);

                // Move cursor up N lines to overwrite
                print!("\x1B[{}A", num_threads);

                for e in &events {
                    let pct = if e.chunk_size == 0 {
                        100
                    } else {
                        (e.rows_done * 100) / e.chunk_size
                    };
                    let filled = pct * 40 / 100;
                    let bar: String = (0..40)
                        .map(|i| if i < filled { '#' } else { ' ' })
                        .collect();
                    let elapsed = start.elapsed().as_millis();
                    let status = if e.finished { "done" } else { "working" };
                    println!(
                        "  T{}  [{}] {:3}%  {:>7} rows  {:>5}ms  {}",
                        e.thread_id, bar, pct, e.rows_done, elapsed, status
                    );
                }

                if all_done {
                    break;
                }
                thread::sleep(Duration::from_millis(50));
            }
        });

        let mut handles = vec![];

        for (idx, chunk) in chunks.into_iter().enumerate() {
            let tx = tx.clone();
            let process_fn = process_fn.clone();
            let log = log.clone();
            let chunk_len = chunk.len();

            let handle = thread::spawn(move || {
                for (i, row) in chunk.into_iter().enumerate() {
                    // Artificial delay
                    thread::sleep(Duration::from_millis(2)); // adjust this

                    if let Some(processed) = process_fn(row) {
                        tx.send(processed).expect("Failed to send row");
                    }

                    let report_every = (chunk_len / 20).max(1);
                    if i % report_every == 0 || i == chunk_len - 1 {
                        let mut log = log.lock().unwrap();
                        log[idx].rows_done = i + 1;
                    }
                }

                let mut log = log.lock().unwrap();
                log[idx].rows_done = chunk_len;
                log[idx].finished = true;
            });

            handles.push(handle);
        }

        drop(tx);

        for handle in handles {
            handle.join().expect("Worker thread panicked");
        }

        printer.join().expect("Printer thread panicked");

        println!("\n  All threads done in {}ms\n", start.elapsed().as_millis());

        let rows = rx.iter().collect();
        let mut updated_df = self.df.clone();
        updated_df.rows = rows;
        updated_df
    }
}