mod dataframe;
mod pipeline;
use dataframe::DataFrame;
use pipeline::Pipeline;

fn main() {
    // Load CSV
    let df = DataFrame::read_csv("Player.csv").expect("Failed to read CSV");
    println!("Loaded dataset: {:?} rows x {:?} cols", df.shape().0, df.shape().1);

    // Clean
    let df = df.remove_empty_rows();
    println!("After removing empty rows: {:?}", df.shape().0);

    // Filter example — adjust column name to match your CSV
    let filtered = df.filter(|row| {
        row.get("height")
        .and_then(|v| v.parse::<f64>().ok())
        .map(|h| h > 180.0)
        .unwrap_or(false)
    });
    println!("After filter: {:?} rows", filtered.shape().0);

    // Run concurrent pipeline
    let pipeline = Pipeline::new(filtered);
    let results = pipeline.run(500, |row| {
        // Process each row — transform, compute, etc.
        Some(row)
    });

    println!("Pipeline output: {} rows", results.rows.len());
    
    for row in results.rows.iter().take(5) {
        println!("{:?}", row)
     }


}





//for multiple files used claude
/*

let paths = vec!["data1.csv", "data2.csv", "data3.csv"];
let df = paths.iter()
    .map(|p| DataFrame::read_csv(p).expect("Failed to read"))
    .reduce(|mut a, b| { a.rows.extend(b.rows); a })
    .unwrap();
*/