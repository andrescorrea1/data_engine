mod dataframe;
mod pipeline;
use dataframe::DataFrame;
use pipeline::Pipeline;


fn main() {
    // Load CSV
    let df = DataFrame::read_csv("Player.csv").expect("Failed to read CSV");
    println!("Loaded dataset: {:?} rows x {:?} cols", df.shape().0, df.shape().1);

    // Clean
    let df = df.remove_empty_rows(); //Shadows the original df variable with a new one that has empty rows removed.
    println!("After removing empty rows: {:?}", df.shape().0);

    // Filter example — adjust column name to match your CSV
    let filtered = df.filter(|row| {
        row.get("height")
        .and_then(|v| v.parse::<f64>().ok())
        .map(|h| h > 180.0)
        .unwrap_or(false)
    });
    println!("After filter: {:?} rows", filtered.shape().0);

    //Retrun height before running the pipeline
    if let Some(heights) = filtered.get_column("height") {
        println!("Heights before pipeline: {:?}", heights.iter().take(5).collect::<Vec<_>>());
    }
    //Slect the 50th id from the data set
    println!("\n50th id: {:?}", filtered.get_cell(49, "id").unwrap_or(&"Not found".to_string())); 


    // Run concurrent pipeline
    let pipeline = Pipeline::new(filtered); // Pipeline takes ownership of the filtered DataFrame
    let results = pipeline.run(500, |mut row| {
        // Process each row — Convert height to inches
        if let Some(value) = row.get_mut("height") {
            let height: f64 = value.parse::<f64>().expect("Failed to parse number");
            let height:f64 = height * 0.393701; //Variable shadowing
            *value = height.to_string();
        }
        Some(row)
    });

    println!("Pipeline output: {} rows", results.shape().0);
    results.head(5); // Print first 5 rows of results 
    //filtered.head(5); //This won't work because the pipeline takes ownership of the df.  
    //Return the heights after running the pipeline
    if let Some(heights) = results.get_column("height") {
        println!("\nHeights after pipeline: {:?}", heights.iter().take(5).collect::<Vec<_>>());
    }

    //Select the 50 id from the results data set
    println!("\n50th id after pipeline: {:?}", results.get_cell(49, "id").unwrap_or(&"Not found".to_string()));
}





//for multiple files used claude
/*

let paths = vec!["data1.csv", "data2.csv", "data3.csv"];
let df = paths.iter()
    .map(|p| DataFrame::read_csv(p).expect("Failed to read"))
    .reduce(|mut a, b| { a.rows.extend(b.rows); a })
    .unwrap();
*/