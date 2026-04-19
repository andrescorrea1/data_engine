mod dataframe;
mod pipeline;
mod filter;
mod transformation;
use dataframe::DataFrame;
use pipeline::Pipeline;
use std::collections::HashMap;
use filter::*;
use transformation::*;


fn main() {
    /*Preprocessing and filtering steps before running the pipeline.
        - Loading the CSV file
        - Cleaning the data 
        - Filtering (Including list of closure variables)
    */ 

    // Load CSV
    println!("\n\x1b[2m────────────────────────────────────────\x1b[0m");
    println!("\x1b[1;35mPreprocessing and filtering steps\x1b[0m");
    println!("\x1b[2m────────────────────────────────────────\x1b[0m");
    let df = DataFrame::read_csv("Player.csv").expect("Failed to read CSV");
    println!("\n\x1b[1;36mLoaded dataset:\x1b[0m {:?} rows x {:?} cols", df.shape().0, df.shape().1);

    // Clean
    println!("\n\x1b[1;36mBefore removing empty rows:\x1b[0m {:?} rows", df.shape().0);
    let df = df.remove_empty_rows(); //Shadows the original df variable with a new one that has empty rows removed.
    println!("\x1b[1;36mAfter removing empty rows:\x1b[0m {:?}", df.shape().0);

    // Filter example  
    println!("\n\x1b[1;36mBefore filter:\x1b[0m {:?} rows", df.shape().0);

    let filtered = df.filter(filter_name); 

    println!("\x1b[1;36mAfter filter:\x1b[0m {:?} rows", filtered.shape().0);

    /* Pipeline execution:
        - Run the concurrent pipeline on the filtered DataFrame.
        - Demonstrate the results by printing the before and after values of the feature that has been changed.
        - Demonstrate concurrency by showing that the order of the rows has changed after running the pipeline. 
    */

    println!("\n\x1b[2m────────────────────────────────────────\x1b[0m");
    println!("\x1b[1;35mRunning the concurrent pipeline\x1b[0m");
    println!("\x1b[2m────────────────────────────────────────\x1b[0m");

    //Retrun height before running the pipeline
    if let Some(heights) = filtered.get_column("height") {
        println!("\n\x1b[1;36mHeights before pipeline:\x1b[0m {:?}", heights.iter().take(5).collect::<Vec<_>>());
    }

    //Return Birthday before running the pipeline
    if let Some(birthdays) = filtered.get_column("birthday") {
        println!("\n\x1b[1;36mBirthdays before pipeline:\x1b[0m {:?}", birthdays.iter().take(5).collect::<Vec<_>>());
    }

    //Return Weight before running the pipeline
    if let Some(weights) = filtered.get_column("weight") {
        println!("\n\x1b[1;36mWeights before pipeline:\x1b[0m {:?}", weights.iter().take(5).collect::<Vec<_>>());
    }

    //Slect the 50th id from the data set
    println!("\n\x1b[1;36m50th id before pipeline:\x1b[0m {:?}", filtered.get_cell(49, "id").unwrap_or(&"Not found".to_string()));


    // Run concurrent pipeline
    let pipeline = Pipeline::new(filtered); // Pipeline takes ownership of the filtered DataFrame
    let results = pipeline.run(500, pipeline_remove_birthday_timestamp); // Run the pipeline with a chunk size of 500 and a transformation function that removes timestamps from the birthday column.
    let pipeline = Pipeline::new(results); // Pipeline takes ownership of the results DataFrame
    let results = pipeline.run(500, pipeline_height_to_inches); // Run the pipeline with a chunk size of 500 and a transformation function that converts height from cm to inches.
    let pipeline = Pipeline::new(results);
    let results = pipeline.run(500, pipeline_weight_to_kg); // Run the pipeline with a chunk size of 500 and a transformation function that converts weight from lbs to kg.
    //println!("Pipeline output: {} rows", results.shape().0);
    //results.head(5); // Print first 5 rows of results 
    //filtered.head(5); //This won't work because the pipeline takes ownership of the df.  
    
    //Return the heights after running the pipeline
    if let Some(heights) = results.get_column("height") {
        println!("\n\x1b[1;36mHeights after pipeline:\x1b[0m {:?}", heights.iter().take(5).collect::<Vec<_>>());
    }

    //Return the birthdays after running the pipeline
    if let Some(birthdays) = results.get_column("birthday") {
        println!("\n\x1b[1;36mBirthdays after pipeline:\x1b[0m {:?}", birthdays.iter().take(5).collect::<Vec<_>>());
    }

    //Return the weights after running the pipeline
    if let Some(weights) = results.get_column("weight") {
        println!("\n\x1b[1;36mWeights after pipeline:\x1b[0m {:?}", weights.iter().take(5).collect::<Vec<_>>());
    }

    //Select the 50 id from the results data set
    println!("\n\x1b[1;36m50th id after pipeline:\x1b[0m {:?}", results.get_cell(49, "id").unwrap_or(&"Not found".to_string()));

    
}





//for multiple files used claude
/*

let paths = vec!["data1.csv", "data2.csv", "data3.csv"];
let df = paths.iter()
    .map(|p| DataFrame::read_csv(p).expect("Failed to read"))
    .reduce(|mut a, b| { a.rows.extend(b.rows); a })
    .unwrap();
*/