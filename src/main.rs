mod dataframe;
mod pipeline;
mod filter;
mod transformation;
use dataframe::DataFrame;
use pipeline::Pipeline;
use std::collections::HashMap;
use std::io::{self, Write};
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

    // Filter selection
    println!("\n\x1b[1;35mSelect a filter to apply:\x1b[0m");
    println!("  \x1b[1;36m1.\x1b[0m Filter by weight    (keep rows where weight > 160.0)");
    println!("  \x1b[1;36m2.\x1b[0m Filter by height    (keep rows where height > 180.0)");
    println!("  \x1b[1;36m3.\x1b[0m Filter by name      (remove rows where player name starts with 'A')");
    println!("  \x1b[1;36m4.\x1b[0m Filter by birth year (keep players born after 1990)");
    print!("\nEnter choice (1/2/3/4): ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();

    println!("\n\x1b[1;36mBefore filter:\x1b[0m {:?} rows", df.shape().0);

    let filtered = match input.trim() {
        "1" => {
            println!("\x1b[2mApplying: filter_weight\x1b[0m");
            df.filter(filter_weight)
        }
        "2" => {
            println!("\x1b[2mApplying: filter_height\x1b[0m");
            df.filter(filter_height)
        }
        "3" => {
            println!("\x1b[2mApplying: filter_name\x1b[0m");
            df.filter(filter_name)
        }
        "4" => {
            println!("\x1b[2mApplying: filter_birth_year\x1b[0m");
            df.filter(filter_birth_year)
        }
        _ => {
            println!("\x1b[1;33mInvalid choice, defaulting to filter_weight.\x1b[0m");
            df.filter(filter_weight)
        }
    };

    println!("\x1b[1;36mAfter filter:\x1b[0m {:?} rows", filtered.shape().0);

    /* Pipeline execution:
        - Run the concurrent pipeline on the filtered DataFrame.
        - Demonstrate the results by printing the before and after values of the feature that has been changed.
        - Demonstrate concurrency by showing that the order of the rows has changed after running the pipeline. 
    */

    println!("\n\x1b[2m────────────────────────────────────────\x1b[0m");
    println!("\x1b[1;35mRunning the concurrent pipeline\x1b[0m");
    println!("\x1b[2m────────────────────────────────────────\x1b[0m");

    //Return height before running the pipeline
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

    //Select the 50th id from the data set
    println!("\n\x1b[1;36m50th id before pipeline:\x1b[0m {:?}", filtered.get_cell(49, "id").unwrap_or(&"Not found".to_string()));

    // Snapshot of first row before pipeline for summary
    let before_birthday = filtered.get_cell(0, "birthday").cloned().unwrap_or_default();
    let before_height   = filtered.get_cell(0, "height").cloned().unwrap_or_default();
    let before_weight   = filtered.get_cell(0, "weight").cloned().unwrap_or_default();

    // Run concurrent pipeline
    let pipeline = Pipeline::new(filtered); // Pipeline takes ownership of the filtered DataFrame
    let results = pipeline.run(500, pipeline_remove_birthday_timestamp); // Run the pipeline with a chunk size of 500 and a transformation function that removes timestamps from the birthday column.
    let pipeline = Pipeline::new(results); // Pipeline takes ownership of the results DataFrame
    let results = pipeline.run(500, pipeline_height_to_inches); // Run the pipeline with a chunk size of 500 and a transformation function that converts height from cm to inches.
    let pipeline = Pipeline::new(results);
    let results = pipeline.run(500, pipeline_weight_to_kg); // Run the pipeline with a chunk size of 500 and a transformation function that converts weight from lbs to kg.
    let pipeline = Pipeline::new(results);
    let results = pipeline.run(500, pipeline_calculate_bmi); // BMI runs last since it needs converted height and weight
    // This line is commented out because it will cause a compile time error. 
    // This error is useful for demonstrating how ownership works in Rust, so the line is left here for demonstration purposes. 
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

    //Select the 50th id from the results data set
    println!("\n\x1b[1;36m50th id after pipeline:\x1b[0m {:?}", results.get_cell(49, "id").unwrap_or(&"Not found".to_string()));

    // Pipeline summary
    let after_birthday = results.get_cell(0, "birthday").cloned().unwrap_or_default();
    let after_height   = results.get_cell(0, "height").cloned().unwrap_or_default();
    let after_weight   = results.get_cell(0, "weight").cloned().unwrap_or_default();
    let after_bmi      = results.get_cell(0, "bmi").cloned().unwrap_or_default();

    println!("\n\x1b[2m────────────────────────────────────────\x1b[0m");
    println!("\x1b[1;35mPipeline Summary (row 1)\x1b[0m");
    println!("\x1b[2m────────────────────────────────────────\x1b[0m");
    println!("\x1b[2m{:<12} {:<25} {}\x1b[0m", "Column", "Before", "After");
    println!("\x1b[2m────────────────────────────────────────\x1b[0m");
    println!("{:<12} {:<25} {}", "birthday", before_birthday, after_birthday);
    println!("{:<12} {:<25} {}", "height",   before_height,   after_height);
    println!("{:<12} {:<25} {}", "weight",   before_weight,   after_weight);
    println!("{:<12} {:<25} {}", "bmi",      "N/A",           after_bmi);
    println!("\x1b[2m────────────────────────────────────────\x1b[0m");

    // BMI table for first 5 players
    println!("\n\x1b[2m────────────────────────────────────────────────────────────────\x1b[0m");
    println!("\x1b[1;35mBMI Calculations (first 5 players)\x1b[0m");
    println!("\x1b[2m────────────────────────────────────────────────────────────────\x1b[0m");
    println!("\x1b[2m{:<25} {:<12} {:<12} {}\x1b[0m", "Player", "Height(in)", "Weight(kg)", "BMI");
    println!("\x1b[2m────────────────────────────────────────────────────────────────\x1b[0m");
    for i in 0..5 {
        let name   = results.get_cell(i, "player_name").cloned().unwrap_or_default();
        let height = results.get_cell(i, "height").cloned().unwrap_or_default();
        let weight = results.get_cell(i, "weight").cloned().unwrap_or_default();
        let bmi    = results.get_cell(i, "bmi").cloned().unwrap_or_default();
        println!("{:<25} {:<12} {:<12} {}", name, height, weight, bmi);
    }
    println!("\x1b[2m────────────────────────────────────────────────────────────────\x1b[0m\n");
}