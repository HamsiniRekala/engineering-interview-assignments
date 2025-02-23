# Solution README
# Solution Documentation

# CORE REQUIREMENTS
# Task
The task was about building automated data pipelines for a motorsport analytics Youtube channel to do rapid analysis of Formula 1 for their viewers.The pipelines would perform automated transformations of incoming datasets immediately following races.

# DESCRIPTION OF THE SOLUTION
# Overview
This repository contains a data pipeline that processes Formula 1 race data from races.csv and results.csv, merges them , generates JSON files for EACH YEAR with race statistics. These files contains details about each race , like race name, round , datetime, winning driver id  and fastest lap.

# Setup
-First the Repo has been setup , steps followed for set up are :
   1.Cloned the assignment repository given to my local machine.
    git clone https://github.com/optima-partners/engineering-recruitment-assignments.git

   2.Created a repository in my personal VCS account i.e GitHub .
    git clone https://github.com/optima-partners/engineering-interview-assignments.git
    https://github.com/HamsiniRekala/engineering-interview-assignments.git

   3.Added the repo i created to the cloned repository as a remote
   git remote add submission https://github.com/HamsiniRekala/engineering-interview-assignments.git

   - started to build the code in VSCODE as main.py 
# pre requisites
 -The pipeline code has been built using python .
 - Python 3.6+ installed on my system.
 -Used Python packages :
    -pandas
    -json
    -unittest (for running tests)

-Installed them  using "pip install -r requirements.txt"

# Runnning the pipeline
STEP BY STEP APPROACH OF BUILDING THE CODE 

1. Imported the required libraries .
   -pandas for handling and manipulating the data.
   -json for saving the final results in JSON format.
   -os for handling file system operations like creating directories and file paths.
   -Defined seperate functions for each task to promote modularity and easier maintainence, isolate testing, clear intent , error handling management and can scale easily if required.

2.Defining the file paths for races.csv , results.csv and output_folder and loading them.
 - Here the races.csv and results.csv are the source data files.
 -Output folder is where the output json files will be saved.
 -Loading the CSV Data using read.csv function.

 3.Pre-processing the Data.( Data Cleansing and Transformations)
  # The pre-processing of data has been done seperately for both races and results to handle missing values, date-time transformations and dealing with inconsistent formats.

  -The preprocess_race_data function ensures that the race data has the required columns (raceId, year, round, etc.).
  -It handles missing race times by filling them with "00:00:00".
  -It combines the date and time columns into a new column, Race Datetime, in ISO 8601 format (e.g., "2024-03-05T15:00:00.000").
  -The preprocess_results_data function checks if the required columns (resultId, raceId, driverId, position, fastestLapTime) are present.
  -If any driverId values are missing, they are replaced with -1 (as specified by the requirements).

4.Merged the processed race data with the processed results data using the common key "race ID".
-According to the requirements,
     -It identifies the race winner (position 1) and extracts the winning driver's ID and the fastest lap time.
    -The columns are renamed for clarity, and data types are adjusted for consistency (e.g., Race Round as an integer).

5. Saved the merged files as JSON ,the save_json function defined groups the merged data by year and writes each year's data to a seperate JSON File
-The to_dict(orient="records") converts each row into a dictionary, ready to be saved as JSON.
- Each JSON file is named stats_{year}.json and is saved in the OUTPUT_FOLDER.

6.The main script provided at the end calls the functions for loading the files, preprocessing the data, merging and saving the final merged files.

# Running the pipeline
 1. Prepare your input data .(races.csv and results.csv)
    -Ensure the following CSV files are available in the direcotory with specified columns.
    -The pipeline expects these files to be in the correct paths specified in the code.
 2.Executing the pipeline.
    - Run the script to load, process, merge and save the data.
      "python main.py"
    -After running this script, the pipeline will generate JSON files in the results/ folder, grouped by race year, with each file named stats_{year}.json, promoting automated transformations for incoming datasets.

# Output
 The output files are stored in the results/ directory, each named stats_{year}.json. The JSON format for each race in the file will look as follows:

[
    {
        "Race Name": "British Grand Prix",
        "Race Round": 12,
        "Race Datetime": "2024-07-07T14:00:00.000",
        "Race Winning driverId": 1,
        "Race Fastest Lap": "01:29.4"
    },
    ...
]
-This pipeline automates the process of transforming and merging the raw race and results data into a structured JSON format. The resulting JSON files are ready for analysis or further processing. Each step of the pipeline is modular and can be adjusted or extended for future requirements.

# STRECH REQUIREMENTS
 -Have implemented unit tests for the fucntions to improve code quality, ensure that pipelines behaves correctly acroass a variety of edge cases , making the system more robust and reliable.

  1.Mocking and Data loading :test_load_csv
  -Purpose:This test ensures that the load_csv function correctly loads a CSV file into a DataFrame.
  - The patch decorator is used to mock the pandas.read_csv method. This allows you to simulate the behavior of reading a CSV file without actually accessing any files.
  -It verifies that the load_csv function returns the expected DataFrame (races_sample).
  
  2.Edge Case Test :test_missing_race_data
  -Purpose: This test validates that missing race times are correctly handled by the    preprocess_race_data function.
  -How it works: A DataFrame (invalid_race_data) with missing times (represented as None) is passed to the preprocess_race_data function.
  -What it checks:
    -It asserts that missing values in the time column are replaced with "00:00:00".
    -It also verifies that existing times are not modified, ensuring that the function only fills missing values.

3. Edge Case Test: test_missing_results_data
   -Purpose: This test ensures that missing driverId values in the results data are handled properly by the preprocess_results_data function.
   -How it works: A DataFrame (invalid_results_data) with a missing driverId value is passed to preprocess_results_data.
   -What it checks:
      -It asserts that missing driverId values are replaced with -1.
      -It ensures that the existing driverId value (202) is not modified.

4. Type and Data Handling Test: test_data_types
    -Purpose: This test checks if the data types of certain columns are handled correctly after preprocessing.
    -How it works: The preprocess_race_data function is called on the sample races_sample DataFrame, and the data types of specific columns are validated.
    -What it checks:
      -It asserts that the round column has an integer data type.
      -It verifies that the date column (which is now the Race Datetime) is treated as a string type.

5. Data Merging Test: test_merge_data
    -Purpose: This test validates that the merge_data function works as expected, merging the race data with the result data.
    -How it works: The merge_data function is called with the races_sample and results_sample DataFrames.
    -What it checks:
       -It ensures that the merged DataFrame contains the columns "Race Name" and "Race Winning driverId".
       -It checks that the number of rows in the merged DataFrame matches the number of rows in the races_sample DataFrame, confirming the correct merging process.

6. File Handling Test: test_save_json_output_folder_not_found
    -Purpose: This test ensures that the save_json function handles situations where the output folder does not exist, by creating the folder and saving the file correctly.
    -How it works: The os.path.exists method is mocked to simulate the non-existence of the output folder. The os.makedirs method is also mocked to verify that the folder is created when necessary.
    -What it checks:
       -It verifies that os.makedirs is called to create the folder if it doesn’t exist.
       -It confirms that the JSON file is saved correctly at the expected path using the correct filename (stats_2024.json).

7. Handling Missing Columns in Results Data:test_preprocess_results_data_missing_columns
    -Purpose: This test checks that the preprocess_results_data function raises an error when required columns are missing from the results data.
    -How it works: A sample results DataFrame is created with the required column (fastestLapTime) missing. The preprocess_results_data function is called with this incomplete DataFrame.
    -What it checks:
       -It verifies that a ValueError is raised with the expected error message: "Results CSV file is missing required columns." .

# Running the tests
-Used the following command to run the test
  -pytest test_main.py 
  -This will validate data processing , merging and JSON output.

# Submission

# Deploying the solution to cloud provider
 - have chose Azure for its robust, scalable, and cost-effective cloud solutions, which are ideal for handling the data pipeline, providing seamless integration with storage, serverless functions, and scalable compute resources.

# Cloud Deployment Notes (Azure)
## Tools to Use:
-AZURE STACK
 1.Azure Blob Storage: Store CSV files (races.csv and results.csv) in Blob Storage for easy access and scalability.
 2.Azure Functions: Use Azure Functions to run the pipeline as a serverless function triggered by file uploads to Blob Storage.
 3.Azure Databricks: For larger datasets or complex processing, Azure Databricks provides a scalable solution.
 4.Azure Logic Apps / Azure Data Factory: Automate the process, including triggering the pipeline after data is uploaded to Blob Storage.
 5.Azure Kubernetes Service (AKS): If containerizing the pipeline, use Azure Kubernetes Service for managing the containerized applications.

 ## Steps for Deployment:
 -Create an Azure Storage Account and upload the CSV files to Blob Storage.
 -Develop an Azure Function that runs the data pipeline:
 -Load CSV files from Blob Storage.
 -Process the data (merge, clean, format).
 -Save the output as JSON to a designated Blob Storage container.
 -Use Azure Logic Apps to automate the process, such as triggering the function when new CSV files are uploaded.
 -Use Azure Monitor to track the performance and status of the pipeline.

 ## Security Considerations:
 -Managed Identity: Use Azure Managed Identity for authentication to Blob Storage and other Azure services.
 -Azure Key Vault: Store any sensitive credentials (such as access keys) in Azure Key Vault.

## Scaling Considerations:
Serverless Options (Azure Functions): Automatically scale the pipeline based on load.
Azure Databricks: For larger volumes of data or complex transformations, Azure Databricks can distribute the processing workload across multiple nodes to ensure scalability and high performance.

### This solution provides an efficient and automated approach to transforming Formula 1 race data into a structured JSON format. It handles the necessary preprocessing, merging, and output generation in a scalable manner. Additionally, the solution can be extended for cloud deployment on Azure, allowing the pipeline to scale and be automated for production use. 


# REQUIREMENTS MET
The solution fulfills the following core requirements:
1.CSV Input Handling: It loads two CSV files, races.csv and results.csv.
2.Data Validation: Ensures the presence of necessary columns in both input files.
3.Data Preprocessing: Handles missing values and generates new columns for proper datetime formatting.
4.Data Merging: Merges the races and results data based on raceId and identifies the race winner and fastest lap.
5.JSON Output: Generates JSON files per year containing:
"Race Name"
"Race Round"
"Race Datetime"
"Race Winning driverId"
"Race Fastest Lap"
7.File Naming: The output JSON files are named stats_{year}.json for each year of races.
8.Error Handling: Includes checks for required columns and provides error messages if columns are missing.
9.Strech requirements- Inluded unit tests for all functions , the pipeline has successfully passed all unit tests, demonstrating its robustness and reliability in handling various edge cases and ensuring correct functionality.
10.Added some notes about deploying this pipeline to a cloud provider(Azure) and list of tool stack to implement the pipeline.





