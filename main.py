import pandas as pd
import json
import os
import logging


# Setup logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

## Define file paths for source and destitnation files.

# Source files
RACES_FILE = "/Users/hamsinirekala/Documents/GitHub/engineering-recruitment-assignments/data-engineering/datapipeline/source-data/races.csv"
RESULTS_FILE = "/Users/hamsinirekala/Documents/GitHub/engineering-recruitment-assignments/data-engineering/datapipeline/source-data/results.csv"
# Destination files
OUTPUT_FOLDER = "/Users/hamsinirekala/Documents/GitHub/engineering-recruitment-assignments/data-engineering/datapipeline/results"


# Ensure output folder exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# loading the csv file into a dataframe
def load_csv(file_path: str) -> pd.DataFrame:
    """Load a CSV file into a DataFrame."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        return pd.DataFrame()

#pre processing race data by filling missing times and converting datetime
def preprocess_race_data(races_df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess race data: fill missing times, convert datetime."""
    required_cols = {"raceId", "year", "round", "name", "date", "time"}
    if not required_cols.issubset(races_df.columns):
        raise ValueError("Race CSV file is missing required columns.")

 # Fill missing race times with a default value of "00:00:00"
    races_df["time"] = races_df["time"].fillna("00:00:00")

    # Combine 'date' and 'time' into a single 'Race Datetime' column in ISO 8601 format
    races_df["Race Datetime"] = races_df["date"] + "T" + races_df["time"] + ".000"
    
    # Convert data types
    races_df["raceId"] = races_df["raceId"].astype(int)
    races_df["year"] = races_df["year"].astype(int)
    races_df["round"] = races_df["round"].astype(int)

    return races_df

#pre process results data by ensuring required columns exist and handling missing values
def preprocess_results_data(results_df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess results data: Ensure required columns exist and handle missing values."""
    required_cols = {"resultId", "raceId", "driverId", "position", "fastestLapTime"}
    if not required_cols.issubset(results_df.columns):
        raise ValueError("Results CSV file is missing required columns.")
    
    # Fill missing driver id values with -1
    results_df["driverId"] = results_df["driverId"].fillna(-1).astype(int)

    return results_df

# merge race data with results data to include winnners and fastest lap times
def merge_data(races_df: pd.DataFrame, results_df: pd.DataFrame) -> pd.DataFrame:
    """Merge races with winners and fastest lap times."""

    #  defining  winners_df by filtering the winner's details adn other required columns for that race
    winners_df = results_df[results_df["position"] == 1][["raceId", "driverId", "fastestLapTime"]]

    # Merge race data with the winners' data using 'raceId'
    merged_df = races_df.merge(winners_df, on="raceId", how="left")

    # Rename columns for clarity
    merged_df.rename(
        columns={
            "name": "Race Name",
            "round": "Race Round",
            "driverId": "Race Winning driverId",
            "fastestLapTime": "Race Fastest Lap",
        },
        inplace=True,
    )

    # Convert numeric columns to proper types
    merged_df["Race Winning driverId"] = merged_df["Race Winning driverId"].astype(pd.Int64Dtype())  # Allows NaN
    merged_df["Race Fastest Lap"] = merged_df["Race Fastest Lap"].astype(str)

    return merged_df

# save the processed data into json files , grouped by year.
def save_json(merged_df: pd.DataFrame, output_folder: str) -> None:
    """Save processed data to JSON files grouped by year."""

   # Ensure 'year' column exists for grouping 
    if "year" not in merged_df.columns:
        logging.error("Error: 'year' column not found.")
        return

# Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)

# Group the merged data by year and save each year's data to a separate JSON file
    for year, group in merged_df.groupby("year"):
        output_file = os.path.join(output_folder, f"stats_{year}.json")
        data = group[["Race Name", "Race Round", "Race Datetime", "Race Winning driverId", "Race Fastest Lap"]].to_dict(orient="records")

        with open(output_file, "w") as f:
            json.dump(data, f, indent=4)

    # Log that the file was saved successfully
        logging.info(f"Saved: {output_file}")


if __name__ == "__main__":
    races_df = load_csv(RACES_FILE)
    results_df = load_csv(RESULTS_FILE)
    # ✅ Check if both DataFrames are non-empty before proceeding
    if races_df.empty:
        logging.error("Races CSV file is empty or could not be loaded. Exiting.")
        exit(1)  # Stops the script with an error code

    if results_df.empty:
        logging.error("Results CSV file is empty or could not be loaded. Exiting.")
        exit(1)

    # Preprocess the data (cleaning and formatting)
        races_df = preprocess_race_data(races_df)
        results_df = preprocess_results_data(results_df)

   # Merge the race and results data
        merged_df = merge_data(races_df, results_df)

   # Save the merged data to JSON files     
        save_json(merged_df, OUTPUT_FOLDER)

   # Log successful completion of the process
        logging.info("JSON files successfully generated!")
