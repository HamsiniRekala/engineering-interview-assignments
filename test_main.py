import pytest
import pandas as pd
import json
import os
from unittest.mock import patch, mock_open
from main import load_csv, preprocess_race_data, preprocess_results_data, merge_data, save_json

# Sample race data
races_sample = pd.DataFrame({
    "raceId": [1, 2],
    "year": [2024, 2024],
    "round": [1, 2],
    "name": ["Bahrain GP", "Saudi GP"],
    "date": ["2024-03-05", "2024-03-12"],
    "time": ["15:00:00", None]  # One missing time value
})

# Sample results data
results_sample = pd.DataFrame({
    "resultId": [101, 102],
    "raceId": [1, 2],
    "driverId": [201, 202],
    "position": [1, 2],
    "fastestLapTime": ["1:32.5", "1:31.8"]
})

#Unit tests for validating the functions in main.py
# ---------- 1. Mocking Test: Loading CSV File ----------
@patch("pandas.read_csv")
def test_load_csv(mock_read_csv):
    mock_read_csv.return_value = races_sample
    df = load_csv("dummy.csv")
    assert df.equals(races_sample)


# ---------- 2. Edge Case Tests: Missing and Invalid Data in Preprocessing ---------
def test_missing_race_data():
    """Test handling of missing values in race data."""
    invalid_race_data = pd.DataFrame({
        "raceId": [1, 2],
        "year": [2024, 2024],
        "round": [1, 2],
        "name": ["Bahrain GP", "Saudi GP"],
        "date": ["2024-03-05", "2024-03-12"],
        "time": [None, "15:00:00"],  # Missing time for first race
    })

    processed_df = preprocess_race_data(invalid_race_data)

    # Check that missing 'time' is replaced with "00:00:00"
    assert processed_df["time"].iloc[0] == "00:00:00", "Missing time was not replaced correctly"
    assert processed_df["time"].iloc[1] == "15:00:00", "Existing time was modified unexpectedly"

def test_missing_results_data():
    """Test handling of missing values in results data."""
    invalid_results_data = pd.DataFrame({
        "resultId": [101, 102],
        "raceId": [1, 2],
        "driverId": [None, 202],  # Missing driverId for first result
        "position": [1, 2],
        "fastestLapTime": ["1:32.5", "1:31.8"]
    })

    processed_df = preprocess_results_data(invalid_results_data)

    # Check that missing 'driverId' is replaced with -1
    assert processed_df["driverId"].iloc[0] == -1, "Missing driverId was not filled with -1"
    assert processed_df["driverId"].iloc[1] == 202, "Existing driverId was modified unexpectedly"

    

# ---------- 3. Type and Data Handling Tests in Preprocessing ----------
def test_data_types():
    processed_df = preprocess_race_data(races_sample.copy())

    # Check if "Race Round" is an integer
    assert pd.api.types.is_integer_dtype(processed_df["round"])

    # Check if "Race Datetime" is a string
    assert pd.api.types.is_string_dtype(processed_df["date"])  # Assuming 'date' column is the datetime column


# ---------- 4. Data Merging Tests ----------
def test_merge_data():
    merged_df = merge_data(races_sample.copy(), results_sample.copy())

    # Ensure merged columns exist
    assert "Race Name" in merged_df.columns  # Merged race name column
    assert "Race Winning driverId" in merged_df.columns  # Merged driverId column

    # Check if merged DataFrame contains the expected number of rows
    assert len(merged_df) == len(races_sample)


# ---------- 5. File Handling Tests: Saving JSON ----------
from unittest.mock import patch, mock_open
import pandas as pd

def test_save_json_output_folder_not_found():
    mock_data = [{
        "Race Name": "Bahrain GP",
        "Race Round": 1,
        "Race Datetime": "2024-03-05T15:00:00.000",
        "year": 2024,
        "Race Winning driverId": 101,
        "Race Fastest Lap": "1:32.5"
    }]
    
    df = pd.DataFrame(mock_data)

    # Mocking os.path.exists and os.makedirs to simulate the folder not existing
    with patch("os.path.exists", return_value=False), \
         patch("os.makedirs") as mock_makedirs, \
         patch("builtins.open", mock_open()) as mocked_file:

        # Calling save_json with the folder path
        save_json(df, "results")

        # Ensure os.makedirs was called to create the directory
        mock_makedirs.assert_called_once_with("results")
        
        # Ensure the file open was called to save the json file at the expected path
        mocked_file.assert_called_once_with("results/stats_2024.json", "w")

# ---------- 6. Handling Missing Columns in Results Data ----------
def test_preprocess_results_data_missing_columns():
    # Sample results dataframe with missing 'fastestLapTime' column
    invalid_results = pd.DataFrame({
        "resultId": [101, 102],
        "raceId": [1, 2],
        "driverId": [201, 202],
        "position": [1, 2],
    })

    # Check that ValueError is raised due to missing 'fastestLapTime'
    with pytest.raises(ValueError, match="Results CSV file is missing required columns."):
        preprocess_results_data(invalid_results)


