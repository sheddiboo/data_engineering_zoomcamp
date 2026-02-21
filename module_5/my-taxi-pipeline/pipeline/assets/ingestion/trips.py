"""@bruin
name: ingestion.trips
type: python
image: python:3.11

# Updated to use your DuckDB connection
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd

def materialize():
    # Grab the dates provided by Bruin's pipeline context
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    
    # Safely load taxi_types (defaults to 'yellow' if not provided)
    vars_json = os.environ.get("BRUIN_VARS", "{}")
    taxi_types = json.loads(vars_json).get("taxi_types", ["yellow"])

    # Generate a list of the start of each month between the start and end dates
    months = pd.date_range(start=start_date, end=end_date, freq='MS')

    all_dataframes = []

    # Loop through each taxi type and each month to fetch the files
    for taxi_type in taxi_types:
        for dt in months:
            year = dt.strftime("%Y")
            month = dt.strftime("%m")
            
            # Construct the dynamic URL
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"
            print(f"Fetching data from: {url}")
            
            try:
                # Read the parquet file directly from the URL
                df = pd.read_parquet(url)
                all_dataframes.append(df)
                print(f"Successfully loaded {len(df)} rows.")
            except Exception as e:
                print(f"Warning: Could not fetch {url}. Error: {e}")

    # Combine all the individual monthly dataframes into one massive dataframe
    if all_dataframes:
        final_dataframe = pd.concat(all_dataframes, ignore_index=True)
    else:
        print("No data was fetched. Returning empty dataframe.")
        final_dataframe = pd.DataFrame()

    return final_dataframe