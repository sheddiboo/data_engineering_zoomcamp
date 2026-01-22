import sys
import pandas as pd

# Print all arguments passed to the script
print("arguments", sys.argv)

# Parse the first argument as an integer
day = int(sys.argv[1])
print(f"Running pipeline for day {day}")

# Create a sample DataFrame
df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

# Assign the 'day' variable to a new column
df['day'] = day
print(df.head())

# Save the DataFrame to a parquet file using the day in the name
df.to_parquet(f"output_day_{day}.parquet")