import pandas as pd
from sqlalchemy import create_engine

# Connect to database
engine = create_engine('postgresql://postgres:postgres@localhost:5433/ny_taxi')

print("Loading Green Taxi Data...")
df = pd.read_parquet('green_tripdata_2025-11.parquet')
df.to_sql(name='green_taxi_trips', con=engine, if_exists='replace', index=False)

print("Loading Zones...")
df_zones = pd.read_csv('taxi_zone_lookup.csv')
df_zones.to_sql(name='zones', con=engine, if_exists='replace', index=False)

print("Done!")