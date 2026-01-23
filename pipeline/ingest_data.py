#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz', nrows=100)


# In[3]:


df.head()


# In[4]:


# Check data types
df.dtypes


# In[5]:


# Check data shape
df.shape


# In[6]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates
)


# In[9]:


df


# In[8]:


# Check data shape
df.shape


# In[10]:


#Create Database Connection
from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[ ]:





# In[11]:


# Get DDL Schema
print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[12]:


# Create the Table
df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# **Ingesting Data in Chunks**

# In[20]:


# Create an iterator to read the CSV in chunks of 100,000 rows to save memory.
df_iter = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)


# In[21]:


df_iter


# In[22]:


from tqdm.auto import tqdm


# In[23]:


# Loop through chunks with a progress bar, appending each batch to the database.
for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
    print(len(df_chunk))


# In[ ]:




