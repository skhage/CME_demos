# Databricks notebook source
# MAGIC %pip install faker pyyaml lxml
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import yaml

# Load the configuration settings
with open("params.yml", "r") as file:
    params = yaml.safe_load(file)
params.get('data_locations').get('raw_data_location')

# COMMAND ----------

import random
import json
from faker import Faker
import time
import pandas as pd 
import yaml

# Load the configuration settings
with open("params.yml", "r") as file:
    params = yaml.safe_load(file)

# Initialize Faker to the US locale
fake = Faker('en_US')

# Define the number of records
num_records = random.randint(40, 4000)

# Define a list to hold the dataset
data = []

# Generate the dataset
for _ in range(num_records):
    # Create a dictionary for each record
    record = {
        'ip_address': fake.ipv4_public(),  # Generate a private US-based IP address
        'timestamp': fake.date_time_this_month().isoformat(),  # Convert datetime to ISO format string
        'url': fake.url(),  # Generate a random URL
        'port': fake.port_number(), # Generate a port number, this field will be used to quarantine records
        'user_agent': fake.user_agent(),  # Generate a random user agent,
        'event_id': fake.uuid4(),
        # 'flagged': 'Yes' if random.random() < flag_fraction else 'No'  # Flag a fraction of the records
    }
    # Add the record to the dataset
    data.append(record)

# Convert the dataset to a JSON string
# json_dat = json.dump(data, indent=4)
currenttime = time.time()
datadir = params.get('data_locations').get('raw_data_location')

pd.DataFrame(data).to_xml(f"{datadir}/web_traffic_data_g{int(currenttime)}.xml")

# COMMAND ----------

geo_dir = params.get('data_locations').get('geo_data_location')

geo = spark.read.format('csv').option('header', 'true').load(geo_dir)
display(geo)

# COMMAND ----------


