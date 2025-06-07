import os
import time

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Path to the occurrence.txt file
FILE_PATH = "./occurrence.txt"

# Define chunk size
CHUNK_SIZE = 1000  # Adjust based on your system's capacity

# Read and process the file in chunks
for chunk in pd.read_csv(FILE_PATH, sep='\t', chunksize=CHUNK_SIZE):
    # Data cleaning and preprocessing steps go here

    # Insert into the database
    chunk.to_sql('obis_data', con=engine, if_exists='append', index=False)

    # Delay to manage resource usage
    time.sleep(5)
