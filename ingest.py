import pandas as pd
from sqlalchemy import create_engine, text 
from dotenv import load_dotenv 
import os
from datetime import datetime, timezone


# Load environment variables
load_dotenv()
db_url = os.getenv('DATABASE_URL')


if not db_url:
    raise Exception("You forgot to set the DATABASE_URL in your .env.")



# Create SQLAlchemy engine
engine = create_engine(db_url)



# File path
file_path = 'occurance.txt'



# Read tab-delimited file
df = pd.read_csv(file_path, sep='\t', dtype=str)  # Force all columns to str to avoid pandas type-guessing mess



# Add metadata columns
df['ingested_at'] = datetime.now(timezone.utc)
df['source_file'] = os.path.basename(file_path)


# Normalize missing fields (replace empty strings or NaNs with NULLs)
df = df.where(pd.notnull(df), None)



# Define table name
table_name = 'occurrence_data'



# Ingest into DB
with engine.connect() as conn:
    # Create table if not exists
    columns = ',\n  '.join([f'"{col}" TEXT' for col in df.columns])
    create_stmt = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
      {columns}
    );
    '''


    conn.execute(text(create_stmt))


    # Ingest data
    df.to_sql(table_name, conn, if_exists='append', index=False)



    # Log ingestion stats
    print(f"✅ Ingestion complete:")
    print(f"✅ Rows ingested: {len(df)}")
    print(f"✅ Missing fields: {df.isnull().sum().sum()}")
    print(f"✅ Ingested at: {df['ingested_at'].iloc[0]}")

    print(f"Data inserted into table: {table_name}")
    print(f"Target DB: {db_url}")
