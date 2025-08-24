# Neythaleon

**Neythaleon** is an efficient, dependency-light data ingestion and observability toolkit for marine biodiversity datasets. Originally built for OBIS datasets, it ingests `.parquet` files, performs a robust cleaning pipeline, streams the data to PostgreSQL, and tracks detailed system metrics.

> _"The Eye Below Logs Everything."_

---

## Features

- **Efficient Ingestion:** Ingests large `.parquet` files in memory-safe chunks using DuckDB.
- **Robust ETL:** A multi-step cleaning and transformation pipeline handles data types, special characters, and complex geometry formats.
- **High-Performance Loading:** Uses PostgreSQL's native `COPY` command for fast, bulk data insertion.
- **Comprehensive Observability:** Logs detailed performance metrics, including CPU, RAM, throughput, and processing time per batch.
- **Schema Safe:** Automatically synchronizes the data's structure with the database table schema to prevent load failures.
- **Automated Setup:** Can automatically create the target database table if it doesn't exist.

---

## 🧠 Architecture

1.  **Extract:** Read `.parquet` files in memory-efficient chunks using DuckDB.
2.  **Transform:** A pipeline of cleaning functions is applied to each chunk:
    * Drop sparse columns and rows without valid coordinates.
    * Enforce correct integer data types.
    * Sanitize text fields by removing special characters.
    * Convert binary geometry (WKB) to text (WKT).
3.  **Align:** The schema of the cleaned data is dynamically matched to the target database table's schema.
4.  **Load:** The prepared data is bulk-loaded into PostgreSQL via an in-memory TSV stream.
5.  **Log:**
    * Time taken, CPU %, and RAM % are recorded for each chunk.
    * Rows ingested and processing speed (rows/sec) are calculated.
    * Metrics are exported to `metrics_log.csv` and human-readable logs.

---

## 📊 Sample Visualizations

> These dashboards are generated from the `metrics_log.csv` file using `plot.py`, providing a clear view of the pipeline's performance across different datasets and schemas.

### 1. OBIS Data (Baseline Performance)
![OBIS Performance Dashboard](media/metrics_dashboard_2.png)
*The baseline run on biodiversity data shows a highly stable throughput of over **11,000 rows/sec** with very low CPU usage, indicating an I/O-bound and efficient process.*

---
### 2. NYC For-Hire Vehicle (FHV) Data
![FHV Performance Dashboard](media/metrics_dashboard_1.png)
*This test on the FHV dataset demonstrates the pipeline's consistency, achieving a stable throughput of **~9,000 rows/sec** with remarkably uniform batch processing times.*

---
### 3. NYC Yellow Taxi Data
![NYC Taxi Performance Dashboard](media/metrics_dashboard_3.png)
*This run on the larger NYC Yellow Taxi dataset shows the pipeline's ramp-up behavior, peaking at **~2,700 rows/sec** and highlighting a positive correlation between CPU usage and throughput.*

---

## 📁 File Structure

```bash
.
Neythaleon/                           # repo root
├─ .env.examples                      # runtime config (DATABASE_URL, PARQUET_DIR, etc.)
├─ main.py                            # top-level runner (calls ingest CLI then plot CLI)
├─ requirements.txt                   # pinned/loose deps
├─ README.md                          # (optional) project overview & usage
│
├─ ingest/                            # ingestion package
│  ├─ __init__.py                     # exports for ingest package
│  ├─ cli.py                          # CLI entrypoint for ingestion
│  ├─ config.py                       # env loading and typed config values
│  ├─ logging_config.py               # centralized logging setup
│  ├─ ingest_runner.py                # main orchestration loop (stream -> transform -> insert)
│  ├─ db_utils.py                     # get_table_columns / copy_insert / failed chunk save
│  ├─ parquet_utils.py                # stream_parquet_chunks, get_full_schema_from_parquet
│  ├─ transform.py                    # clean_null_bytes, enforce_integer_types,          convert_geometry_to_wkt, process_chunk
│  ├─ scheme_utils.py                 # coerce_df_to_schema (DB-type based coercion)
│  └─ metrics.py                      # track_metrics + persist_metrics (CSV)
│
├─ plot/                              # plotting package
│  ├─ __init__.py                     # exports for plot package
│  ├─ cli.py                          # CLI entrypoint for plotting (reads media/metrics CSV)
│  ├─ dashboard.py                    # create_single_png (layout + save)
│  ├─ metrics_loader.py               # fuzzy CSV reading + numeric coercion
│  └─ plotting_panels.py              # plot_throughput, plot_cpu_vs_throughput, plot_memory, plot_batch_time
│
├─ parquet/                           # (input) place your .parquet files here (PARQUET_DIR)
│    └─ *.parquet
│
├─ media/                             # output directory for metrics & saved PNGs (METRICS_FILE lives here)
│  ├─ ingestion_metrics.csv
│  └─ metrics_dashboard.png           # Multiple PNGs for different runs are presented i this markdown
│
└─ failed_chunks/                     # saved CSVs when an insert/processing chunk fails
   └─ failed_chunk_<id>_<ts>.csv
```
---

## 🔧 Requirements
- Python 3.8+

- DuckDB

- Pandas

- SQLAlchemy

- Shapely

- Psycopg2 (or other DB driver)

- A PostgreSQL-compatible database

### Install via:
```bash
pip install -r requirements.txt
```
---

## How to Use and Configure

The pipeline is configured using a `.env` file. To get started, simply copy the provided template file to a new file named `.env` and then edit the values to match your dataset and database credentials.

You can do this in your terminal with the following command:

```bash
cp .env.example .env
```

Now, open the newly created `.env` file and edit the variables.

## Variable Explanations

- **DATABASE_URL**: Your full connection string for your PostgreSQL database.

- **DB_TABLE**: The name of the table where the data will be ingested. If the table doesn't exist, the script will create it based on the schema of the first data chunk.  
  Example:  
  ```env
  DB_TABLE="nyc_taxi_trips"
  ```

- **PARQUET_DIR**: The path to the folder containing your `.parquet` files.  
  Example:  
  ```env
  PARQUET_DIR="fhv_data"
  ```

- **LAT_COLUMN & LON_COLUMN**: The exact column names for latitude and longitude in your dataset. These are only used if `VALIDATE_COORDS` is enabled.  
  Example (for old taxi data):  
  ```env
  LAT_COLUMN="pickup_latitude"
  ```

- **VALIDATE_COORDS**: Set to `true` to clean data based on coordinate columns. Set to `false` if your dataset does not have latitude/longitude columns (like the recent NYC TLC data).  
  Example:  
  ```env
  VALIDATE_COORDS="false"
  ```

---
## Datasets

This project was originally designed for records from **OBIS** - the Ocean Biodiversity Information System. It is built to handle the raw complexity of this data, performing the necessary preprocessing to make it database-ready.

### Additional Test Datasets

The pipeline's flexibility has been validated against other public datasets, including:
- **NYC TLC Trip Records:** Data for Yellow Taxis and For-Hire Vehicles (FHV) tests the script's ability to handle different schemas, particularly those without the coordinate columns used for geospatial validation.

- **GBIF & USGS:** Other biodiversity and scientific datasets used to confirm the coordinate validation and data cleaning steps.

### **Citation**:

**New York City TLC Data**
> New York City Taxi and Limousine Commission. (2025). *Yellow Taxi Trip Records* and *For-Hire Vehicle Trip Records*. Retrieved August 18, 2025, from [nyc.gov](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)


**Ocean Biodiversity Information System Data**
> OBIS (2025). *Global distribution records from the OBIS database*. Ocean Biodiversity Information System. Intergovernmental Oceanographic Commission of UNESCO. Available at: [OBIS](https://obis.org/data/access/).


---

## Why This Matters

Building observable and reproducible data pipelines is fundamental to reliable data science. This project serves as a real-world example of analyzing and optimizing a Python ETL script, demonstrating how to identify and resolve complex performance bottlenecks through iterative testing and analysis.

---

## 🪪 License
- Code: MIT
- Data: CC0 1.0 

--- 
 
## ⚓ Quote That Hit Different
> "The sea, once it casts its spell, holds one in its net of wonder forever."
   — Jacques Cousteau
---