# Neythaleon

**Neythaleon** is an energy-aware marine observability toolkit focused on ingesting, monitoring, and visualizing ocean biodiversity datasets. Originally developed using IndOBIS Kochi region data later to OBIS, it emphasizes sustainable data engineering using DuckDB, Pandas, GeoPandas, and PostgreSQL (Neon).

> *“The Eye Below Logs Everything.”*

---

## Features

- 🔹 Ingest `.parquet` datasets with fallback support
- 🔹 Track latency, nulls, and data volume in real time
- 🔹 Plot marine biodiversity using Folium + Matplotlib
- 🔹 Run on limited hardware — optimized for edge cases

---

## 🧠 Architecture Summary

The pipeline reads OBIS data in chunks using DuckDB, processes it with null/coordinate filters, and attempts to insert each chunk into a PostgreSQL table. If batch insertion fails, it retries each row individually. Performance metrics are tracked and logged for each chunk, with results saved to `metrics_log.csv`.

---

## 📁 File Structure(Incomplete)
    
    ├── occurrence.txt 
    ├── ingestion.py 
    ├── .env 
    ├── ingestion.log # Log file (auto-generated)
    └── metrics_log.csv # Performance metrics output (auto-generated)

---

## 🔧 Requirements

- Python 3.8+
- PostgreSQL or compatible DB

---

### Install requirements:
    
    pip install -r requirements.txt

Add --debug for verbose logging.

---

## 🐙 Dataset Information

This project uses a bulk data dump from the Ocean Biodiversity Information System (OBIS). The ZIP archive, downloaded in July 2025, contains .parquet files representing global marine biodiversity records. No geographic, taxonomic, or temporal filters were applied during download. The ingestion pipeline performs basic cleaning (column null filtering, coordinate validation) but does not modify the data beyond ingestion prep.

---

**Dataset Source**

The data used in this project was downloaded from the Ocean Biodiversity Information System (OBIS) in July 2025 as a bulk ZIP archive containing `.parquet` files. No geographic, taxonomic, or temporal filters were applied during download. The ingestion pipeline performs basic cleaning, including null filtering and coordinate validation, but the dataset represents a raw, unfiltered sample of the OBIS global database.

---

**Citation**

OBIS (2025). Global distribution records from the OBIS database.  
Ocean Biodiversity Information System. Intergovernmental Oceanographic Commission of UNESCO.  
Available at: [https://obis.org/data/access/](https://obis.org/data/access/).

---

### 🪪 License
This project is released under the MIT License.
OBIS data is provided under the [CC0 Public Domain Dedication](https://creativecommons.org/publicdomain/zero/1.0/).

---