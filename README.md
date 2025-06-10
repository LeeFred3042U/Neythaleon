# OceanScope
---
# 🐙 OBIS Data Ingestion Script

This Python script ingests a large TSV file (`occurrence.txt`) into a PostgreSQL-compatible database, logging key metrics along the way. It supports chunked ingestion, row-by-row fallbacks, performance tracking, and environmental configuration.

---

## 🚀 Features

- Chunked reading and insertion for large files
- Row-level fallback on chunk insert failure
- Environment-based configuration with `.env`
- CPU and ingestion speed metrics logged
- Automatic retry/resume-safe architecture
- PEP-8 compliant and production-ready logging

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
- Dependencies:
  - pandas
  - sqlalchemy
  - python-dotenv
  - psutil

Install requirements:

    
    pip install -r requirements.txt




---
## License

This work is licensed under a [Creative Commons Attribution (CC-BY 4.0) License](https://creativecommons.org/licenses/by/4.0/).

Researchers using OceanScope should respect this rights statement and provide appropriate attribution when using any associated materials.

---
### Dataset Attribution

**Indian Ocean Marine Fauna Voucher Specimens Collections (CMLRE), Kochi, India.**  
Version 1.0. Centre for Marine Living Resources & Ecology. Occurrence dataset (IndOBIS).  

