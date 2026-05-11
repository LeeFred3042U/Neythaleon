# Neythaleon

**Neythaleon** is a pre-ingestion audit and data pipeline toolkit for large scientific Parquet datasets. It profiles a dataset before any rows are loaded, produces a schema recommendation report, streams data into PostgreSQL in memory-bounded chunks, and generates a performance dashboard automatically on completion.

> _"The Eye Below Logs Everything."_

---

## Features

- **Pre-Ingestion Audit:** Built into the `main.py` wizard. Classifies columns, flags candidates for indexing or exclusion, and provides cost estimates before any data is loaded.
- **Go Schema Profiler:** Scans an entire Parquet dataset in seconds by reading only file footers. Produces the `schema_manifest.json` that drives the entire pipeline.
- **Manifest-Driven Ingestion:** When a manifest is present, table creation requires zero sample I/O. `INT_COLUMNS` is auto-populated from integer-physical-type columns above a density threshold.
- **Parallel Transform:** Chunks are transformed concurrently via `ThreadPoolExecutor` (controlled by `PARALLELISM`), overlapping I/O-bound reads with CPU-bound cleaning.
- **High-Performance Loading:** Uses PostgreSQL's native `COPY` protocol for bulk insertion, with a `to_sql` fallback on failure.
- **Robust ETL:** Handles null bytes, special characters, WKB geometry, integer coercion, and nullable types in a fixed pipeline per chunk.
- **Adaptive Throttling:** Polls CPU and RAM after every chunk. Sleeps and optionally GCs when thresholds are exceeded.
- **Fault Isolation:** Failed chunks are written to `failed_chunks/` as CSV files. The run always continues.
- **Automatic Observability:** Every chunk appends a row to `media/ingestion_metrics.csv`. A four-panel PNG dashboard is generated immediately after ingestion completes.

---

## Architecture

The pipeline is orchestrated as a sequential CLI wizard in `main.py`. It uses a checkpoint system to persist state, allowing you to resume interrupted runs.

- **Phase 0:** Manifest check (requires `schema_manifest.json`)
- **Phase 1:** Interactive column selection
- **Phase 2:** Bit-array metadata intake
- **Phase 3:** Cost estimation
- **Phase 4:** User confirmation
- **Phase 5:** Data ingestion (via PostgreSQL COPY)
- **Phase 6:** Dashboard & Graph generation

### Transform pipeline (per chunk, in fixed order)

1. Strip null bytes (`\x00`) from string columns — vectorised
2. Sanitise special characters (`\n`, `\r`, `\t` → space) — regex
3. Coerce integer columns to nullable `Int64` — from `INT_COLUMNS` or manifest inference
4. Decode WKB binary geometry → WKT text via Shapely

### Audit column categories

| Category | Density | Default action                     |
| -------- | ------- | ---------------------------------- |
| Primary  | ≥ 80%   | Keep; index if integer or boolean  |
| Moderate | 20–79%  | Keep as nullable                   |
| Sparse   | 1–19%   | Warn; candidate for EAV side table |
| Dead     | 0%      | Recommend DROP                     |

---

## Developer Setup & Running

Follow these steps to set up and run the Neythaleon project locally.

### 1. Environment Setup

Clone the repository and set up your Python virtual environment:

```bash
git clone <repository-url>
cd Neythaleon
python -m venv venv

# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

Install the required Python dependencies:

```bash
pip install -r requirements.txt
```

### 2. Configuration & Data Preparation

Copy the environment template and configure it with your local settings (e.g., your PostgreSQL `DATABASE_URL`):

```bash
cp .env.example .env
```

Next, place the `.parquet` files you want to ingest into the `parquet_files/` directory (or update the `PARQUET_DIR` variable in `.env`).

### 3. Running the Project

Neythaleon provides an interactive, resumable 6-phase CLI wizard for the full ingestion pipeline, as well as a standalone module for pre-ingestion auditing.

#### Full Interactive Pipeline (Audit → Ingest → Dashboard)

The main pipeline uses checkpoints. If interrupted, re-running the script will prompt you to resume from where you left off.

```bash
# Step 1 — Run the Go profiler once to generate the schema manifest
go run goSchemeReader/main.go --json ./parquet_files > schema_manifest.json

# Step 2 — Start the interactive orchestrator wizard
python main.py
```

#### Integrated Pipeline (Audit → Ingest → Dashboard)

The main pipeline is interactive. Simply follow the prompts to select columns and review cost estimates before confirming the ingestion.

```bash
# Step 1 — Run the Go profiler once to generate the schema manifest
go run goSchemeReader/main.go --json ./parquet_files > schema_manifest.json

# Step 2 — Start the interactive orchestrator wizard
python main.py
```

#### Quick Inspection Tool (No Database)

If you just want a quick JSON summary of column densities and storage estimates without running the full wizard:

```bash
python inspect_parquet.py ./parquet_files --json
```

Leave `INT_COLUMNS` empty in `.env` to let the manifest auto-populate it. Set explicitly to override.

---

## Go + Python Integration

The Go tool and Python pipeline share the same manifest format. Run the Go tool once to generate it; all subsequent `python main.py` and `python -m audit` runs load it from disk without rescanning.

```bash
# Human-readable report (stdout only)
go run goSchemeReader/main.go ./parquet_files

# JSON manifest (redirect to file)
go run goSchemeReader/main.go --json ./parquet_files > schema_manifest.json
```

The manifest drives three things on the Python side:

- **Audit advisor** — column classification, DDL generation, EAV recommendation
- **Table bootstrap** — `build_schema_df()` maps Go physical types to pandas dtypes, skipping sample-chunk I/O
- **INT_COLUMNS inference** — `infer_int_columns()` returns `INT32`/`INT64` columns with ≥50% density when `INT_COLUMNS` is not set in `.env`

---

## File Structure

```
Neythaleon/
├── .env.example                    # copy to .env and edit
├── main.py                         # orchestrator: audit → ingest → dashboard
├── requirements.txt
├── schema_manifest.json            # produced by: go run goSchemeReader/main.go --json <dir>
│
├── checkpoint/                    # checkpoint system
│   └── checkpoint.py              # persists phase state to checkpoint.json
│
├── bitarray/                      # bit-array handling
│   ├── detector.py                # identifies bit-encoded columns
│   ├── decoder.py                 # unpacks bytes to columns
│   └── metadata_loader.py         # validates encoding JSON
│
├── cost/                          # cost estimation logic
│   └── calculator.py
│
├── selector/                      # column selection system
│   ├── column_selector.py         # Rich TUI selector
│   └── recommender.py             # heuristic recommendations
│
├── plot/                          # graphing & dashboards
│   ├── dashboard.py               # performance metrics dashboard
│   └── user_graphs.py             # data-driven graph wizard
│
├── goSchemeReader/                # Go-based fast schema profiler
│   └── main.go
```

---

## Requirements

- **Python** 3.10+
- **Go** 1.21+ (for `goSchemeReader`; optional — audit falls back to PyArrow schema scan if Go is unavailable)
- **PostgreSQL** (not required for audit-only mode)

### Python dependencies

```bash
pip install -r requirements.txt
```

Key packages: `pandas`, `pyarrow`, `duckdb`, `sqlalchemy`, `psycopg2-binary`, `shapely`, `psutil`, `matplotlib`, `python-dotenv`

---

## Configuration

```bash
cp .env.example .env
```

| Variable                  | Default                       | Description                                                                           |
| ------------------------- | ----------------------------- | ------------------------------------------------------------------------------------- |
| `DATABASE_URL`            | _(required for ingest)_       | PostgreSQL connection string                                                          |
| `DB_TABLE`                | `ingested_data`               | Target table name; created automatically if absent                                    |
| `PARQUET_DIR`             | `parquet_files`               | Folder containing `.parquet` input files                                              |
| `FAILED_DIR`              | `failed_chunks`               | Destination for chunk-failure CSVs                                                    |
| `MEDIA_DIR`               | `media`                       | Output folder for all generated files                                                 |
| `CHUNK_SIZE`              | `10000`                       | Rows per batch; keep ≤20,000 to avoid OOM warnings                                    |
| `PARALLELISM`             | `4`                           | `ThreadPoolExecutor` workers for concurrent transforms                                |
| `VALIDATE_COORDS`         | `false`                       | Coordinate validation hook (reserved; not yet implemented)                            |
| `INT_COLUMNS`             | _(empty)_                     | Comma-separated columns to coerce to `Int64`; auto-populated from manifest when empty |
| `MANIFEST_FILE`           | `schema_manifest.json`        | Path to Go manifest; skips sample-chunk inference when present                        |
| `METRICS_FILE`            | `media/ingestion_metrics.csv` | Append-only metrics log                                                               |
| `LOG_FILE`                | `ingestion.log`               | File log destination                                                                  |
| `CPU_THRESHOLD`           | `85`                          | CPU % above which the loop sleeps                                                     |
| `THROTTLE_DELAY_HIGH_CPU` | `2`                           | Seconds to sleep under high CPU or RAM                                                |
| `THROTTLE_DELAY`          | `0`                           | Baseline delay between chunks (seconds)                                               |

---

## License

- Code: MIT
- Data: CC0 1.0

---

> "The sea, once it casts its spell, holds one in its net of wonder forever."
> — Jacques Cousteau
