# Neythaleon

**Neythaleon** is a pre-ingestion audit and data pipeline toolkit for large scientific Parquet datasets. It profiles a dataset before any rows are loaded, produces a schema recommendation report, streams data into PostgreSQL in memory-bounded chunks, and generates a performance dashboard automatically on completion.

> _"The Eye Below Logs Everything."_

---

## Features

- **Pre-Ingestion Audit:** Classifies every column by density, infers PostgreSQL types, flags geometry and integer candidates, recommends DROP / EAV / index strategy — all before touching a database. Outputs a self-contained HTML report.
- **Go Schema Profiler:** Scans an entire Parquet dataset in seconds by reading only file footers — no row materialisation. Produces a JSON manifest that drives both the audit and the ingest pipeline.
- **Manifest-Driven Ingestion:** When a manifest is present, table creation requires zero sample I/O. `INT_COLUMNS` is auto-populated from integer-physical-type columns above a density threshold.
- **Parallel Transform:** Chunks are transformed concurrently via `ThreadPoolExecutor` (controlled by `PARALLELISM`), overlapping I/O-bound reads with CPU-bound cleaning.
- **High-Performance Loading:** Uses PostgreSQL's native `COPY` protocol for bulk insertion, with a `to_sql` fallback on failure.
- **Robust ETL:** Handles null bytes, special characters, WKB geometry, integer coercion, and nullable types in a fixed pipeline per chunk.
- **Adaptive Throttling:** Polls CPU and RAM after every chunk. Sleeps and optionally GCs when thresholds are exceeded.
- **Fault Isolation:** Failed chunks are written to `failed_chunks/` as CSV files. The run always continues.
- **Automatic Observability:** Every chunk appends a row to `media/ingestion_metrics.csv`. A four-panel PNG dashboard is generated immediately after ingestion completes.

---

## Architecture

```
goSchemeReader/main.go                      (run once, before anything else)
  WalkDir → jobs chan → N workers → aggregator goroutine
  Reads Parquet footers only — never materialises rows
  --json  →  schema_manifest.json
  (no flag) →  human-readable column density + storage estimate report

main.py
  ├── Phase 0: Audit            (always runs; no DB required)
  │     load_manifest()         ← schema_manifest.json (Go output or existing)
  │     advisor.analyse()
  │       classify columns by density (primary / moderate / sparse / dead)
  │       infer INT_COLUMNS, geometry, FK candidates, index suggestions
  │       generate CREATE TABLE DDL + EAV recommendation
  │     report.write_report()   →  media/audit_report.html
  │     --audit-only: stop here
  │
  ├── Phase 1: Ingest
  │     load_manifest()         ← reuses same manifest
  │     _create_table_if_missing()
  │       ├── manifest path: build_schema_df()    (no sample I/O)
  │       └── fallback path:  stream sample chunk, infer dtypes
  │     ThreadPoolExecutor(PARALLELISM workers)
  │       for batch in stream_parquet_chunks():
  │         futures = [submit(process_chunk) for chunk in batch]
  │         for future in futures:
  │           df = future.result()
  │           df.reindex(db_cols)
  │           coerce_df_to_schema(df, schema_map)
  │           copy_insert()  →  PostgreSQL (COPY) or to_sql fallback
  │           track_metrics() → persist_metrics() → media/*.csv
  │           throttle_if_needed()
  │           on error: save_failed_chunk() → failed_chunks/
  │
  └── Phase 2: Dashboard
        read_metrics_fuzzy(media/)
        create_single_png()    →  media/dashboard_yellow.png
```

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

## Quickstart

### Audit only (no database needed)

```bash
# Profile the dataset and open the HTML report
python -m audit ./parquet_files --html media/audit_report.html

# Save the manifest for reuse in subsequent runs
python -m audit ./parquet_files --html media/audit_report.html \
    --save-manifest schema_manifest.json

# Print recommended CREATE TABLE DDL only
python -m audit ./parquet_files --ddl

# Use an existing manifest (skip the Go scan)
python -m audit ./parquet_files --manifest schema_manifest.json \
    --html media/audit_report.html --table my_table
```

### Full pipeline (audit → ingest → dashboard)

```bash
# Step 1 — run the Go profiler once (reads metadata only)
go run goSchemeReader/main.go --json ./parquet_files > schema_manifest.json

# Step 2 — copy and edit config
cp .env.example .env

# Step 3 — run everything
python main.py

# Audit only, then stop before any DB writes
python main.py --audit-only

# Skip audit, go straight to ingest
python main.py --skip-audit
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
├── audit/                          # pre-ingestion audit package
│   ├── __init__.py
│   ├── __main__.py                 # enables: python -m audit <folder>
│   ├── advisor.py                  # column classification + DDL generation engine
│   ├── report.py                   # self-contained HTML report renderer
│   └── cli.py                      # CLI: --html, --ddl, --manifest, --table, --columns
│
├── goSchemeReader/
│   ├── main.go                     # Go CLI: schema profiler + storage estimator
│   ├── README.md                   # goSchemeReader-specific docs
│   └── outputLog.md                # sample output from a full dataset run
│
├── ingest/
│   ├── cli.py                      # argparse entry point (--debug flag)
│   ├── config.py                   # env loading; all typed config values
│   ├── logging_config.py           # dual-sink logging (stdout + file)
│   ├── ingest_runner.py            # orchestration loop (stream → transform → insert)
│   ├── manifest.py                 # loads Go manifest; drives schema and INT_COLUMNS
│   ├── db_utils.py                 # table inspect, COPY insert, failed-chunk save
│   ├── parquet_utils.py            # chunk streaming (PyArrow) + schema union
│   ├── transform.py                # process_chunk: null bytes, chars, ints, geometry
│   ├── scheme_utils.py             # coerce_df_to_schema: DB-type-aware coercion
│   └── metrics.py                  # track_metrics + persist_metrics (append-only CSV)
│
├── plot/
│   ├── cli.py                      # reads media/*.csv, calls dashboard
│   ├── dashboard.py                # create_single_png (2×2 layout)
│   ├── metrics_loader.py           # fuzzy CSV reader with numeric coercion
│   └── plotting_panels.py          # 4 panels: throughput, CPU scatter, memory, batch time
│
├── parquet_files/                  # place your .parquet files here (PARQUET_DIR default)
│   └── *.parquet
│
├── media/                          # all outputs land here
│   ├── audit_report.html           # pre-ingestion schema audit (open in browser)
│   ├── ingestion_metrics.csv       # append-only per-chunk metrics log
│   └── dashboard_yellow.png        # post-ingestion performance dashboard
│
└── failed_chunks/                  # CSVs written when a chunk fails; run continues
    └── failed_chunk_<n>.csv
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
