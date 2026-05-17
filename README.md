# Neythaleon

Parquet-to-PostgreSQL ingestion pipeline for large-scale species occurrence datasets.

---

## What it does

Neythaleon reads partitioned Parquet files (OBIS H3-gridded species grids), lets you select which columns to ingest, resolves bit-array encoded columns, estimates storage cost before committing, then bulk-loads the data into a PostgreSQL table using `COPY`. Each pipeline phase is checkpointed so interrupted runs can be resumed from where they left off. After ingestion, a performance dashboard is generated from collected metrics.

**Typical use case:** ingesting the OBIS `speciesgrids/h3_7` dataset (~2M+ rows across dozens of Parquet files) into a self-hosted or Neon-hosted PostgreSQL instance for downstream querying and analysis.

---

## Workflow

```mermaid
graph TD
    Start([Start]) --> LoadManifest[Load Schema Manifest]
    LoadManifest --> SelectCols["1. Column Selection (Picker + Recommendations)"]
    
    SelectCols --> BitIntake["2. Bit-Array Intake (Resolve bit-packed columns)"]
    
    BitIntake --> CostEst["3. Cost Estimation (Storage & compute projection)"]
    
    CostEst --> Confirm{User Confirms?}
    Confirm -- No --> Abort([End])
    Confirm -- Yes --> Ingest["4. Bulk Ingestion (Parallel transform & bulk load)"]
    
    subgraph "Ingestion Loop (per chunk)"
    Ingest --> Transform[Transform Data]
    Transform --> CheckStorage{Storage Full?}
    CheckStorage -- Yes --> Interaction["Interaction (Retry / Skip / Abort)"]
    Interaction -- Retry --> Transform
    CheckStorage -- No --> Copy[SQL COPY]
    Copy --> Checkpoint[Save Chunk Checkpoint]
    Checkpoint --> NextChunk{More Chunks?}
    NextChunk -- Yes --> Ingest
    end
    
    NextChunk -- No --> Graphs["5. Graph Generation (Dashboard + Wizard)"]
    Graphs --> Done([Done])

    %% Resume logic
    subgraph "Phase Checkpoints"
    SelectCols -.-> |Resume/Overwrite| SelectCols
    BitIntake -.-> |Resume/Overwrite| BitIntake
    CostEst -.-> |Resume/Overwrite| CostEst
    Ingest -.-> |Resume/Overwrite| Ingest
    Graphs -.-> |Resume/Overwrite| Graphs
    end

    style Start fill:#f9f,stroke:#333,stroke-width:2px
    style Done fill:#f9f,stroke:#333,stroke-width:2px
    style SelectCols fill:#e1f5fe,stroke:#01579b
    style BitIntake fill:#e1f5fe,stroke:#01579b
    style CostEst fill:#e1f5fe,stroke:#01579b
    style Ingest fill:#e1f5fe,stroke:#01579b
    style Graphs fill:#e1f5fe,stroke:#01579b
    style Interaction fill:#fff9c4,stroke:#fbc02d
```

---

## Tech stack

| Layer | Technology |
|---|---|
| Language (pipeline) | Python 3.11+ |
| Language (schema scan) | Go 1.21+ |
| Database | PostgreSQL (psycopg2 via SQLAlchemy) |
| Data format | Parquet (PyArrow / DuckDB) |
| Parallelism | `ProcessPoolExecutor` (transform) + `ThreadPoolExecutor` (write) |
| CLI / TUI | Rich, Questionary |
| Plotting | Matplotlib |
| Linter | Ruff |
| Tests | pytest |

**Python dependencies** (see `requirements.txt`):

```
python-dotenv, psutil, pandas, duckdb, sqlalchemy, psycopg2-binary,
shapely, numpy, matplotlib, pyarrow, tqdm, rich, questionary, pytest
```

---

## AI Features (Optional)

Neythaleon includes a provider-agnostic AI advisor (`ai_advisor.py`) to assist you during the pipeline using large language models.

- **AI Column Advisor:** Recommends which columns to ingest based on a natural language description of your analysis goals.
- **AI Graph Advisor:** Suggests meaningful visualisations tailored to the semantics of your selected columns.
- **AI Query Builder:** Generates safe, read-only PostgreSQL `SELECT` queries from natural language questions.

**Supported Providers:**
- `anthropic` (requires `pip install anthropic`)
- `openai` (requires `pip install openai`)
- `ollama` (local, no SDK required)
- `openai_compat` (LM Studio, Together, Groq, etc.)

Configure your chosen provider and model using the `.env` variables described in the Configuration section.

---

## Installation

```bash
git clone https://github.com/LeeFred3042U/Neythaleon.git
cd Neythaleon

python -m venv venv
# Windows
venv\Scripts\activate
# Linux / macOS
source venv/bin/activate
# Git Bash
source venv/Scripts/activate

pip install -r requirements.txt
```

**Go tool** (schema manifest generator — only needed to regenerate `schema_manifest.json`):

```bash
go build -o goSchemeReader ./main.go
```

Requires Go 1.21+ and the Apache Arrow Go module (fetched automatically via `go mod download`).

---

## Configuration

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description | Example |
|---|---|---|
| `DATABASE_URL` | SQLAlchemy-compatible Postgres URL | `postgresql://user:pass@host:5432/db` |
| `DB_TABLE` | Target table name | `ingested_data` |
| `PARQUET_DIR` | Path to Parquet files (relative to project root) | `parquet_files` |
| `FAILED_DIR` | Directory for failed chunk dumps | `failed_chunks` |
| `MEDIA_DIR` | Output directory for plots and metrics CSV | `media` |
| `CHUNK_SIZE` | Rows per ingestion chunk | `100000` |
| `METRICS_FILE` | CSV path for per-chunk metrics | `media/ingestion_metrics.csv` |
| `LOG_FILE` | Log output path | `ingestion.log` |
| `CPU_THRESHOLD` | CPU % at which throttling activates | `85` |
| `RAM_THRESHOLD` | RAM % at which GC + throttle activates | `85.0` |
| `THROTTLE_DELAY_HIGH_CPU` | Sleep (seconds) when threshold exceeded | `2` |
| `THROTTLE_DELAY` | Baseline sleep between chunks (0 = off) | `0` |
| `VALIDATE_COORDS` | Validate geometry coordinates | `false` |
| `MANIFEST_FILE` | Path to schema manifest JSON | `schema_manifest.json` |
| `FAIL_FAST` | Abort on first chunk error | `false` |
| `EXPECTED_THROUGHPUT_RPS` | Used for cost-estimate compute time | `50000` |
| `GRAPH_SAMPLE_LIMIT` | Max rows sampled for plot generation | `500000` |
| `TARGET_CHUNK_MB` | Target chunk size in MB <!-- TODO: verify this is used --> | `256` |
| `INT_COLUMNS` | Comma-separated column names to coerce to `Int64` | `year,individualCount` |
| `LLM_PROVIDER` | AI provider (`anthropic`, `openai`, `ollama`, `openai_compat`) | `anthropic` |
| `LLM_MODEL` | AI model to use | `claude-sonnet-4-5` |
| `ANTHROPIC_API_KEY` | API key for Anthropic | `sk-ant-...` |
| `OPENAI_API_KEY` | API key for OpenAI / OpenAI-compatible | `sk-proj-...` |
| `OPENAI_BASE_URL` | Endpoint for `openai_compat` | `http://localhost:1234/v1` |
| `OLLAMA_MODEL` | Local model name for Ollama | `llama3` |

---

## Usage

### 1. Generate the schema manifest (Go)

The manifest is pre-generated at `schema_manifest.json`. To regenerate it from your Parquet directory:

```bash
# Human-readable report
go run main.go parquet_files

# JSON manifest (required by the pipeline)
go run main.go --json parquet_files > schema_manifest.json
```

### 2. Run the ingestion pipeline

```bash
python main.py
```
> Note: If using git bash
```
winpty python3 main.py
```

The pipeline walks you through the following phases interactively. Each phase can be resumed, overwritten, or aborted if a previous checkpoint exists.

| Phase | Description |
|---|---|
| **Column selection** | Choose which columns to ingest via interactive checkbox or comma-separated list |
| **Bit-array intake** | Detect and resolve packed bit-array columns using a JSON encoding metadata file or auto-lookup |
| **Cost estimate** | Calculates estimated storage and compute cost before any data is transferred |
| **Ingestion** | Parallel transform (multi-process) → bulk COPY insert (multi-thread). Press `q` to gracefully stop |
| **Graph generation** | Generates a performance dashboard PNG and an interactive graph wizard |

> **Storage full handling:** If the database reports a disk-full error (e.g., Neon project size limit), the pipeline pauses and prompts you to **[R]etry**, **[S]kip**, or **[A]bort** before continuing.

### 3. Plot the performance dashboard independently

```bash
python -m plot.cli
```

Output is written to `media/dashboard_yellow.png`.

---

## Testing

### Downloading test data

The test dataset is the OBIS H3-level-7 species grid, hosted on a public S3 bucket. Files are downloaded without extensions and must be renamed.

**Windows PowerShell:**

```powershell
aws s3 cp --recursive s3://obis-products/speciesgrids/h3_7 . --no-sign-request

Get-ChildItem -File -Recurse | Where-Object {
    $_.Extension -eq ""
} | Rename-Item -NewName {
    $_.Name + ".parquet"
}
```

**Git Bash (Windows) / Linux / Arch:**

```bash
aws s3 cp --recursive s3://obis-products/speciesgrids/h3_7 . --no-sign-request

find . -type f ! -name "*.*" -exec bash -c '
for f; do
    mv "$f" "$f.parquet"
done
' bash {} +
```

**Safer variant (only rename files inside `h3_7`):**

```bash
find ./h3_7 -type f ! -name "*.*" -exec bash -c '
for f; do
    mv "$f" "$f.parquet"
done
' bash {} +
```

**Verify files are valid Parquet (Linux / Git Bash):**

```bash
file somefile.parquet
```

### Running the test suite

```bash
# Activate the venv first
venv\Scripts\activate        # Windows
source venv/bin/activate     # Linux / macOS

python -m pytest tests/
```

| Test file | Covers |
|---|---|
| `test_transform.py` | Null-byte cleaning, special character stripping, integer coercion, WKB→WKT geometry |
| `test_db_utils.py` | `_prepare_for_copy` — nullable ints, booleans, string escaping |
| `test_decoder.py` | Bit-array unpacking and null propagation |
| `test_ingest_runner.py` | Storage-full error handling, interactive prompt logic |
| `test_scheme_utils.py` | SQL type coercion (int, float, bool, datetime) |
| `test_metrics.py` | Metric tracking and field normalization |
| `test_checkpoint.py` | Checkpoint save/load/resume lifecycle |
| `test_recommender.py` | Column exclusion recommendations by density |

---

## Project structure

```
Neythaleon/
├── main.py                  # Pipeline entry point — orchestrates all phases
├── ai_advisor.py            # AI assistant for column selection, graphing, and SQL querying
├── main.go                  # Go schema scanner — reads Parquet metadata, emits JSON manifest
├── schema_manifest.json     # Pre-generated schema manifest (commit or regenerate as needed)
├── .env.example             # Environment variable template
├── requirements.txt         # Python dependencies
├── go.mod / go.sum          # Go module files
│
├── ingest/
│   ├── config.py            # Loads all env vars via dotenv
│   ├── ingest_runner.py     # Core orchestration: process pool → thread pool → COPY insert
│   ├── db_utils.py          # copy_insert, _prepare_for_copy, schema helpers
│   ├── transform.py         # Per-chunk data cleaning and type enforcement
│   ├── parquet_utils.py     # Parquet streaming and schema inference
│   ├── manifest.py          # Schema manifest loading and column inference
│   ├── scheme_utils.py      # DataFrame → SQL type coercion
│   ├── metrics.py           # Per-chunk metrics tracking and CSV persistence
│   └── logging_config.py    # Logging setup
│
├── bitarray/
│   ├── decoder.py           # Unpacks packed bit columns into individual boolean columns
│   ├── detector.py          # Identifies bit-array columns from manifest metadata
│   ├── lookup.py            # Auto-resolution of bit-array encoding definitions
│   └── metadata_loader.py   # Loads and validates bit-array encoding JSON
│
├── checkpoint/
│   └── checkpoint.py        # Phase-level checkpoint: save, load, resume/overwrite prompt
│
├── selector/
│   ├── column_selector.py   # Interactive column picker (checkbox or manual entry)
│   └── recommender.py       # Recommends columns to exclude based on data density
│
├── cost/
│   └── calculator.py        # Pre-ingestion cost estimate (storage, network, compute)
│
├── plot/
│   ├── cli.py               # Standalone dashboard entry point
│   ├── dashboard.py         # Renders 4-panel PNG from metrics CSV
│   ├── plotting_panels.py   # Individual panel renderers (throughput, CPU, memory, batch time)
│   ├── metrics_loader.py    # Fuzzy CSV loader for metrics files
│   └── user_graphs.py       # Interactive graph wizard against the ingested database
│
├── tests/                   # pytest test suite (no live DB required)
├── media/                   # Generated plots and metrics CSV
├── failed_chunks/           # Parquet dumps of chunks that failed to insert
└── parquet_files/           # Input data directory
```

---

## Citation

OBIS (2024). speciesgrids (version 0.2.0). Ocean Biodiversity Information System. Intergovernmental Oceanographic Commission of UNESCO. https://doi.org/10.5281/zenodo.19392660

GBIF.org (1 May 2024) GBIF Occurrence Data https://doi.org/10.15468/dl.ubwn8z

OBIS (25 October 2023) OBIS Occurrence Snapshot. Ocean Biodiversity Information System. Intergovernmental Oceanographic Commission of UNESCO. https://obis.org.

World Register of Marine Species. Available from https://www.marinespecies.org at VLIZ. Accessed 2024-05-01. doi:10.14284/170.

IUCN. 2023. The IUCN Red List of Threatened Species. Version 2023-1. https://www.iucnredlist.org. Accessed on 13 May 2024.

Gearty W, Chamberlain S (2022). rredlist: IUCN Red List Client. R package version 0.7.1, https://CRAN.R-project.org/package=rredlist.


* GitHub URl <https://github.com/iobis/speciesgrids>
---
## License

MIT License — Copyright (c) 2025 LeeFred3042U. See [LICENSE](LICENSE) for full text.