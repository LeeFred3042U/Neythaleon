# spikeByte
---


### The Architecture
`spikeByte` simulates a fast, lock-free preprocessing layer. By treating the telemetry storage as columnar data (Parquet in this simulation), the pipeline:
- Avoids row-materialization entirely.
- Reads only chunk metadata/footers in `O(1)` time per chunk using `apache/arrow`.
- Employs a **Multi-Producer, Single-Consumer (MPSC)** channel architecture in Go to aggregate global metrics without mutex contention.
- Strips out 0% density metrics to serialize only highly dense, active signals into a structured prompt.

---

## Setup
### Download Dataset  
- `https://github.com/iobis/obis-open-data`
- You can also use the small dataset provided in the repository, by unzipping it.
> Note: Copy the path of the unzipped folder and put it in place of `<folder_path>`

### Run
```bash
go run main.go <folder_path>
```

---

## Output

> Note: This is for demonstration purposes only. The actual output will depend on the dataset used.
> Here I used the large dataset from `iobis/obis-open-data`.

* Check `outputLog.md` for the full context serialization output.
* Here are the Totals and PostgreSQL Storage Estimations:

> Note: Elapsed Time can vary. Through multiple runs, I found it has a range of 23-40s on Windows 11, varying depending on the system resources and background processes running. Here are some examples:

**With normal background processes (24-30s):**

```bash
_____TOTALS_____
Valid Files:   6741
Failed Files:  33 (Skipped silently)
Total Rows:    223075869
Total Columns: 419
Elapsed Time:  24.4630135s

_____STORAGE ESTIMATION_____
Current Parquet Disk Size:       51.49 GB (Highly compressed, columnar)
Est. PG Overhead (Tuples/Nulls): 16.00 GB (Just headers & null bitmaps!)
Est. PG Unoptimized (Strings):   ~243.11 GB (Row-based, text columns)
Est. PG Optimized (WoRMS/Dict):  ~106.95 GB (Row-based, INT dimension tables)

```

**With heavy processes running like Brave and Slack (+33s):**

```bash
_____TOTALS_____
Valid Files:   6741
Failed Files:  33 (Skipped silently)
Total Rows:    223075869
Total Columns: 419
Elapsed Time:  38.20496s

_____STORAGE ESTIMATION_____
Current Parquet Disk Size:       51.49 GB (Highly compressed, columnar)
Est. PG Overhead (Tuples/Nulls): 16.00 GB (Just headers & null bitmaps!)
Est. PG Unoptimized (Strings):   ~243.11 GB (Row-based, text columns)
Est. PG Optimized (WoRMS/Dict):  ~106.95 GB (Row-based, INT dimension tables)

```

---

## Hardware Profile

My current system specs used for these benchmarks:

| **Component** | **Specification** |
| --- | --- |
| Processor | AMD Ryzen 7 7735HS with Radeon Graphics (3.20 GHz) |
| Installed RAM | 16.0 GB (13.3 GB usable) |
| OS Edition | Windows 11 Home Single Language |
| OS Version | 25H2 |
