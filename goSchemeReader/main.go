// Command inspect-parquet scans a directory tree and aggregates column-level
// statistics across Parquet files.
//
// The program reads Parquet metadata only and never materializes row data.
// Work is structured as a channel-based pipeline:
//
//   - Producer: walks the filesystem and sends parquet paths to `jobs`.
//   - Workers: inspect files concurrently and emit FileResult values.
//   - Aggregator: single goroutine that owns Totals and merges results.
//
// Concurrency relies on Go channels rather than shared-memory locking.
// Database storage estimates are derived from tuple header and null bitmap
// assumptions to approximate PostgreSQL row-based overhead.
package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/apache/arrow/go/v15/parquet/file"
)

// ColStat aggregates metadata for a single column across all processed files.
// PhysicalType and LogicalType describe schema characteristics, while the
// counters accumulate value and null totals from each row group.
type ColStat struct {
	PhysicalType string
	LogicalType  string
	TotalValues  int64
	TotalNulls   int64
}

// FileResult represents the result of inspecting a single parquet file.
// Workers emit this struct to the aggregator to avoid shared mutable state.
type FileResult struct {
	Failed    bool
	SizeBytes int64
	Rows      int64
	RowGroups int64
	ColStats  map[string]*ColStat
}

// Totals holds global aggregation state owned exclusively by the aggregator
// goroutine. Because a single consumer mutates this struct, no mutex is needed.
type Totals struct {
	Files       int64
	FailedFiles int64
	Rows        int64
	RowGroups   int64
	SizeBytes   int64
	Columns     map[string]*ColStat
}

// inspect parses parquet metadata and returns a FileResult describing the file.
// The function is stateless and safe for concurrent execution by multiple workers.
func inspect(path string) FileResult {
	res := FileResult{
		ColStats: make(map[string]*ColStat),
	}

	f, err := os.Open(path)
	if err != nil {
		res.Failed = true
		return res
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil || stat.Size() < 12 {
		res.Failed = true
		return res
	}
	res.SizeBytes = stat.Size()

	// SectionReader provides a ReaderAt implementation compatible with Windows.
	sr := io.NewSectionReader(f, 0, stat.Size())

	r, err := file.NewParquetReader(sr)
	if err != nil {
		res.Failed = true
		return res
	}
	defer r.Close()

	meta := r.MetaData()
	if meta == nil {
		res.Failed = true
		return res
	}

	schema := meta.Schema
	res.Rows = meta.NumRows
	res.RowGroups = int64(len(meta.RowGroups))

	// Schema columns are 0-indexed.
	for i := 0; i < schema.NumColumns(); i++ {
		col := schema.Column(i)
		name := col.Name()

		phys := col.PhysicalType().String()
		logical := "<none>"
		if col.LogicalType() != nil {
			logical = col.LogicalType().String()
		}

		var vals, nulls int64

		// Aggregate statistics from each row group.
		for rg := 0; rg < len(meta.RowGroups); rg++ {
			rgMeta := meta.RowGroup(rg)

			colChunk, err := rgMeta.ColumnChunk(i)
			if err != nil {
				continue
			}

			vals += colChunk.NumValues()

			stats, err := colChunk.Statistics()
			if err == nil && stats != nil && stats.HasNullCount() {
				nulls += stats.NullCount()
			}
		}

		res.ColStats[name] = &ColStat{
			PhysicalType: phys,
			LogicalType:  logical,
			TotalValues:  vals,
			TotalNulls:   nulls,
		}
	}

	return res
}

// worker consumes paths from jobs and emits inspection results.
// Multiple workers run concurrently and act as producers to the results channel.
func worker(jobs <-chan string, results chan<- FileResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for p := range jobs {
		results <- inspect(p)
	}
}

// main wires the producer, worker pool, and aggregator pipeline, then prints
// column density metrics and PostgreSQL storage estimations.
func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: go run main.go <folder>")
		os.Exit(1)
	}

	root := os.Args[1]
	start := time.Now()
	numWorkers := runtime.NumCPU()

	jobs := make(chan string, numWorkers*2)
	results := make(chan FileResult, numWorkers*2)
	var wg sync.WaitGroup

	totals := &Totals{
		Columns: make(map[string]*ColStat),
	}

	// Aggregator: single consumer responsible for merging results.
	aggDone := make(chan struct{})
	go func() {
		for res := range results {
			if res.Failed {
				totals.FailedFiles++
				continue
			}
			totals.Files++
			totals.SizeBytes += res.SizeBytes
			totals.Rows += res.Rows
			totals.RowGroups += res.RowGroups

			for name, stat := range res.ColStats {
				if _, exists := totals.Columns[name]; !exists {
					totals.Columns[name] = &ColStat{
						PhysicalType: stat.PhysicalType,
						LogicalType:  stat.LogicalType,
					}
				}
				totals.Columns[name].TotalValues += stat.TotalValues
				totals.Columns[name].TotalNulls += stat.TotalNulls
			}
		}
		close(aggDone)
	}()

	// Start worker pool sized to available CPUs.
	wg.Add(numWorkers)
	for range numWorkers {
		go worker(jobs, results, &wg)
	}

	// Producer: walk directory tree and enqueue parquet files.
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if filepath.Ext(p) == ".parquet" {
			jobs <- p
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Error walking directory: %v\n", err)
		os.Exit(1)
	}

	close(jobs)
	wg.Wait()
	close(results)
	<-aggDone

	elapsed := time.Since(start)

	// Sort column names for deterministic output ordering.
	var colNames []string
	for name := range totals.Columns {
		colNames = append(colNames, name)
	}
	sort.Strings(colNames)

	fmt.Println("\n_____COLUMN DENSITY REPORT_____")

	var totalRawDataBytes int64
	var totalDictDataBytes int64

	for _, name := range colNames {
		stat := totals.Columns[name]

		density := 0.0
		nonNulls := stat.TotalValues - stat.TotalNulls
		if stat.TotalValues > 0 {
			density = float64(nonNulls) / float64(stat.TotalValues) * 100
		}

		var rawBytes, dictBytes int64
		switch stat.PhysicalType {
		case "INT64", "DOUBLE":
			rawBytes = nonNulls * 8
			dictBytes = rawBytes
		case "INT32", "FLOAT":
			rawBytes = nonNulls * 4
			dictBytes = rawBytes
		case "BOOLEAN":
			rawBytes = nonNulls * 1
			dictBytes = rawBytes
		case "BYTE_ARRAY":
			rawBytes = nonNulls * 15
			dictBytes = nonNulls * 4
		default:
			rawBytes = nonNulls * 8
			dictBytes = rawBytes
		}

		totalRawDataBytes += rawBytes
		totalDictDataBytes += dictBytes

		fmt.Printf("- %-30s | %-10s | %-10s | Density: %6.2f%% (Vals: %d, Nulls: %d)\n",
			name,
			stat.PhysicalType,
			stat.LogicalType,
			density,
			stat.TotalValues,
			stat.TotalNulls,
		)
	}

	numCols := int64(len(totals.Columns))
	pgTupleHeaderSize := int64(24)
	pgNullBitmapSize := (numCols + 7) / 8
	pgRowOverhead := pgTupleHeaderSize + pgNullBitmapSize
	pgTotalOverhead := totals.Rows * pgRowOverhead

	gb := float64(1024 * 1024 * 1024)
	parquetGB := float64(totals.SizeBytes) / gb
	pgRawGB := float64(pgTotalOverhead+totalRawDataBytes) / gb
	pgOptimizedGB := float64(pgTotalOverhead+totalDictDataBytes) / gb

	fmt.Println("\n_____TOTALS_____")
	fmt.Printf("Valid Files:   %d\n", totals.Files)
	fmt.Printf("Failed Files:  %d (Skipped silently)\n", totals.FailedFiles)
	fmt.Printf("Total Rows:    %d\n", totals.Rows)
	fmt.Printf("Total Columns: %d\n", numCols)
	fmt.Printf("Elapsed Time:  %s\n", elapsed)

	fmt.Println("\n_____STORAGE ESTIMATION_____")
	fmt.Printf("Current Parquet Disk Size:       %.2f GB (Highly compressed, columnar)\n", parquetGB)
	fmt.Printf("Est. PG Overhead (Tuples/Nulls): %.2f GB (Just headers & null bitmaps!)\n", float64(pgTotalOverhead)/gb)
	fmt.Printf("Est. PG Unoptimized (Strings):   ~%.2f GB (Row-based, text columns)\n", pgRawGB)
	fmt.Printf("Est. PG Optimized (WoRMS/Dict):  ~%.2f GB (Row-based, INT dimension tables)\n", pgOptimizedGB)
}