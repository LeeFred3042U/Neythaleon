package main

import (
	"encoding/json"
	"flag"
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

type ColStat struct {
	PhysicalType string
	LogicalType  string
	TotalValues  int64
	TotalNulls   int64
	TypeLength   int32
	Compression  string
	Encoding     string
}

type FileResult struct {
	Failed    bool
	SizeBytes int64
	Rows      int64
	RowGroups int64
	ColStats  map[string]*ColStat
}

type Totals struct {
	Files       int64
	FailedFiles int64
	Rows        int64
	RowGroups   int64
	SizeBytes   int64
	Columns     map[string]*ColStat
}

type colManifest struct {
	PhysicalType string  `json:"physical_type"`
	LogicalType  string  `json:"logical_type"`
	TotalValues  int64   `json:"total_values"`
	TotalNulls   int64   `json:"total_nulls"`
	DensityPct   float64 `json:"density_pct"`
	TypeLength   int32   `json:"type_length,omitempty"`
	Compression  string  `json:"compression,omitempty"`
	Encoding     string  `json:"encoding,omitempty"`
}

type storageEstimateGB struct {
	ParquetGB       float64 `json:"parquet_gb"`
	PGOverheadGB    float64 `json:"pg_overhead_gb"`
	PGUnoptimisedGB float64 `json:"pg_unoptimised_gb"`
	PGOptimisedGB   float64 `json:"pg_optimised_gb"`
}

type manifest struct {
	ValidFiles      int64                    `json:"valid_files"`
	FailedFiles     int64                    `json:"failed_files"`
	TotalRows       int64                    `json:"total_rows"`
	TotalSizeBytes  int64                    `json:"total_size_bytes"`
	ElapsedSec      float64                  `json:"elapsed_sec"`
	StorageEstimate storageEstimateGB        `json:"storage_estimate_gb"`
	Columns         map[string]*colManifest  `json:"columns"`
}

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

	for i := 0; i < schema.NumColumns(); i++ {
		col := schema.Column(i)
		name := col.Name()

		phys := col.PhysicalType().String()
		logical := "<none>"
		if col.LogicalType() != nil {
			logical = col.LogicalType().String()
		}

		var vals, nulls int64
		var typeLength int32
		var compression, encoding string

		// Capture TypeLength for FIXED_LEN_BYTE_ARRAY
		if phys == "FIXED_LEN_BYTE_ARRAY" {
			typeLength = int32(col.TypeLength())
		}

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

			// Capture compression and encoding from first row group only
			if rg == 0 {
				compression = colChunk.Compression().String()
				encs := colChunk.Encodings()
				if len(encs) > 0 {
					encoding = encs[0].String()
				}
			}
		}

		res.ColStats[name] = &ColStat{
			PhysicalType: phys,
			LogicalType:  logical,
			TotalValues:  vals,
			TotalNulls:   nulls,
			TypeLength:   typeLength,
			Compression:  compression,
			Encoding:     encoding,
		}
	}

	return res
}

func worker(jobs <-chan string, results chan<- FileResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for p := range jobs {
		results <- inspect(p)
	}
}

func main() {
	jsonFlag := flag.Bool("json", false, "emit JSON manifest to stdout instead of human-readable report")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: go run main.go [--json] <folder>")
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	root := args[0]
	start := time.Now()
	numWorkers := runtime.NumCPU()

	jobs := make(chan string, numWorkers*2)
	results := make(chan FileResult, numWorkers*2)
	var wg sync.WaitGroup

	totals := &Totals{
		Columns: make(map[string]*ColStat),
	}

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
						TypeLength:   stat.TypeLength,
						Compression:  stat.Compression,
						Encoding:     stat.Encoding,
					}
				}
				totals.Columns[name].TotalValues += stat.TotalValues
				totals.Columns[name].TotalNulls += stat.TotalNulls
			}
		}
		close(aggDone)
	}()

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go worker(jobs, results, &wg)
	}

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
		fmt.Fprintf(os.Stderr, "Error walking directory: %v\n", err)
		os.Exit(1)
	}

	close(jobs)
	wg.Wait()
	close(results)
	<-aggDone

	elapsed := time.Since(start)

	var colNames []string
	for name := range totals.Columns {
		colNames = append(colNames, name)
	}
	sort.Strings(colNames)

	var totalRawDataBytes, totalDictDataBytes int64

	type colResult struct {
		name     string
		stat     *ColStat
		density  float64
		nonNulls int64
		rawBytes int64
		dictBytes int64
	}

	colResults := make([]colResult, 0, len(colNames))
	for _, name := range colNames {
		stat := totals.Columns[name]
		nonNulls := stat.TotalValues - stat.TotalNulls
		density := 0.0
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
		case "FIXED_LEN_BYTE_ARRAY":
			typeLen := int64(8)
			if stat.TypeLength > 0 {
				typeLen = int64(stat.TypeLength)
			}
			rawBytes = nonNulls * typeLen
			dictBytes = rawBytes
		default:
			rawBytes = nonNulls * 8
			dictBytes = rawBytes
		}

		totalRawDataBytes += rawBytes
		totalDictDataBytes += dictBytes

		colResults = append(colResults, colResult{
			name: name, stat: stat,
			density: density, nonNulls: nonNulls,
			rawBytes: rawBytes, dictBytes: dictBytes,
		})
	}

	numCols := int64(len(totals.Columns))
	pgTupleHeaderSize := int64(24)
	pgNullBitmapSize := (numCols + 7) / 8
	pgRowOverhead := pgTupleHeaderSize + pgNullBitmapSize
	pgTotalOverhead := totals.Rows * pgRowOverhead

	gb := float64(1024 * 1024 * 1024)
	parquetGB := float64(totals.SizeBytes) / gb
	pgRawGB := float64(pgTotalOverhead+totalRawDataBytes) / gb
	pgOptimisedGB := float64(pgTotalOverhead+totalDictDataBytes) / gb
	pgOverheadGB := float64(pgTotalOverhead) / gb

	if *jsonFlag {
		m := manifest{
			ValidFiles:     totals.Files,
			FailedFiles:    totals.FailedFiles,
			TotalRows:      totals.Rows,
			TotalSizeBytes: totals.SizeBytes,
			ElapsedSec:     elapsed.Seconds(),
			StorageEstimate: storageEstimateGB{
				ParquetGB:       parquetGB,
				PGOverheadGB:    pgOverheadGB,
				PGUnoptimisedGB: pgRawGB,
				PGOptimisedGB:   pgOptimisedGB,
			},
			Columns: make(map[string]*colManifest, len(colResults)),
		}
		for _, cr := range colResults {
			m.Columns[cr.name] = &colManifest{
				PhysicalType: cr.stat.PhysicalType,
				LogicalType:  cr.stat.LogicalType,
				TotalValues:  cr.stat.TotalValues,
				TotalNulls:   cr.stat.TotalNulls,
				DensityPct:   cr.density,
				TypeLength:   cr.stat.TypeLength,
				Compression:  cr.stat.Compression,
				Encoding:     cr.stat.Encoding,
			}
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(m); err != nil {
			fmt.Fprintf(os.Stderr, "JSON encode error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Println("\n_____COLUMN DENSITY REPORT_____")
	for _, cr := range colResults {
		fmt.Printf(
			"- %-30s | %-10s | %-10s | Density: %6.2f%% (Vals: %d, Nulls: %d)\n",
			cr.name, cr.stat.PhysicalType, cr.stat.LogicalType,
			cr.density, cr.stat.TotalValues, cr.stat.TotalNulls,
		)
	}

	fmt.Println("\n_____TOTALS_____")
	fmt.Printf("Valid Files:   %d\n", totals.Files)
	fmt.Printf("Failed Files:  %d (Skipped silently)\n", totals.FailedFiles)
	fmt.Printf("Total Rows:    %d\n", totals.Rows)
	fmt.Printf("Total Columns: %d\n", numCols)
	fmt.Printf("Elapsed Time:  %s\n", elapsed)

	fmt.Println("\n_____STORAGE ESTIMATION_____")
	fmt.Printf("Current Parquet Disk Size:       %.2f GB (Highly compressed, columnar)\n", parquetGB)
	fmt.Printf("Est. PG Overhead (Tuples/Nulls): %.2f GB (Just headers & null bitmaps!)\n", pgOverheadGB)
	fmt.Printf("Est. PG Unoptimized (Strings):   ~%.2f GB (Row-based, text columns)\n", pgRawGB)
	fmt.Printf("Est. PG Optimized (WoRMS/Dict):  ~%.2f GB (Row-based, INT dimension tables)\n", pgOptimisedGB)
}
