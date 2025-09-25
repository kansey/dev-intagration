package metrics

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

// BenchmarkResult — структура с результатами замеров
type BenchmarkResult struct {
	Library      string
	Elapsed      time.Duration
	AllocMB      float64
	TotalAllocMB float64
	SysMB        float64
	NumGC        uint32
}

// Collect собирает метрики в структуру BenchmarkResult
func Collect(lib string, start time.Time) BenchmarkResult {
	elapsed := time.Since(start)

	// --- память ---
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return BenchmarkResult{
		Library:      lib,
		Elapsed:      elapsed,
		AllocMB:      float64(m.Alloc) / 1024 / 1024,
		TotalAllocMB: float64(m.TotalAlloc) / 1024 / 1024,
		SysMB:        float64(m.Sys) / 1024 / 1024,
		NumGC:        m.NumGC,
	}
}

// WriteCSV пишет результат в CSV по пути, заданному через ENV
func WriteCSV(result BenchmarkResult) {
	path := os.Getenv("BENCHMARK_FILE")
	if path == "" {
		path = "./benchmarks.csv"
		log.Printf("BENCHMARK_FILE не задан, пишу в %s", path)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		log.Printf("Ошибка открытия файла %s: %v", path, err)
		return
	}
	defer file.Close()

	// пишем заголовок, если файл пустой
	stat, _ := file.Stat()
	if stat.Size() == 0 {
		_, err := file.WriteString("lib,elapsed_ms,alloc_mb,total_alloc_mb,sys_mb,num_gc\n")
		if err != nil {
			return
		}
	}

	// строка CSV
	_, err = file.WriteString(fmt.Sprintf("%s,%d,%.2f,%.2f,%.2f,%d\n",
		result.Library,
		result.Elapsed.Milliseconds(),
		result.AllocMB,
		result.TotalAllocMB,
		result.SysMB,
		result.NumGC,
	))
	if err != nil {
		return
	}
}
