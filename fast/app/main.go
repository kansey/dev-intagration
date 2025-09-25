package main

import (
	"context"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"app/consumers"
	"app/metrics"
	"app/producers"
)

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")
	asyncStr := os.Getenv("KAFKA_ASYNC")
	lib := os.Getenv("KAFKA_LIB")
	msgCountStr := os.Getenv("KAFKA_MSG_COUNT")
	runConsumer := os.Getenv("KAFKA_RUN_CONSUMER")

	if brokers == "" || topic == "" {
		log.Fatal("Не заданы KAFKA_BROKERS или KAFKA_TOPIC")

	}

	async := strings.ToLower(asyncStr) == "true"
	brokerList := strings.Split(brokers, ",")

	var producer producers.Producer

	switch lib {
	case "franz":
		producer = producers.NewFranzProducer(brokerList, topic, async)
	default:
		producer = producers.NewSaramaProducer(brokerList, topic, async)
	}
	log.Printf("Использую библиотеку: %s", lib)

	msgCount := 1 // по умолчанию 1 сообщение
	if msgCountStr != "" {
		if n, err := strconv.Atoi(msgCountStr); err == nil && n > 0 {
			msgCount = n
		} else {
			log.Printf("Некорректное значение KAFKA_MSG_COUNT (%s), использую 1", msgCountStr)
		}
	}

	var messages []string
	for i := 0; i < msgCount; i++ {
		messages = append(messages, "Hello Kafka! #"+strconv.Itoa(i+1))
	}

	// --- метрики времени ---
	start := time.Now()

	if err := producer.Run(context.Background(), messages); err != nil {
		log.Fatalf("Ошибка работы продьюсера: %v", err)
	}

	elapsed := time.Since(start)

	// --- метрики памяти ---
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Alloc       – текущий объём памяти в куче (что реально используется)
	// TotalAlloc  – сколько памяти всего выделялось за время работы (счётчик)
	// Sys         – память, выделенная у ОС (включая резервы)
	// NumGC       – количество срабатываний GC
	log.Printf("⏱ Время выполнения: %s", elapsed)
	log.Printf("💾 Память: Alloc = %.2f MB, TotalAlloc = %.2f MB, Sys = %.2f MB, NumGC = %d",
		float64(m.Alloc)/1024/1024,
		float64(m.TotalAlloc)/1024/1024,
		float64(m.Sys)/1024/1024,
		m.NumGC)

	// собираем и пишем метрики
	//result := metrics.Collect(lib, start)
	//metrics.WriteCSV(result)

	log.Printf("Замеры записаны в файл (lib=%s)", lib)

	if runConsumer != "" {
		//start = time.Now()
		log.Println("▶ Запускаю consumers для проверки сообщений...")

		var consumer consumers.Consumer

		switch lib {
		case "franz":
			consumer = consumers.NewFranzConsumer(brokerList, topic, "consumer-"+topic, msgCount)
		default:
			consumer = consumers.NewSaramaConsumer(brokerList, topic, "consumer-"+topic, msgCount)
		}
		log.Printf("Использую библиотеку: %s", lib)

		if err := consumer.Run(context.Background()); err != nil {
			log.Fatalf("Ошибка работы consumers: %v", err)
		}

		elapsed = time.Since(start)

		// Alloc       – текущий объём памяти в куче (что реально используется)
		// TotalAlloc  – сколько памяти всего выделялось за время работы (счётчик)
		// Sys         – память, выделенная у ОС (включая резервы)
		// NumGC       – количество срабатываний GC
		log.Printf("⏱ Время выполнения: %s", elapsed)
		log.Printf("💾 Память: Alloc = %.2f MB, TotalAlloc = %.2f MB, Sys = %.2f MB, NumGC = %d",
			float64(m.Alloc)/1024/1024,
			float64(m.TotalAlloc)/1024/1024,
			float64(m.Sys)/1024/1024,
			m.NumGC)

		// собираем и пишем метрики
		result := metrics.Collect(lib, start)
		metrics.WriteCSV(result)

		log.Printf("Замеры записаны в файл (lib=%s)", lib)
	}
}
