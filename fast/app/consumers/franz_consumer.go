package consumers

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type FranzConsumer struct {
	client      *kgo.Client
	topic       string
	maxMessages int
}

func NewFranzConsumer(brokers []string, topic string, groupID string, maxMsg int) *FranzConsumer {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(groupID),
		//kgo.AutoCommitMarks(),
		kgo.FetchMaxBytes(50_000_000),
		kgo.DialTLSConfig(enableTlsConfig()),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("не удалось создать franz-go consumer: %v", err)
	}

	return &FranzConsumer{
		client:      client,
		topic:       topic,
		maxMessages: maxMsg,
	}
}

func (c *FranzConsumer) Run(ctx context.Context) error {
	defer c.client.Close()

	err := c.connect(ctx)

	if err != nil {
		c.client.Close()
		return err
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	log.Printf("[franz-go] начинаю читать из топика: %s", c.topic)

	for {
		select {
		case <-ctx.Done():
			log.Println("[franz-go] контекст завершён, выходим")
			return ctx.Err()
		case <-sigs:
			log.Println("[franz-go] пойман сигнал завершения, закрываю consumer")
			return nil
		default:
			// читаем сообщения пачкой
			fetches := c.client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, e := range errs {
					log.Printf("[franz-go] ошибка получения сообщений: %v", e)
				}
				return errs[0].Err
			}

			var records []*kgo.Record

			fetches.EachRecord(func(r *kgo.Record) {
				records = append(records, r)
				//log.Printf("[franz-go] Consume message: topic=%s partition=%d offset=%d value=%s",
				//	r.Topic, r.Partition, r.Offset, string(r.Value))
				msgCount++
			})

			if err := c.client.CommitRecords(context.Background(), records...); err != nil {
				fmt.Printf("[franz-go] ошибка коммита сообщений: %v", err)
				continue
			}

			if msgCount >= c.maxMessages {
				c.client.Close()
				log.Printf("[franz-go]  msg=%d maxMsg=%d",
					msgCount, c.maxMessages)
				return nil
			}
		}
	}
}

func (c *FranzConsumer) connect(ctx context.Context) error {
	err := c.client.Ping(ctx)
	if err != nil {
		return err
	}

	return nil
}

func enableTlsConfig() *tls.Config {
	caCert, err := os.ReadFile("../kafka/ca.crt")
	if err != nil {
		log.Fatalf("Failed to read CA certificate: %v", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		log.Fatalf("Failed to append CA certificate")
	}

	clientCert, err := tls.LoadX509KeyPair("../kafka/client.crt", "../kafka/client.key")
	if err != nil {
		log.Fatalf("Failed to load client cert/key: %v", err)
	}

	return &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{clientCert},
		MinVersion:   tls.VersionTLS12,
		//InsecureSkipVerify: true,
	}
}
