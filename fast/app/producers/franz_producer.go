package producers

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"os"
	"sync"
)

type FranzProducer struct {
	brokers []string
	topic   string
	async   bool
	client  *kgo.Client
}

func NewFranzProducer(brokers []string, topic string, async bool) *FranzProducer {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.DialTLSConfig(enableTlsConfig()),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("Не удалось создать franz-go клиент: %v", err)
	}

	return &FranzProducer{
		brokers: brokers,
		topic:   topic,
		async:   async,
		client:  client,
	}
}

func (p *FranzProducer) Run(ctx context.Context, messages []string) error {
	defer p.client.Close()

	err := p.connect(ctx)
	if err != nil {
		return err
	}

	if p.async {
		log.Println("[franz-go] runAsync")
		err := p.runAsync(ctx, messages)

		return err
	}

	log.Println("[franz-go] runSync")
	err = p.runSync(ctx, messages)

	return err
}

func (p *FranzProducer) runSync(ctx context.Context, messages []string) error {
	for _, msg := range messages {
		err := p.client.ProduceSync(ctx, &kgo.Record{
			Topic: p.topic,
			Value: []byte(msg),
		}).FirstErr()
		if err != nil {
			return fmt.Errorf("ошибка синхронной отправки: %w", err)
		}
		log.Printf("[franz-go] ✅ Отправлено sync: %s", msg)
	}
	return nil
}

func (p *FranzProducer) runAsync(ctx context.Context, messages []string) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(messages))

	for _, msg := range messages {
		wg.Add(1)
		p.client.Produce(ctx, &kgo.Record{
			Topic: p.topic,
			Value: []byte(msg),
		}, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				errs <- fmt.Errorf("ошибка асинхронной отправки: %w", err)
			} else {
				//log.Printf("[franz-go] ✅ Отправлено async: %s", msg)
			}
		})
	}

	wg.Wait()
	close(errs)

	if len(errs) > 0 {
		return <-errs
	}
	return nil
}

func (p *FranzProducer) connect(ctx context.Context) error {
	err := p.client.Ping(ctx)
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
