package producers

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"time"
)

type SaramaProducer struct {
	brokers []string
	topic   string
	async   bool
	cfg     *sarama.Config
}

func NewSaramaProducer(brokers []string, topic string, async bool) *SaramaProducer {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true
	//cfg.ClientID = "go-sarama-producer"

	cfg = enableSaramaConfig(cfg)

	return &SaramaProducer{
		brokers: brokers,
		topic:   topic,
		async:   async,
		cfg:     cfg,
	}
}

func (p *SaramaProducer) Run(ctx context.Context, messages []string) error {
	if p.async {
		log.Println("[sarama] runAsync")
		return p.runAsync(ctx, messages)
	}
	log.Println("[sarama] runSync")
	return p.runSync(ctx, messages)
}

func (p *SaramaProducer) runSync(ctx context.Context, messages []string) error {
	producer, err := sarama.NewSyncProducer(p.brokers, p.cfg)
	if err != nil {
		return err
	}
	defer producer.Close()

	for _, msg := range messages {
		m := &sarama.ProducerMessage{
			Topic: p.topic,
			Value: sarama.StringEncoder(msg),
		}
		partition, offset, err := producer.SendMessage(m)
		if err != nil {
			log.Printf("[sarama] ошибка отправки: %v", err)
			return err
		}
		log.Printf("[sarama] ✅ sync: topic=%s partition=%d offset=%d msg=%s", p.topic, partition, offset, msg)
	}

	return nil
}

func (p *SaramaProducer) runAsync(ctx context.Context, messages []string) error {
	p.cfg.Producer.Return.Successes = true
	p.cfg.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(p.brokers, p.cfg)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		producer.AsyncClose()
		<-shutdownCtx.Done()
	}()

	go func() {
		for {
			select {
			case succ := <-producer.Successes():
				if succ != nil {
					//log.Printf("[sarama] ✅ async: topic=%s partition=%d offset=%d", succ.Topic, succ.Partition, succ.Offset)
				}
			case err := <-producer.Errors():
				if err != nil {
					log.Printf("[sarama] ❌ error: %v", err.Err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	for _, msg := range messages {
		producer.Input() <- &sarama.ProducerMessage{
			Topic: p.topic,
			Value: sarama.StringEncoder(msg),
		}
	}

	return nil
}

func enableSaramaConfig(cfg *sarama.Config) *sarama.Config {
	cfg.Net.TLS.Enable = true

	cert, err := tls.LoadX509KeyPair("../kafka/client.crt", "../kafka/client.key")
	if err != nil {
		log.Fatalf("Failed to load client certificate: %v", err)
	}

	caCert, err := os.ReadFile("../kafka/ca.crt")
	if err != nil {
		log.Fatalf("Failed to read CA certificate: %v", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		log.Fatalf("Failed to append CA certificate")
	}

	cfg.Net.TLS.Config = &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
		//InsecureSkipVerify: true, // отключает проверку CN/SAN
	}

	return cfg
}
