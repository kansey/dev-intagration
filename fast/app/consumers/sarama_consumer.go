package consumers

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/Shopify/sarama"
	"log"
	"os"
)

type SaramaConsumer struct {
	brokers []string
	topic   string
	groupID string
	maxMsg  int
}

func NewSaramaConsumer(brokers []string, topic, groupID string, maxMsg int) *SaramaConsumer {
	if groupID == "" {
		groupID = "sarama-consumers-group"
	}
	return &SaramaConsumer{
		brokers: brokers,
		topic:   topic,
		groupID: groupID,
		maxMsg:  maxMsg,
	}
}

func (c *SaramaConsumer) Run(ctx context.Context) error {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.MaxVersion
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Fetch.Max = 50 * 1024 * 1024

	enableSaramaConfig(cfg)
	group, err := sarama.NewConsumerGroup(c.brokers, c.groupID, cfg)
	if err != nil {
		return err
	}
	defer group.Close()

	handler := &consumerGroupHandler{topic: c.topic, maxMsg: c.maxMsg, msg: 0}

	log.Printf("[sarama] начинаю читать из топика: %s", c.topic)

	for {
		if err := group.Consume(ctx, []string{c.topic}, handler); err != nil {
			log.Printf("[sarama] ошибка Consume: %v", err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if handler.msg >= handler.maxMsg {
			log.Printf("[sarama]  msg=%d maxMsg=%d",
				handler.msg, handler.maxMsg)
			break
		}
	}

	return nil
}

type consumerGroupHandler struct {
	topic  string
	maxMsg int
	msg    int
}

func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		//log.Printf("[sarama] Consume message: topic=%s partition=%d offset=%d value=%s",
		//	msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		sess.MarkMessage(msg, "")
		h.msg++

		if h.maxMsg > 0 && h.msg >= h.maxMsg {
			return nil
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
