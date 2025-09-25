package consumers

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
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
	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

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
		log.Printf("[sarama] Consume message: topic=%s partition=%d offset=%d value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		sess.MarkMessage(msg, "")
		h.msg++

		if h.maxMsg > 0 && h.msg >= h.maxMsg {
			return nil
		}
	}
	return nil
}
