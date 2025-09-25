package consumers

import (
	"context"
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

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0
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
				log.Printf("[franz-go] Consume message: topic=%s partition=%d offset=%d value=%s",
					r.Topic, r.Partition, r.Offset, string(r.Value))
				msgCount++
			})

			if err := c.client.CommitRecords(context.Background(), records...); err != nil {
				fmt.Printf("[franz-go] ошибка коммита сообщений: %v", err)
				continue
			}

			if msgCount >= c.maxMessages {
				return nil
			}
		}
	}
}
