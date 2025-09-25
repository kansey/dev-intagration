package producers

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
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
	if p.async {
		log.Println("[franz-go] runAsync")
		err := p.runAsync(ctx, messages)
		p.client.Close()
		return err
	}

	log.Println("[franz-go] runSync")
	err := p.runSync(ctx, messages)
	// p.client.Close()  // закрывать после отправки можно по ситуации
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
				log.Printf("[franz-go] ✅ Отправлено async: %s", msg)
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
