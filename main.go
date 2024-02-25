package main

import (
	"context"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type saramaHandler struct{}

//setup callback for example can be opening up connections db
func (h *saramaHandler) Setup(sarama.ConsumerGroupSession) error   { 
	
	
	return nil 
}

//assuming db connectiion is opened up, can close the connections hore 
func (h *saramaHandler) Cleanup(sarama.ConsumerGroupSession) error { 
	
	return nil 
}

func (h *saramaHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := context.Background()
	for {
		select {
		case msg := <-claim.Messages():
			decodedEvent, err := DecodeTestEvent(msg.Value)
			if err != nil {
				log.Printf("Failed to decode event: %v", err)
				continue
			}

			log.Printf("Received message: '%s' from user '%s'\n", decodedEvent.Message, decodedEvent.From)

			sess.MarkMessage(msg, "")
		case <-ctx.Done():
			return nil
		}
	}
}
func main() {
	runConsumer("test_events")
}

func runConsumer(topic string) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_6_0_0
	config.Consumer.Return.Errors = true

	client, err := sarama.NewClient([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient("my_group", client)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	ctx := context.Background()
	log.Println("listeing")
	for {
		topics := []string{topic}
		err := consumerGroup.Consume(ctx, topics, &saramaHandler{})
		if err != nil {
			log.Printf("Failed to start consuming messages: %v", err)
		}
		if ctx.Err() != nil {
			break
		}
		<-time.After(10 * time.Second)
	}

	log.Println("closing")
	consumerGroup.Close()
}
