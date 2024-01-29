package main

import (
	"log"
	//"fmt"
	"encoding/json"
	"os"
	"os/signal"
	//"strconv"
	//"time"

	"github.com/IBM/sarama"
)

const (
	kafkaBrokers = "localhost:9092"
	kafkaTopic = "example-topic"
)

type MyMessage struct {
	Instructions string `json:"instructions"`
	FileName string `json:"fileName"`
	FileData string `json:"fileData"`
}

func main() {
	consumer, err := sarama.NewConsumer([]string{kafkaBrokers}, nil)
	if err != nil {
		log.Fatalf("[ERROR] Can not create new conumer: %v", err)
	}

	partConsumer, err := consumer.ConsumePartition(kafkaTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("[ERROR] Failed to consume partition: %v", err)
	}
	defer partConsumer.Close()

	osSignalsChan := make(chan os.Signal, 1)
	signal.Notify(osSignalsChan, os.Interrupt)

	// Горутина для обработки входящих сообщений от Kafka
	go func() {
		for {
			select {
			// Чтение сообщения из Kafka
			case msg, ok := <-partConsumer.Messages():
				if !ok {
					log.Println("Channel closed, exiting goroutine")
					return
				}
				//log.Printf("[OK] Messange was recived: %v", msg)

				var myMessage MyMessage
				err := json.Unmarshal(msg.Value, &myMessage)
				if err != nil {
					log.Printf("[ERROR] Encoding JSON was failed: %v", err)
				}

				log.Printf("[OK] Messange was recived: %v", myMessage)
			}
		}
	}()

	<-osSignalsChan
	log.Println("Service 2 was stoped!")
}