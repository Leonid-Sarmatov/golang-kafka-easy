package main

import (
	"log"
	//"fmt"
	"os"
	"os/signal"
	"time"
	"encoding/json"
	"strconv"

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
	// Создание настроек
	config := sarama.NewConfig()
	// Ожидание подтверждения от локального брокера
	//config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	// Использование сжатия Snappy
	//config.Producer.Compression = sarama.CompressionSnappy
	// Частота отправки сообщений
	//config.Producer.Flush.Frequency = 500 * time.Millisecond

	// Создание продюсера
	producer, err := sarama.NewAsyncProducer([]string{kafkaBrokers}, config)
	if err != nil {
		log.Fatalf("[ERROR] Can not create new producer: %v", err)
	}

	// Закрытие продюсера
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("[ERROR] Closing producer was failed:, %v", err)
		}
	}()

	osSignalsChan := make(chan os.Signal, 1)
	signal.Notify(osSignalsChan, os.Interrupt)

	go func() {
		for {
			select {
			case success := <-producer.Successes():
				log.Printf("[OK] Successful transmit: %v\n", success.Value)
			case err := <-producer.Errors():
				log.Printf("[ERROR] Failed transmit: %v\n", err)
			}
		}
	}()

	go func() {
		for {
			jsonMessage := MyMessage {
				Instructions: "write",
				FileName: "data_1.txt",
				FileData: "Hello, file!\n"+strconv.Itoa(int(time.Now().Unix())),
			}

			jsonData, err := json.Marshal(jsonMessage)
			if err != nil {
				log.Printf("[ERROR] Encoding to JSON was failed: %v\n", err)
			}

			message := &sarama.ProducerMessage{
				Topic: kafkaTopic,
				Key:   sarama.StringEncoder("key"),
				Value: sarama.ByteEncoder(jsonData),
			}

			producer.Input() <- message
			time.Sleep(1 * time.Second)
		}
	}()
	
	<-osSignalsChan
	log.Println("Service 1 was stoped!")
}