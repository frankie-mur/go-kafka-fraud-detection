package main

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/frankie-mur/go-kafka/pkg/bankaccount"
	"log"
	"os"
	"strconv"
)

const (
	KafkaServerAddress = "localhost:9092"
	KafkaTopic         = "notifications"
)

func setupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress},
		config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}
	return producer, nil
}

func sendKafkaMessage(producer sarama.SyncProducer, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Value: sarama.ByteEncoder(message),
	}
	_, _, err := producer.SendMessage(msg)
	return err
}

func main() {
	p, err := setupProducer()
	if err != nil {
		log.Fatalf("Failed to setup producer: %v", err.Error())
	}
	defer p.Close()
	////Get size arguments from command line if provided
	size := 1
	if len(os.Args) > 1 {
		size, err = strconv.Atoi(os.Args[1])
		if err != nil {
			fmt.Printf("Please provide a valid size")
			os.Exit(1)
		}
	}
	for i := 0; i < size; i++ {
		//Generate fake bank account data
		bank, err := bankaccount.GenerateBankAccount()
		if err != nil {
			fmt.Printf("Failed to generate bank account: %v", err.Error())
		}
		//Convert the bank account data to []byte
		data, err := json.Marshal(bank)
		if err != nil {
			fmt.Printf("Failed to marshal bank account: %v", err.Error())
		}
		//Send the message
		err = sendKafkaMessage(p, data)
	}
}
