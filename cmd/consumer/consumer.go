package main

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/frankie-mur/go-kafka/pkg/models"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
)

const (
	KafkaServerAddress = "localhost:9092"
	KafkaTopic         = "notifications"
)

func connectConsumer() (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// Create new consumer
	conn, err := sarama.NewConsumer([]string{KafkaServerAddress}, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func handleMessage(msg *sarama.ConsumerMessage) error {
	fmt.Printf("Received message | Topic(%s) | Message(%s) \n", msg.Topic, string(msg.Value))
	//Parse the message back into our BankAccount struct
	bankAccount := models.BankAccount{}
	err := json.Unmarshal(msg.Value, &bankAccount)
	if err != nil {
		return fmt.Errorf("failed to parse message: %w", err)
	}
	//Call our fake Veritas API
	succeeded := callVeritas(bankAccount)
	if !succeeded {
		fmt.Printf("--UNAUTHORIZED-- bank account for user %s\n", bankAccount.FirstName)
	} else {
		fmt.Printf("--AUTHORIZED-- bank account for user %s\n", bankAccount.FirstName)
	}
	return nil
}

func callVeritas(bankAccount models.BankAccount) bool {
	fmt.Printf("Calling Veritas for %s bank...\n", bankAccount.FirstName)
	//Simulate calling an actual verification service
	//Here we just randomly return true or false
	return rand.Intn(2) == 0
}

func main() {
	worker, err := connectConsumer()
	if err != nil {
		panic(err)
	}
	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	// NOTE: setting offset to OffsetNewest, consumer will only receive messages after its start
	// OffsetOldest will have all messages sent on start
	consumer, err := worker.ConsumePartition(KafkaTopic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	fmt.Println("Consumer started ")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Get signal for finish
	doneCh := make(chan struct{})

	//spins up a goroutine to fetch messages from Kafka
	//and handle them, while also monitoring for shutdown signals
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				//Handle the message received  from Kafka
				err := handleMessage(msg)
				if err != nil {
					fmt.Printf("Failed to handle message for consumer with key: %v, with error: %v\n", msg.Key, err)
				}
			case <-sigchan:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed all messages, awaiting more...")

	if err := worker.Close(); err != nil {
		panic(err)
	}

}
