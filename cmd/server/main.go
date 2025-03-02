package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sahil3304/learn-pub-sub-starter/internal/pubsub"
	"github.com/sahil3304/learn-pub-sub-starter/internal/routing"
)

func connectRabbitMQ(connectionString string) *amqp.Connection {
	var connection *amqp.Connection
	var err error
	for i := 0; i < 5; i++ { // Retry 5 times
		connection, err = amqp.Dial(connectionString)
		if err == nil {
			return connection
		}
		fmt.Printf("Retrying RabbitMQ connection... (%d/5)\n", i+1)
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("Failed to connect to RabbitMQ after retries: %v", err)
	return nil
}

func main() {
	fmt.Println("Starting Peril server...")

	connectionString := "amqp://guest:guest@localhost:5672/"
	connection := connectRabbitMQ(connectionString)
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		log.Fatal("error opening channel")
	}
	pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})

	fmt.Println("Connected to RabbitMQ successfully!")

	// Graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Shutting down Peril server...")
}
