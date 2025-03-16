package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sahil3304/learn-pub-sub-starter/internal/gamelogic"
	"github.com/sahil3304/learn-pub-sub-starter/internal/pubsub"
	"github.com/sahil3304/learn-pub-sub-starter/internal/routing"
)

func HandleLogs(log routing.GameLog) pubsub.AckType {
	defer fmt.Println(">")
	err := gamelogic.WriteLog(log)
	if err != nil {
		fmt.Printf("Error writing log: %v\n", err)
		return pubsub.NackRequeue
	}
	return pubsub.Ack
}

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
	defer channel.Close()
	if err != nil {
		log.Fatal("error opening channel")
	}

	fmt.Println("Connected to RabbitMQ successfully!")
	_, queu, err := pubsub.DeclareAndBind(
		connection,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+"."+"*",
		pubsub.SimpleQueueDurable,
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queu.Name)
	gamelogic.PrintServerHelp()

	err = pubsub.SubscribeGOB(connection, "peril_topic", "game_logs", "game_logs.*", pubsub.SimpleQueueDurable, HandleLogs)
	if err != nil {
		log.Fatalf("Failed to subscribe to game logs: %v", err)
	}

	i := 0
	for i = 0; i < 1; {
		s := gamelogic.GetInput()
		if len(s) == 0 {
			continue
		} else {
			l := log.Default()
			if s[0] == "pause" {
				l.Print("sending pause")
				pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			} else if s[0] == "resume" {
				l.Println("sending resume")
				pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			} else if s[0] == "quit" {
				l.Println("quit")
				break
			} else {
				l.Println("wrong command")
			}
		}

	}

	// Graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Shutting down Peril server...")
}
