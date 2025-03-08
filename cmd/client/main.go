package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sahil3304/learn-pub-sub-starter/internal/gamelogic"
	"github.com/sahil3304/learn-pub-sub-starter/internal/pubsub"
	"github.com/sahil3304/learn-pub-sub-starter/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(t routing.PlayingState) {
	return func(t routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(t)
	}
}
func main() {
	fmt.Println("Starting Peril client...")
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not get username: %v", err)
	}

	gs := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.SimpleQueueTransient, handlerPause(gs))
	if err != nil {
		fmt.Printf("error in subscrib json in client %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "move":
			_, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}

			// TODO: publish the move
		case "spawn":
			err = gs.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			// TODO: publish n malicious logs
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command")
		}
	}

}
