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
func handlerMove(gs *gamelogic.GameState) func(move gamelogic.ArmyMove) {

	return func(move gamelogic.ArmyMove) {
		defer fmt.Print(">")
		gs.HandleMove(move)
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
		fmt.Printf("error in subscribe json in client to server %v", err)
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "army_moves."+username, "army_moves.*", pubsub.SimpleQueueTransient, handlerMove(gs))
	if err != nil {
		fmt.Printf("error in subscribe json in client to client  %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "move":
			move, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
			ch, _ := conn.Channel()
			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, "army_moves."+username, move)
			if err != nil {
				fmt.Printf("Failed to publish move: %v\n", err)
			} else {
				fmt.Println("Move published successfully!")
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
