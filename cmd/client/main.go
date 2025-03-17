package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sahil3304/learn-pub-sub-starter/internal/gamelogic"
	"github.com/sahil3304/learn-pub-sub-starter/internal/pubsub"
	"github.com/sahil3304/learn-pub-sub-starter/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(t routing.PlayingState) pubsub.AckType {
	return func(t routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(t)
		return pubsub.Ack
	}
}
func handlerMove(gs *gamelogic.GameState, conn *amqp.Connection) func(move gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print(">")
		outcome := gs.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutcomeSamePlayer:
			fmt.Println("same player no need to war")
			return pubsub.NackDiscard
		case gamelogic.MoveOutComeSafe:
			fmt.Println("not affected no units in area")
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			fmt.Println("war between " + move.Player.Username + " and " + gs.Player.Username)
			c, _ := conn.Channel()
			err := pubsub.PublishJSON(c, "peril_topic", routing.WarRecognitionsPrefix+"."+gs.GetUsername(), gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.Player,
			})
			if err != nil {
				fmt.Printf("failed to publish move %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}

}
func handlerWar(gs *gamelogic.GameState, c *amqp.Connection) func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(rw)
		log.Printf("War outcome: %v, Winner: %s, Loser: %s", outcome, winner, loser)

		var message string
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			log.Println("Not involved in this war, requeuing message...")
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			log.Println("No units in overlapping location, discarding message...")
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeDraw, gamelogic.WarOutcomeYouWon:
			if outcome == gamelogic.WarOutcomeDraw {
				message = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			} else {
				message = fmt.Sprintf("%s won a war against %s", winner, loser)
			}

			log.Println("Publishing war event log:", message)

			cc, err := c.Channel()
			if err != nil {
				log.Println("Failed to open a channel for publishing logs:", err)
				return pubsub.NackRequeue
			}
			defer cc.Close()

			gameLog := routing.GameLog{
				CurrentTime: time.Now(),
				Username:    winner,
				Message:     message,
			}

			err = pubsub.PublishGob(cc, routing.ExchangePerilTopic, routing.GameLogSlug+"."+rw.Attacker.Username, gameLog)
			if err != nil {
				log.Println("Failed to publish game log:", err)
				return pubsub.NackRequeue
			}

			log.Println("Game log successfully published.")
			return pubsub.Ack
		default:
			log.Println("Unexpected outcome, discarding message...")
			return pubsub.NackDiscard
		}
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
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "army_moves."+username, "army_moves.*", pubsub.SimpleQueueTransient, handlerMove(gs, conn))
	if err != nil {
		fmt.Printf("error in subscribe json in client to client  %v", err)
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "war", "war.*", pubsub.SimpleQueueDurable, handlerWar(gs, conn))
	if err != nil {
		fmt.Printf("error in subscribe json in client to client  %v", err)
	}
	ch, _ := conn.Channel()
	defer ch.Close()

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

			if len(words) == 2 {
				if len(words) < 2 {
					fmt.Println("Usage: spam <number_of_logs>")
					return
				}

				n, err := strconv.Atoi(words[1])
				if err != nil || n <= 0 {
					fmt.Println("Invalid number of logs")
					return
				}

				logMsg := gamelogic.GetMaliciousLog()

				for i := 0; i < n; i++ {
					err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, "game_logs."+username, routing.GameLog{
						Message:     logMsg,
						CurrentTime: time.Now(),
					})
					if err != nil {
						fmt.Printf("Failed to send malicious log %v\n", err)
					} else {
						fmt.Println("Log sent")
					}
				}
			}

		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command")
		}
	}

}
