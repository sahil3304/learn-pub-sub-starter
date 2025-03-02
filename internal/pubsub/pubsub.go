package pubsub

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	bytesBody, err := json.Marshal(val)
	if err != nil {
		log.Fatal("unable to marshal val")
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		Body:        bytesBody,
		ContentType: "application/json",
	})
}
