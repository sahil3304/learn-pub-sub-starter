package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	bytesBody, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("unable to marshal value: %v", err)
	}
	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Body:        bytesBody,
			ContentType: "application/json",
		},
	)
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %v", err)
	}

	queue, err := ch.QueueDeclare(
		queueName,
		simpleQueueType == SimpleQueueDurable, // durable
		simpleQueueType != SimpleQueueDurable, // auto-delete
		false,                                 // exclusive
		false,                                 // no-wait
		nil,                                   // arguments
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %v", err)
	}

	err = ch.QueueBind(
		queue.Name,
		key,
		exchange,
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %v", err)
	}

	return ch, queue, nil
}
func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		fmt.Printf("Error while subscribing :%v \n", err)
		return err
	}
	conchannel, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		fmt.Printf("Error while consume :%v \n", err)
		return err
	}
	go func() {
		for i := range conchannel {
			var msg T
			err = json.Unmarshal(i.Body, &msg)
			if err != nil {
				fmt.Printf("error in message consumption %v \n", err)
			}
			handler(msg)
			i.Ack(false)
		}
	}()
	return nil

}
