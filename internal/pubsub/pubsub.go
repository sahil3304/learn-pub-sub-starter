package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack         AckType = iota // Successfully processed
	NackRequeue                // Failed, requeue for retry
	NackDiscard                // Failed, discard the message
)

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
func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var b bytes.Buffer
	g := gob.NewEncoder(&b)
	err := g.Encode(val)
	if err != nil {
		fmt.Print("Error in gobbing ")
		return err
	}
	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Body:        b.Bytes(),
			ContentType: "application/gob",
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
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		}, // arguments
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
func SubscribeGOB[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		fmt.Printf("Error while subscribing: %v\n", err)
		return err
	}

	conchannel, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		fmt.Printf("Error while consuming: %v\n", err)
		return err
	}

	go func() {
		for i := range conchannel {
			var msg T
			var b bytes.Buffer
			b.Write(i.Body)

			g := gob.NewDecoder(&b)
			if decodeErr := g.Decode(&msg); decodeErr != nil {
				fmt.Printf("Error in message consumption: %v\n", decodeErr)
				i.Nack(false, false)
				continue
			}

			ack := handler(msg)
			switch ack {
			case Ack:
				i.Ack(false)
				fmt.Println("Message acknowledged")
			case NackRequeue:
				i.Nack(false, true)
				fmt.Println("Message requeued")
			case NackDiscard:
				i.Nack(false, false)
				fmt.Println("Message discarded")
			}
		}
	}()

	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
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
				i.Nack(false, false)
				continue
			}
			ack := handler(msg)
			switch ack {
			case Ack:
				i.Ack(false)
				fmt.Println("msg acknowledged")
			case NackRequeue:
				i.Nack(false, true)
				fmt.Println("message requed")
			case NackDiscard:
				i.Nack(false, false)
				fmt.Print("message discarded")
			}

		}
	}()
	return nil

}
