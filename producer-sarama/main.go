package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gofiber/fiber/v2"
)

// Comment struct
type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {

	app := fiber.New()
	api := app.Group("/api/v1") // /api

	api.Post("/comments", createComment)

	app.Listen(":8080")

}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Metadata.Timeout = time.Second * 30
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 1
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1

	// s.SaramaConfig.Producer.RequiredAcks = sarama.WaitForAll

	// s.SaramaConfig.Producer.Return.Successes = true
	// s.SaramaConfig.Producer.Return.Errors = true

	// s.SaramaConfig.Producer.Retry.Max = 0 // disable retry, use application level retry
	// s.SaramaConfig.Producer.Flush.Bytes = 1024 * 1024
	// s.SaramaConfig.Producer.Flush.Frequency = time.Millisecond
	// s.SaramaConfig.Net.MaxOpenRequests = 5

	// s.SaramaConfig.Producer.Flush.MaxMessages = 10
	// s.SaramaConfig.Producer.Flush.Messages = 10
	// s.SaramaConfig.Producer.MaxMessageBytes = 10 * 1024 * 1024

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func PushCommentToQueue(topic string, message []byte) error {

	brokersUrl := []string{"kafka:9092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

// createComment handler
func createComment(c *fiber.Ctx) error {

	// Instantiate new Message struct
	cmt := new(Comment)

	//  Parse body into comment struct
	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}
	// convert body into bytes and send it to kafka
	cmtInBytes, err := json.Marshal(cmt)
	err = PushCommentToQueue("comments", cmtInBytes)
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error pushing comment to queue",
		})
		return err
	}

	// Return Comment in JSON format
	err = c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": cmt,
	})
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error creating product",
		})
		return err
	}

	return err
}
