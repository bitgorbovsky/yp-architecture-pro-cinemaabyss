package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

// Models
type MovieEvent struct {
	MovieId     *int     `json:"movie_id" validate:"required"`
	Title       *string  `json:"title" validate:"required"`
	Action      *string  `json:"action" validate:"required"`
	UserId      int      `json:"user_id"`
	Rating      float64  `json:"rating"`
	Genres      []string `json:"genres"`
	Description string   `json:"description"`
}

type UserEvent struct {
	UserId    int    `json:"user_id"`
	Username  string `json:"username"`
	Email     string `json:"email"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentId  int     `json:"payment_id"`
	UserId     int     `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType string  `json:"method_type"`
}

type Event struct {
	Id        string `json:"id"`
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
	Payload   any    `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     *Event `json:"event"`
}

type Session struct {
	ready chan bool
}

func (self *Session) Setup(sarama.ConsumerGroupSession) error {
	close(self.ready)
	return nil
}

func (self *Session) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (self *Session) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			log.Printf("event: %s; ts = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

var (
	producer sarama.SyncProducer
	consumer sarama.ConsumerGroup
	running  bool
)

const (
	MOVIE_EVENTS_TOPIC   = "movie-events"
	USER_EVENTS_TOPIC    = "user-events"
	PAYMENT_EVENTS_TOPIC = "payment-events"
)

var TOPICS = []string{
	MOVIE_EVENTS_TOPIC,
	USER_EVENTS_TOPIC,
	PAYMENT_EVENTS_TOPIC}

func main() {
	running = true

	// Set up HTTP routes
	http.HandleFunc("/api/events/movie", handleMovieEvent)
	http.HandleFunc("/api/events/user", handleUserEvent)
	http.HandleFunc("/api/events/payment", handlePaymentEvent)
	http.HandleFunc("/api/events/health", handleHealth)

	createProducer()
	createConsumer()
	defer func() {
		if producer != nil {
			if err := producer.Close(); err != nil {
				log.Fatalf("cannot close producer")
			}
		}
		if consumer != nil {
			if err := consumer.Close(); err != nil {
				log.Fatalf("cannot close consumer")
			}
		}
	}()

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081" // Note: Using a different port than the monolith
	}
	log.Printf("start events service listening %s port", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func createProducer() {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	log.Printf("brokers: %s", brokers)

	attempts := 15

	for attempts > 0 {
		var err error
		producer, err = sarama.NewSyncProducer(brokers, nil)
		if err != nil {
			log.Printf("cannot connect producer to Kafka: %s", err.Error())
			attempts--
			if attempts == 0 {
				log.Fatalf("cannot connect producer to Kafka: %s; exit", err.Error())
			}
			time.Sleep(5 * time.Second)
		} else {
			log.Printf("producer connected")
			break
		}
	}
}

func createConsumer() {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	config := sarama.NewConfig()
	config.Version = sarama.DefaultVersion
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRange()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	attempts := 15

	for attempts > 0 {
		var err error
		consumer, err = sarama.NewConsumerGroup(brokers, "events-consumer", config)
		if err != nil {
			log.Printf("cannot connect consumer to Kafka: %s", err.Error())
			attempts--
			if attempts == 0 {
				log.Fatalf("cannot connect consumer to Kafka: %s; exit", err.Error())
			}
			time.Sleep(5 * time.Second)
		} else {
			log.Printf("consumer connected")
			break
		}
	}

	session := Session{ready: make(chan bool)}
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			if err := consumer.Consume(ctx, TOPICS, &session); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			session.ready = make(chan bool)
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-session.ready
		log.Println("consumer is active")
		for running {
			select {
			case <-ctx.Done():
				log.Println("terminating: context cancelled")
				running = false
			case <-sigterm:
				log.Println("terminating: via signal")
				running = false
			}
		}

		cancel()
		wg.Wait()
		if err := consumer.Close(); err != nil {
			log.Panicf("Error closing client: %v", err)
		}
	}()
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		{
			var event MovieEvent
			if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
				replyError(w, err, http.StatusBadRequest)
				return
			}
			validate := validator.New()
			if err := validate.Struct(&event); err != nil {
				replyError(w, err, http.StatusBadRequest)
				return
			}

			pushAndReply(w, MOVIE_EVENTS_TOPIC, &Event{
				Id:      uuid.New().String(),
				Type:    "movie",
				Payload: event,
			})
		}
	default:
		replyError(w, errors.New("Method not allowed"), http.StatusMethodNotAllowed)
	}
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		{
			var event UserEvent
			if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
				replyError(w, err, http.StatusBadRequest)
				return
			}
			validate := validator.New()
			if err := validate.Struct(&event); err != nil {
				replyError(w, err, http.StatusBadRequest)
				return
			}

			pushAndReply(w, USER_EVENTS_TOPIC, &Event{
				Id:      uuid.New().String(),
				Type:    "user",
				Payload: event,
			})
		}
	default:
		replyError(w, errors.New("Method not allowed"), http.StatusMethodNotAllowed)
	}
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		{
			var event PaymentEvent
			if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
				replyError(w, err, http.StatusBadRequest)
				return
			}
			validate := validator.New()
			if err := validate.Struct(&event); err != nil {
				replyError(w, err, http.StatusBadRequest)
				return
			}

			pushAndReply(w, PAYMENT_EVENTS_TOPIC, &Event{
				Id:        uuid.New().String(),
				Type:      "payment",
				Payload:   event,
				Timestamp: time.Now().Format(time.RFC3339),
			})
		}
	default:
		replyError(w, errors.New("Method not allowed"), http.StatusMethodNotAllowed)
	}
}

func replyError(w http.ResponseWriter, err error, status int) {
	var sb strings.Builder
	json.NewEncoder(&sb).Encode(map[string]string{"error": err.Error()})
	w.Header().Set("Content-Type", "application/json")
	http.Error(w, sb.String(), status)
}

func pushAndReply(w http.ResponseWriter, topic string, event *Event) {
	bytes, err := json.Marshal(event)
	if err != nil {
		log.Printf("error: cannot send event to bus: err = %s", err.Error())
		replyError(w, err, http.StatusInternalServerError)
		return
	}

	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(bytes),
	})
	if err != nil {
		log.Printf("error: cannot send event to bus: err = %s", err.Error())
		replyError(w, err, http.StatusInternalServerError)
		return
	}

	bytes, err = json.Marshal(&EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	})
	if err != nil {
		log.Printf("error: cannot serialize event: err = %s", err.Error())
		replyError(w, err, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	w.Write(bytes)
}
