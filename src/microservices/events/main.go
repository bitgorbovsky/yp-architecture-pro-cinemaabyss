package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"

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

var producer sarama.SyncProducer

const MOVIE_EVENTS_TOPIC = "movie-events"
const USER_EVENTS_TOPIC = "user-events"
const PAYMENT_EVENTS_TOPIC = "payment-events"

func main() {
	// Set up HTTP routes
	http.HandleFunc("/api/events/movie", handleMovieEvent)
	http.HandleFunc("/api/events/user", handleUserEvent)
	http.HandleFunc("/api/events/payment", handlePaymentEvent)
	http.HandleFunc("/api/events/health", handleHealth)

	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	log.Printf("brokers: %s", brokers)

	var err error
	producer, err = sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		log.Fatalf("cannot connect to Kafka: %s", err.Error())
		os.Exit(1)
		return
	}
	defer func() {
		if producer != nil {
			producer.Close()
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
				Id:      uuid.New().String(),
				Type:    "payment",
				Payload: event,
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
