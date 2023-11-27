package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/giobart/Message-Broker/broker"
	"log"
	"net/http"
)

type PubMessage struct {
	Data string `json:"data"`
	QoS  int    `json:"qos"`
}

type SubMessage struct {
	QoS  int    `json:"qos"`
	Port string `json:"port"`
}

var brokerServer broker.PubSubBroker = broker.GetPubSubBroker()

func pub(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

	topic := r.URL.Path[len("/pub/"):]

	var message PubMessage
	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		log.Default().Printf("ERROR Unable to decode %s", r.URL.String())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	log.Default().Printf("Publishing message to topic '%s': %s\n", topic, message.Data)

	err = brokerServer.Publish(broker.Message{
		Qos:     message.QoS,
		Message: message.Data,
		Topic:   topic,
	})
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func sub(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

	topic := r.URL.Path[len("/sub/"):]
	client := r.RemoteAddr

	var message SubMessage
	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		log.Default().Printf("ERROR Unable to decode %s", r.URL.String())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	log.Default().Printf("Client %s subscribed to %s\n", client, topic)

	err = brokerServer.Subscribe(broker.Subscriber{
		Address: client,
		Topic:   topic,
		Port:    message.Port,
	})
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func heartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

	err := brokerServer.Heartbeat(r.RemoteAddr)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func main() {
	port := flag.String("p", "8020", "Listen port, default value 8020.")

	http.HandleFunc("/pub/", pub)
	http.HandleFunc("/sub/", sub)
	http.HandleFunc("/heartbeat", heartbeat)

	log.Default().Printf("Listening on port %s", *port)
	err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%s", *port), nil)
	if err != nil {
		return
	}
}
