package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/giobart/message-broker/pkg/broker"
)

type PubMessage struct {
	Data string `json:"data"`
	QoS  int    `json:"qos"`
}

type SubMessage struct {
	QoS  int    `json:"qos"`
	Port string `json:"port"`
}

var message PubMessage

var brokerServer broker.PubSubBroker

var brokermsg = broker.Message{
	Qos:     0,
	Message: "",
	Topic:   "",
}

func pub(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

	topic := r.URL.Path[len("/pub/"):]

	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		fmt.Printf("ERROR Unable to decode %s", r.URL.String())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	//log.Default().Printf("Publishing message to topic '%s': %s\n", topic, message.Data)
	brokermsg.Message = message.Data
	brokermsg.Topic = topic

	err = brokerServer.Publish(&brokermsg)
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
	strings.Split(r.RemoteAddr, ":")
	client := strings.Split(r.RemoteAddr, ":")[0]

	var message SubMessage
	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		fmt.Printf("ERROR Unable to decode %s", r.URL.String())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	//log.Default().Printf("Client %s subscribed to %s\n", client, topic)

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

	err := brokerServer.Heartbeat(strings.Split(r.RemoteAddr, ":")[0])
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func main() {
	port := 9999

	brokerServer = broker.GetSimplePubSubBroker()

	http.HandleFunc("/pub/", pub)
	http.HandleFunc("/sub/", sub)
	http.HandleFunc("/hb", heartbeat)

	fmt.Printf("Listening on port %d\n", port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		fmt.Printf("Error %v", err)
		return
	}
}
