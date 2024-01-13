package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/giobart/message-broker/pkg/broker"
	"net/http"
	"runtime"
	"strings"
)

type PubMessage struct {
	Data string `json:"data"`
	QoS  int    `json:"qos"`
}

type SubMessage struct {
	QoS  int    `json:"qos"`
	Port string `json:"port"`
}

var brokerServer broker.PubSubBroker

func pub(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

	topic := r.URL.Path[len("/pub/"):]

	var message PubMessage
	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		fmt.Printf("ERROR Unable to decode %s", r.URL.String())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	//log.Default().Printf("Publishing message to topic '%s': %s\n", topic, message.Data)

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
	port := flag.String("p", "9999", "Listen port, default value 9999.")
	workers := flag.Int("w", 1, "Number of parallel works in work pool, default 1.")
	flag.Parse()
	runtime.GOMAXPROCS(1)

	brokerServer = broker.GetPubSubBroker(broker.WithCustomWorkersNumber(*workers))

	http.HandleFunc("/pub/", pub)
	http.HandleFunc("/sub/", sub)
	http.HandleFunc("/hb", heartbeat)

	fmt.Printf("Listening on port %d\n", port)
	err := http.ListenAndServe(fmt.Sprintf(":%s", *port), nil)
	if err != nil {
		fmt.Printf("Error %v", err)
		return
	}
}
