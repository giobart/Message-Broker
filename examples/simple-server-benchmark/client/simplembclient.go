package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type MessageBrokerClientOpt func(m *simpleclient)
type MessageBrokerClientMessageCallback func(data string, topic string)

type MessgageFromBroker struct {
	Data  string `json:"data"`
	Topic string `json:"Topic"`
}

type SubMessage struct {
	QoS  int    `json:"qos"`
	Port string `json:"port"`
}

type PubMessage struct {
	Data string `json:"data"`
	QoS  int    `json:"qos"`
}

type MessageBrokerClient interface {
	Publish(data string, topic string) error
	Subscribe(topic string, callback MessageBrokerClientMessageCallback) error
}

type simpleclient struct {
	topics        map[string]MessageBrokerClientMessageCallback
	port          int
	errChan       chan error
	heartbeatChan chan bool
	initialized   int
	brokerUrl     string
}

// GetSimpleMessageBrokerClient Creates a broker client. It will connect to brokerUrl with the first publish/subscribe
// It will listen by default on port 10000 for incoming sub messages
func GetSimpleMessageBrokerClient(brokerUrl string, opt ...MessageBrokerClientOpt) MessageBrokerClient {
	brokerclient := simpleclient{
		topics:        make(map[string]MessageBrokerClientMessageCallback),
		port:          10000,
		errChan:       make(chan error),
		heartbeatChan: make(chan bool),
		brokerUrl:     brokerUrl,
	}

	for _, op := range opt {
		op(&brokerclient)
	}

	http.HandleFunc("/msg", brokerclient.messagehandler)
	http.HandleFunc("/hb", brokerclient.hbhandler)
	log.Default().Printf("Listening on port %d", brokerclient.port)
	go func() {
		err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", brokerclient.port), nil)
		if err != nil {
			log.Fatal(err)
		}
	}()
	return &brokerclient
}

func (c *simpleclient) Publish(data string, topic string) error {
	url := fmt.Sprintf("http://%s/pub/%s", c.brokerUrl, topic)
	jsonData, err := json.Marshal(PubMessage{
		QoS:  0,
		Data: data,
	})
	if err != nil {
		log.Default().Printf("ERROR unable to encode sub request for topic %s", topic)
		return err
	}
	return doPost(url, bytes.NewBuffer(jsonData))
}

func (c *simpleclient) Subscribe(topic string, callback MessageBrokerClientMessageCallback) error {
	err := c.sendSubRequest(topic)
	if err != nil {
		return err
	}
	c.topics[topic] = callback
	return nil
}

func (c *simpleclient) reConnect() {
	//wait for re-connection timeout
	time.Sleep(time.Second)

	//try to re-subscribe to topics
	for topic, _ := range c.topics {
		err := c.sendSubRequest(topic)
		if err != nil {
			c.errChan <- err
			return
		}
	}

	log.Default().Printf("Re-connected to topics")
}

func (c *simpleclient) sendSubRequest(topic string) error {
	url := fmt.Sprintf("http://%s/sub/%s", c.brokerUrl, topic)
	jsonData, err := json.Marshal(SubMessage{
		QoS:  0,
		Port: fmt.Sprintf("%d", c.port),
	})
	if err != nil {
		log.Default().Printf("ERROR unable to encode sub request for topic %s", topic)
		return err
	}
	return doPost(url, bytes.NewBuffer(jsonData))
}

func (c *simpleclient) messagehandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	var message MessgageFromBroker
	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		log.Default().Printf("ERROR Unable to decode %s", r.URL.String())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	//execute topic callback if registered topic
	callback := c.topics[message.Topic]
	if callback != nil {
		go callback(message.Data, message.Topic)
	}

	w.WriteHeader(http.StatusAccepted)
}

func (c *simpleclient) hbhandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	w.WriteHeader(http.StatusAccepted)
	c.heartbeatChan <- true
}

func doPost(url string, body io.Reader) error {
	client := http.Client{
		Timeout: 5 * time.Second,
	}
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusAccepted {
		return errors.New(fmt.Sprintf("Request status code %d", resp.StatusCode))
	}

	err = resp.Body.Close()
	if err != nil {
		return err
	}

	return nil
}

// WithCustomListenPort Changes default listen port for broker client
func WithCustomListenPort(port int) MessageBrokerClientOpt {
	return func(m *simpleclient) {
		m.port = port
	}
}
