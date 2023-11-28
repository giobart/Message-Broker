package main

import (
	mbclient "github.com/giobart/Message-Broker/pkg/client"
	"log"
	"time"
)

const (
	NUMBER_OF_PUB_MESSAGES     = 10
	WAIT_TIME_BETWEEN_MESSAGES = 10 * time.Second
)

func main() {

	//Connect to a broker
	client := mbclient.GetMessageBrokerClient("0.0.0.0:9999", mbclient.WithCustomListenPort(10000))

	//Subscribe to topic with a callback
	err := client.Subscribe("hello/world", func(data string, topic string) {
		log.Default().Printf("RECEIVED: %s", data)
	})
	if err != nil {
		log.Fatal(err)
	}

	//Publish to topic with a callback
	for i := 0; i < NUMBER_OF_PUB_MESSAGES; i++ {
		err = client.Publish("hello", "hello/world")
		if err != nil {
			log.Default().Print(err)
		}
		time.Sleep(WAIT_TIME_BETWEEN_MESSAGES)
	}

}
