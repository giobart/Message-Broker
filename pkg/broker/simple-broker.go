package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type simpleMessageBroker struct {
	subscribedToTopic map[string]*[]string
}

func GetSimplePubSubBroker() PubSubBroker {
	responseBroker := &simpleMessageBroker{
		subscribedToTopic: make(map[string]*[]string),
	}
	return responseBroker
}

func (b *simpleMessageBroker) Publish(msg Message) error {
	//get subscribers list
	subscribed := []string{}
	if b.subscribedToTopic[msg.Topic] != nil {
		subscribed = *b.subscribedToTopic[msg.Topic]
	}

	//sent notification to subscribers workers
	//log.Default().Printf("Worker_%d: Sending to %d subscribers", workerId, len(subscribed))
	for _, subscriber := range subscribed {
		url := fmt.Sprintf("http://%s/msg", subscriber)
		jsonData, err := json.Marshal(MessgageToClient{
			Data:  msg.Message,
			Topic: msg.Topic,
		})
		if err != nil {
			fmt.Printf("ERROR unable to encode Message %v \n", msg)
		}
		err = doPost(url, bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Printf("ERROR unable to send post data to %s... unsubscribing it \n", fmt.Sprintf("http://%s/msg", subscriber))
		}
	}

	return nil
}

func (b *simpleMessageBroker) Subscribe(sub Subscriber) error {
	subAddr := fmt.Sprintf("%s:%s", sub.Address, sub.Port)
	if b.subscribedToTopic[sub.Topic] == nil {
		subscribers := []string{subAddr}
		b.subscribedToTopic[sub.Topic] = &subscribers
	} else {
		subscribers := append(*b.subscribedToTopic[sub.Topic], subAddr)
		b.subscribedToTopic[sub.Topic] = &subscribers
	}
	return nil
}

func (b *simpleMessageBroker) Heartbeat(address string) error {
	return nil
}

func (b *simpleMessageBroker) Stop() {
	return
}
