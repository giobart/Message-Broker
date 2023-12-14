package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type Message struct {
	Qos       int
	Message   string
	Topic     string
	heartbeat bool
}

type MessgageToClient struct {
	Data  string `json:"data"`
	Topic string `json:"Topic"`
}

type topic struct {
	topic string
}

type Subscriber struct {
	Address string
	Port    string
	Topic   string
}

type messageBroker struct {
	pubChannel              chan Message
	subChannel              chan Subscriber
	heartbeatChannel        chan string
	subscribers             map[string]*chan Message
	subscribedToTopic       map[string]*map[string]*string
	killChan                chan bool
	workers                 int
	subscribedToTopicRwLock sync.RWMutex
	subscriberRwLock        sync.RWMutex
}

type PubSubBroker interface {
	Publish(msg Message) error
	Subscribe(sub Subscriber) error
	Heartbeat(address string) error
	Stop()
}

// BrokerOptions Functional Borker Options
type BrokerOptions func(*messageBroker)

// WithCustomWorkersNumber Select the number of background workers to handle messageBroker requests
func WithCustomWorkersNumber(number int) BrokerOptions {
	return func(b *messageBroker) {
		b.workers = number
	}
}

func GetPubSubBroker(opts ...BrokerOptions) PubSubBroker {
	responseBroker := &messageBroker{
		pubChannel:              make(chan Message, 1000),
		subChannel:              make(chan Subscriber, 1000),
		subscribers:             make(map[string]*chan Message),
		subscribedToTopic:       make(map[string]*map[string]*string),
		heartbeatChannel:        make(chan string, 100),
		killChan:                make(chan bool, 100),
		workers:                 1,
		subscribedToTopicRwLock: sync.RWMutex{},
		subscriberRwLock:        sync.RWMutex{},
	}
	//add functional arguments to the messageBroker

	for _, opt := range opts {
		opt(responseBroker)
	}
	//start workers
	for i := 0; i < responseBroker.workers; i++ {
		go responseBroker.worker(i)
	}
	return responseBroker
}

func (b *messageBroker) Publish(msg Message) error {
	if len(b.pubChannel) < cap(b.pubChannel) {
		b.pubChannel <- msg
	} else {
		return errors.New("publish channel at capacity at the moment")
	}
	return nil
}

func (b *messageBroker) Subscribe(sub Subscriber) error {
	if len(b.subChannel) < cap(b.subChannel) {
		b.subChannel <- sub
	} else {
		return errors.New("subscribe channel at capacity at the moment")
	}
	return nil
}

func (b *messageBroker) Heartbeat(address string) error {
	if len(b.heartbeatChannel) < cap(b.heartbeatChannel) {
		b.heartbeatChannel <- address
	} else {
		return errors.New("subscribe channel at capacity at the moment")
	}
	return nil
}

func (b *messageBroker) Stop() {
	//kill all the workers
	for i := 0; i < b.workers; i++ {
		b.killChan <- true
	}
}
func (b *messageBroker) worker(workerId int) {
	for {
		select {
		case <-b.killChan:
			log.Default().Printf("Killing worker %d\n", workerId)
			return
		case pubMsg := <-b.pubChannel:
			//log.Default().Printf("Worker_%d: Received Pub Message for Topic %s\n", workerId, pubMsg.Topic)

			//get subscribers list
			b.subscribedToTopicRwLock.RLock()
			subscribed := make(map[string]*string)
			if b.subscribedToTopic[pubMsg.Topic] != nil {
				subscribed = *b.subscribedToTopic[pubMsg.Topic]
			}
			b.subscribedToTopicRwLock.RUnlock()

			//sent notification to subscribers workers
			b.subscriberRwLock.RLock()
			//log.Default().Printf("Worker_%d: Sending to %d subscribers", workerId, len(subscribed))
			for subscriber, _ := range subscribed {
				msgChan := b.subscribers[subscriber]
				if msgChan != nil {
					if len(*msgChan) < cap(*msgChan) {
						*msgChan <- pubMsg
					} else {
						log.Default().Printf("Worker_%d: ERROR: Full worker %s channel \n", workerId, subscriber)
					}
				} else {
					log.Default().Printf("Worker_%d: ERROR: No Subscribers for %s channel \n", workerId, subscriber)
					// if the subscriber channel does not exist anymore, remove the subscriber from the map
					b.subscribedToTopicRwLock.Lock()
					subscribedMap := *b.subscribedToTopic[pubMsg.Topic]
					delete(subscribedMap, subscriber)
					b.subscribedToTopic[pubMsg.Topic] = &subscribedMap
					b.subscribedToTopicRwLock.Unlock()
				}
			}
			b.subscriberRwLock.RUnlock()

		case subMsg := <-b.subChannel:
			//log.Default().Printf("Worker_%d: Received Sub Message for Topic %s\n", workerId, subMsg.Topic)

			// check if this is known otherwise create it
			b.subscriberRwLock.Lock()
			msgChan := b.subscribers[subMsg.Address]
			if msgChan == nil {
				newMsgChan := make(chan Message, 100)
				b.subscribers[subMsg.Address] = &newMsgChan
				//spawn internal client twin worker
				go b.clientTwinWorker(newMsgChan, subMsg.Address, subMsg.Port)
			}
			b.subscriberRwLock.Unlock()

			// add Topic subscription
			b.subscribedToTopicRwLock.Lock()
			subscribersToTopic := b.subscribedToTopic[subMsg.Topic]
			if subscribersToTopic == nil {
				newSubscribersToTopic := make(map[string]*string)
				b.subscribedToTopic[subMsg.Topic] = &newSubscribersToTopic
				subscribersToTopic = &newSubscribersToTopic
			}
			subscribersToTopicMap := *subscribersToTopic
			if subscribersToTopicMap[subMsg.Address] == nil {
				subscribersToTopicMap[subMsg.Address] = &subMsg.Address
				b.subscribedToTopic[subMsg.Topic] = &subscribersToTopicMap
			}
			b.subscribedToTopicRwLock.Unlock()

		case hartbeatMsg := <-b.heartbeatChannel:
			//log.Default().Printf("Worker_%d: Received Heartbeat Message from %s\n", workerId, hartbeatMsg)
			// send heartbeat to internal worker
			b.subscriberRwLock.RLock()
			msgChan := b.subscribers[hartbeatMsg]
			if msgChan != nil {
				*msgChan <- Message{heartbeat: true}
			}
			b.subscriberRwLock.RUnlock()
		}
	}
}

func (b *messageBroker) clientTwinWorker(msgChan <-chan Message, address string, port string) {
	defer func() {
		b.subscriberRwLock.Lock()
		b.subscribers[address] = nil
		b.subscriberRwLock.Unlock()
	}()
	for {
		select {
		case <-time.NewTimer(10 * time.Second).C:
			//timeout
			log.Default().Printf("Worker %s disconnected\n", address)
			return
		case msg := <-msgChan:

			//if heartbeat just send heartbeat response
			if msg.heartbeat {
				url := fmt.Sprintf("http://%s:%s/hb", address, port)
				err := doPost(url, nil)
				if err != nil {
					log.Default().Printf("ERROR unable to finalize heartbeat for %s \n", fmt.Sprintf("http://%s:%s/hb", address, port))
					return
				}
				continue
			}
			url := fmt.Sprintf("http://%s:%s/msg", address, port)
			jsonData, err := json.Marshal(MessgageToClient{
				Data:  msg.Message,
				Topic: msg.Topic,
			})
			if err != nil {
				log.Default().Printf("ERROR unable to encode Message %v \n", msg)
			}
			err = doPost(url, bytes.NewBuffer(jsonData))
			if err != nil {
				log.Default().Printf("ERROR unable to send post data to %s... unsubscribing it \n", fmt.Sprintf("http://%s:%s/msg", address, port))
				return
			}

			//if err != nil return
		}
	}
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
