package broker

import (
	"fmt"
	"github.com/jarcoal/httpmock"
	"log"
	"net/http"
	"testing"
	"time"
)

func TestPubSub1Topic(t *testing.T) {

	nOfTestMessages := 10

	//mock http
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	TopicMessages := 0
	finishChan := make(chan bool, 1)

	httpmock.RegisterResponder("POST", "http://0.0.0.0:2030/msg",
		func(req *http.Request) (*http.Response, error) {
			resp, err := httpmock.NewJsonResponse(200, map[string]interface{}{
				"value": "ok",
			})
			TopicMessages += 1
			log.Default().Printf("Received message n.%d from topic\n", TopicMessages)
			if TopicMessages == nOfTestMessages {
				finishChan <- true
			}
			return resp, err
		},
	)

	//Get a broker with 10 workers and subscribe to /test
	mybroker := GetPubSubBroker(WithCustomWorkersNumber(1))
	err := mybroker.Subscribe(Subscriber{
		Address: "0.0.0.0",
		Port:    "2030",
		Topic:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	//Pub 10 messages
	for i := 0; i < nOfTestMessages; i++ {
		err = mybroker.Publish(Message{
			Qos:       0,
			Message:   "test",
			Topic:     "test",
			heartbeat: false,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	//check if subscriber got 10 messages
	select {
	case <-finishChan:
		return
	case <-time.NewTimer(3 * time.Second).C:
		t.Fatal("Timeout!")
	}
}

func TestPubSub10Topic(t *testing.T) {

	nOfTestMessages := 10
	nofTopics := 10

	//mock http
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	finishChan := make(chan bool, 1)

	for i := 0; i < nofTopics; i++ {
		TopicMessages := 0
		httpmock.RegisterResponder("POST", fmt.Sprintf("http://0.0.0.%d:2030/msg", i),
			func(req *http.Request) (*http.Response, error) {
				resp, err := httpmock.NewJsonResponse(200, map[string]interface{}{
					"value": "ok",
				})
				TopicMessages += 1
				log.Default().Printf("Received message n.%d from topic %d\n", TopicMessages, i)
				if TopicMessages == nOfTestMessages {
					finishChan <- true
				}
				return resp, err
			},
		)
	}

	//Get a broker with 10 workers and subscribe to /test
	mybroker := GetPubSubBroker(WithCustomWorkersNumber(1))
	for i := 0; i < nofTopics; i++ {
		err := mybroker.Subscribe(Subscriber{
			Address: fmt.Sprintf("0.0.0.%d", i),
			Port:    "2030",
			Topic:   fmt.Sprintf("test%d", i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Second)

	//Pub 10 messages for each topic
	for j := 0; j < nofTopics; j++ {
		for i := 0; i < nOfTestMessages; i++ {
			err := mybroker.Publish(Message{
				Qos:       0,
				Message:   "test",
				Topic:     fmt.Sprintf("test%d", j),
				heartbeat: false,
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	//check if all 10 subscribers got 10 messages
	for i := 0; i < nofTopics; i++ {
		select {
		case <-finishChan:
			log.Default().Printf("Topic %d COMPLETED\n", i)
		case <-time.NewTimer(3 * time.Second).C:
			t.Fatal("Timeout!")
		}
	}
}

func TestPubSub1Topic10Subscribers(t *testing.T) {

	nOfTestMessages := 10
	nofSubscribers := 10

	//mock http
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	finishChan := make(chan bool, 1)

	for i := 0; i < nofSubscribers; i++ {
		TopicMessages := 0
		httpmock.RegisterResponder("POST", fmt.Sprintf("http://0.0.0.%d:2030/msg", i),
			func(req *http.Request) (*http.Response, error) {
				resp, err := httpmock.NewJsonResponse(200, map[string]interface{}{
					"value": "ok",
				})
				TopicMessages += 1
				log.Default().Printf("Received message n.%d from topic %d\n", TopicMessages, i)
				if TopicMessages == nOfTestMessages {
					finishChan <- true
				}
				return resp, err
			},
		)
	}

	//Get a broker with 10 workers and subscribe to /test
	mybroker := GetPubSubBroker(WithCustomWorkersNumber(1))
	for i := 0; i < nofSubscribers; i++ {
		err := mybroker.Subscribe(Subscriber{
			Address: fmt.Sprintf("0.0.0.%d", i),
			Port:    "2030",
			Topic:   "test",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Second)

	//Pub 10 messages for topic test
	for i := 0; i < nOfTestMessages; i++ {
		err := mybroker.Publish(Message{
			Qos:       0,
			Message:   "test",
			Topic:     "test",
			heartbeat: false,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	//check if all 10 subscribers got 10 messages
	for i := 0; i < nofSubscribers; i++ {
		select {
		case <-finishChan:
			log.Default().Printf("Topic %d COMPLETED\n", i)
		case <-time.NewTimer(3 * time.Second).C:
			t.Fatal("Timeout!")
		}
	}
}

func TestHeartbeat(t *testing.T) {

	nOfTestHb := 3

	//mock http
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	finishChan := make(chan bool, 1)
	receivedHb := 0

	httpmock.RegisterResponder("POST", "http://0.0.0.0:2030/msg",
		func(req *http.Request) (*http.Response, error) {
			resp, err := httpmock.NewJsonResponse(200, map[string]interface{}{
				"value": "ok",
			})
			log.Default().Printf("Received message from topic\n")

			finishChan <- true
			return resp, err
		},
	)

	httpmock.RegisterResponder("POST", "http://0.0.0.0:2030/hb",
		func(req *http.Request) (*http.Response, error) {
			resp, err := httpmock.NewJsonResponse(200, map[string]interface{}{
				"value": "ok",
			})
			receivedHb += 1
			log.Default().Printf("Received HB response message n.%d from broker\n", receivedHb)
			return resp, err
		},
	)

	//Get a broker with 10 workers and subscribe to /test
	mybroker := GetPubSubBroker(WithCustomWorkersNumber(1))
	err := mybroker.Subscribe(Subscriber{
		Address: "0.0.0.0",
		Port:    "2030",
		Topic:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	//send heartbeats
	for i := 0; i < nOfTestHb; i++ {
		err := mybroker.Heartbeat("0.0.0.0")
		if err != nil {
			t.Fatal(err)
		}
		log.Default().Printf("HB n.%d sent!\n", i)
		time.Sleep(time.Second * 4)
	}

	//check if pub message can be received
	err = mybroker.Publish(Message{
		Qos:       0,
		Message:   "test",
		Topic:     "test",
		heartbeat: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-finishChan:
		log.Default().Printf("Received pub message!")
	case <-time.NewTimer(3 * time.Second).C:
		t.Fatal("Timeout!")
	}

	//Wait until HB expires
	log.Default().Printf("Not sending HB messages for 11 seconds!")
	time.Sleep(time.Second * 11)

	//publishing new message. It should fail!
	err = mybroker.Publish(Message{
		Qos:       0,
		Message:   "test",
		Topic:     "test",
		heartbeat: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-finishChan:
		t.Fatal("Message received but subscribed should have been disconnected")
	case <-time.NewTimer(3 * time.Second).C:
		log.Default().Printf("No message received! Good!")
	}

	//Re-subscribing
	log.Default().Printf("Re subscribing to topic")
	err = mybroker.Subscribe(Subscriber{
		Address: "0.0.0.0",
		Port:    "2030",
		Topic:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	//check if pub message can be received
	log.Default().Printf("Sending new pub message")
	err = mybroker.Publish(Message{
		Qos:       0,
		Message:   "test",
		Topic:     "test",
		heartbeat: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-finishChan:
		log.Default().Printf("Received pub message!")
	case <-time.NewTimer(3 * time.Second).C:
		t.Fatal("Timeout!")
	}

}
