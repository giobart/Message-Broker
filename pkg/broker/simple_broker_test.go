package broker

import (
	"fmt"
	"github.com/jarcoal/httpmock"
	"log"
	"net/http"
	"testing"
	"time"
)

func TestSimplePubSub1Topic(t *testing.T) {

	nOfTestMessages := 10

	//mock http
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	TopicMessages := 0
	finishChan := make(chan bool, 1)

	httpmock.RegisterResponder("POST", "http://0.0.0.0:2030/msg",
		func(req *http.Request) (*http.Response, error) {
			resp, err := httpmock.NewJsonResponse(http.StatusAccepted, map[string]interface{}{
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

	//Get a messageBroker with 10 workers and subscribe to /test
	mybroker := GetSimplePubSubBroker()
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

func TestSimplePubSub10Topic(t *testing.T) {

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
				resp, err := httpmock.NewJsonResponse(http.StatusAccepted, map[string]interface{}{
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

	//Get a messageBroker with 10 workers and subscribe to /test
	mybroker := GetSimplePubSubBroker()
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
