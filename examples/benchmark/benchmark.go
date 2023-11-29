package main

import (
	"fmt"
	mbclient "github.com/giobart/message-broker/pkg/client"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const (
	NUMBER_OF_PUB_MESSAGES     = 10000
	WAIT_TIME_BETWEEN_MESSAGES = 0 * time.Millisecond
)

func main() {

	receivede2e := make([]int, NUMBER_OF_PUB_MESSAGES)

	rand := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	benchmarkId := rand.Int()
	topic := fmt.Sprintf("benchmark/id/%d", benchmarkId)

	//Connect to a broker
	client := mbclient.GetMessageBrokerClient("0.0.0.0:9999", mbclient.WithCustomListenPort(10000))

	//Subscribe to topic with a callback
	err := client.Subscribe(topic, func(data string, topic string) {
		dataArray := strings.Split(data, ",")
		timesent, _ := strconv.Atoi(dataArray[0])
		seqnum, _ := strconv.Atoi(dataArray[1])
		receivede2e[seqnum] = time.Now().Nanosecond() - timesent
		//log.Default().Printf("E2E: ", data)
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Default().Printf("Sending %d messages\n", NUMBER_OF_PUB_MESSAGES)
	time.Sleep(time.Second * 1)
	//Publish to topic with a callback
	for i := 0; i < NUMBER_OF_PUB_MESSAGES; i++ {
		err = client.Publish(fmt.Sprintf("%d,%d", time.Now().Nanosecond(), i), topic)
		if err != nil {
			log.Default().Print(err)
		}
		time.Sleep(WAIT_TIME_BETWEEN_MESSAGES)
	}

	//Cooldown
	log.Default().Printf("Publish finished, waiting for cooldown\n")
	time.Sleep(time.Second * 5)

	//Calculate average delay and drop rate
	avgE2E := 0
	received := 0
	for _, e2e := range receivede2e {
		if e2e > 0 {
			received += 1
			avgE2E += e2e
		}
	}

	log.Default().Printf("TOT_SENT:%d, TOT_RECEIVED:%d, AVG_E2E:%dns\n", NUMBER_OF_PUB_MESSAGES, received, avgE2E/received)
}
