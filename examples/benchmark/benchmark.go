package main

import (
	"flag"
	"fmt"
	mbclient "github.com/giobart/message-broker/pkg/client"
	"log"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

func main() {

	serverAddress := flag.String("a", "0.0.0.0:9999", "Broker address")
	pubMessages := flag.Int("m", 10000, "Number of benchmark messages to be sent.")
	cooldown := flag.Int("c", 1, "Cooldown expressed in nanoseconds between two consectuive messages. Default 0.")
	maxExpTime := flag.Int("d", 10, "Max experiment time expressed in seconds, default 10s")
	flag.Parse()

	receivede2e := make([]int, *pubMessages)

	rand := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	benchmarkId := rand.Int()
	topic := fmt.Sprintf("benchmark/id/%d", benchmarkId)

	//Connect to a broker
	client := mbclient.GetMessageBrokerClient(*serverAddress, mbclient.WithCustomListenPort(10000))

	endTime := time.Now().Nanosecond()
	allReceived := make(chan bool)
	//Subscribe to topic with a callback
	err := client.Subscribe(topic, func(data string, topic string) {
		dataArray := strings.Split(data, ",")
		timesent, _ := strconv.Atoi(dataArray[0])
		seqnum, _ := strconv.Atoi(dataArray[1])
		currTime := time.Now().Nanosecond()
		receivede2e[seqnum] = currTime - timesent
		if currTime > endTime {
			endTime = currTime
		}
		if seqnum >= len(receivede2e)-1 {
			allReceived <- true
		}
		//log.Default().Printf("E2E: ", data)
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Default().Printf("Sending %d messages\n", *pubMessages)
	time.Sleep(time.Second * 1)
	// Start time
	startTime := time.Now().Nanosecond()
	//Publish to topic with a callback
	for i := 0; i < (*pubMessages); i++ {
		err = client.Publish(fmt.Sprintf("%d,%d", time.Now().Nanosecond(), i), topic)
		if err != nil {
			log.Default().Print(err)
		}
		time.Sleep(time.Duration((*cooldown)))
	}

	//Cooldown
	log.Default().Printf("Publish finished, waiting for cooldown\n")
	select {
	case <-time.After(time.Duration(int(time.Second) * (*maxExpTime))):
		log.Default().Printf("Cooldown timer finished\n")
	case <-allReceived:
		log.Default().Printf("All messages received!\n")
	}

	//Calculate average delay and drop rate
	avgE2E := 0
	received := 0
	for _, e2e := range receivede2e {
		if e2e > 0 {
			received += 1
			avgE2E += e2e
		}
	}

	totTime := endTime - startTime

	log.Default().Printf("\nTOT_SENT:%d, TOT_RECEIVED:%d, AVG_E2E:%dns, MESSAGES_X_NSEC:%f message/ms, TOT_EXP_TIME:%fms\n", *pubMessages, received, avgE2E/received, 1/(float64(totTime)/(math.Pow(10, 6))/float64(received)), float64(totTime)/(math.Pow(10, 6)))
}
