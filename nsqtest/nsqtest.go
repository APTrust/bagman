package main

import (
	"fmt"
	"log"
	"github.com/bitly/go-nsq"
)

type PrintHandler struct {
	counter   uint64
}

func (ph *PrintHandler) HandleMessage(m *nsq.Message, outputChannel chan *nsq.FinishedMessage) {
	fmt.Println(string(m.Body))
	ph.counter++
	success := true

	// Throw in an occasional failure for re-queueing.
	if ph.counter % 4 == 0 {
		success = false
	}
	finishedMessage := &nsq.FinishedMessage{m.Id, 1000, success}
	outputChannel <- finishedMessage
}

// Simple queue test program. See nsq/README.md for info on setting up queue.
// Then publish to the queue with this:
// curl -d 'Sample #1' http://127.0.0.1:4151/put?topic=sample_topic
// View output on stdout
// View stats on http://localhost:4171/
// TODO: Handle interrupt signals for graceful exit.
// Read up on queue timeout and backoff.
func main() {
	reader, err := nsq.NewReader("sample_topic", "sample_channel")
	if err != nil {
		log.Fatalf(err.Error())
	}

	handler := &PrintHandler{}
	reader.SetMaxInFlight(100)
	reader.AddAsyncHandler(handler)
	reader.AddAsyncHandler(handler)
	reader.AddAsyncHandler(handler)
	reader.ConnectToLookupd("127.0.0.1:4161")

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-reader.ExitChan
}
