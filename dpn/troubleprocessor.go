package dpn

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/nsqio/go-nsq"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

// TroubleProcessor dumps the ProcessResult structure of
// items that failed the ingest process into JSON files.
// The JSON is formatted and human-readable, and may be
// deserialized and loaded into other processes in the future.
// The ProcessResult structure contains fairly detailed
// information about every stage of the ingest process.
type TroubleProcessor struct {
	ProcUtil *bagman.ProcessUtil
	// WaitGroup is for running local tests only.
	WaitGroup           sync.WaitGroup
}

func NewTroubleProcessor(procUtil *bagman.ProcessUtil) (*TroubleProcessor) {
	return &TroubleProcessor{
		ProcUtil: procUtil,
	}
}

func (troubleProcessor *TroubleProcessor) HandleMessage(message *nsq.Message) error {
	result := &DPNResult{}
	err := json.Unmarshal(message.Body, &result)
	if err != nil {
		detailedError := fmt.Errorf(
			"Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		troubleProcessor.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	result.NsqMessage = message

	bagId := result.BagIdentifier
	if bagId == "" && result.DPNBag != nil {
		bagId = result.DPNBag.UUID
	}

	troubleProcessor.dumpToFile(result)
	if result.FluctusProcessStatus != nil {
		troubleProcessor.ProcUtil.MessageLog.Info(
			"Trying to flag ProcessedItem as failed for bag %s", bagId)
	}
	troubleProcessor.ProcUtil.MessageLog.Info("Processed DPN bag %s", bagId)
	return nil
}

func (troubleProcessor *TroubleProcessor) dumpToFile(result *DPNResult) error {
	outdir := path.Join(troubleProcessor.ProcUtil.Config.LogDirectory, "dpn_trouble")
	if _, err := os.Stat(outdir); os.IsNotExist(err) {
		err := os.Mkdir(outdir, 0766)
		if err != nil {
			panic(err)
		}
	}
	json, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	bagUUID := ""
	if result.DPNBag != nil {
		bagUUID = result.DPNBag.UUID
	} else {
		bagUUID = result.PackageResult.BagBuilder.UUID
	}
	filePath := path.Join(outdir, bagUUID)
	os.MkdirAll(filepath.Dir(filePath), 0755)
	err = ioutil.WriteFile(filePath, json, 0644)
	if err != nil {
		panic(err)
	}
	if result.NsqMessage != nil {
		result.NsqMessage.Finish()
	} else {
		troubleProcessor.WaitGroup.Done()
	}
	return nil
}

func (troubleProcessor *TroubleProcessor) updateProcessedItem(result *DPNResult) {
	if result.FluctusProcessStatus == nil {
		return
	}
	processedItem := result.FluctusProcessStatus
	processedItem.Date = time.Now()
	processedItem.Status = "Failed"
	processedItem.Note = result.ErrorMessage
	err := troubleProcessor.ProcUtil.FluctusClient.UpdateProcessedItem(processedItem)
	if err != nil {
		troubleProcessor.ProcUtil.MessageLog.Error(
			"Error updating ProcessedItem status in Fluctus: %v", err)
	}
}


func (troubleProcessor *TroubleProcessor) RunTest(result *DPNResult) {
	troubleProcessor.WaitGroup.Add(1)
	troubleProcessor.dumpToFile(result)
	troubleProcessor.WaitGroup.Wait()
	fmt.Println("TroubleProcessor is done")
}
