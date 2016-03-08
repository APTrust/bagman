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

	// Ignore errors, as bags may not have processed item records,
	// and our goal is mainly to dump out the manifest at this point.
	result.processStatus, _ = troubleProcessor.ProcUtil.FluctusClient.GetBagStatusById(result.ProcessedItemId)
	if result.processStatus != nil {
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
	} else if result.PackageResult != nil && result.PackageResult.BagBuilder != nil {
		bagUUID = result.PackageResult.BagBuilder.UUID
	} else {
		bagUUID = fmt.Sprintf("unknown-bag-%d", time.Now().UTC().Unix())
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
	if result.processStatus == nil {
		return
	}
	result.processStatus.Date = time.Now()
	result.processStatus.Status = "Failed"
	result.processStatus.Note = result.ErrorMessage
	result.processStatus.SetNodePidState(result, troubleProcessor.ProcUtil.MessageLog)
	result.processStatus.Node = ""
	result.processStatus.Pid = 0
	err := troubleProcessor.ProcUtil.FluctusClient.UpdateProcessedItem(result.processStatus)
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
