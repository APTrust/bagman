package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

// FailedReplicationProcessor dumps the ReplicationResult structure of
// items where replication check could not be completed into readable
// JSON files for review.
type FailedReplicationProcessor struct {
	ProcUtil *bagman.ProcessUtil
}

func NewFailedReplicationProcessor(procUtil *bagman.ProcessUtil) (*FailedReplicationProcessor) {
	return &FailedReplicationProcessor{
		ProcUtil: procUtil,
	}
}

func (processor *FailedReplicationProcessor) HandleMessage(message *nsq.Message) error {
	var file bagman.File
	err := json.Unmarshal(message.Body, &file)
	if err != nil {
		detailedError := fmt.Errorf(
			"Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		processor.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	processor.dumpToFile(&file)
	processor.ProcUtil.MessageLog.Info("Processed %s", file.Identifier)
	return nil
}

func (processor *FailedReplicationProcessor) dumpToFile(file *bagman.File) error {
	outdir := path.Join(processor.ProcUtil.Config.LogDirectory, "replication_failures")
	if _, err := os.Stat(outdir); os.IsNotExist(err) {
		err := os.Mkdir(outdir, 0766)
		if err != nil {
			panic(err)
		}
	}
	filename := fmt.Sprintf(
		strings.Replace(file.Identifier, "/", "-", -1))
	json, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(path.Join(outdir, filename), json, 0644)
	if err != nil {
		panic(err)
	}
	return nil
}
