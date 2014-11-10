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

// FailedFixityProcessor dumps the FixityResult structure of
// items where fixity check could not be completed into readable
// JSON files for review.
type FailedFixityProcessor struct {
	ProcUtil *bagman.ProcessUtil
}

func NewFailedFixityProcessor(procUtil *bagman.ProcessUtil) (*FailedFixityProcessor) {
	return &FailedFixityProcessor{
		ProcUtil: procUtil,
	}
}

func (processor *FailedFixityProcessor) HandleMessage(message *nsq.Message) error {
	var result bagman.FixityResult
	err := json.Unmarshal(message.Body, &result)
	if err != nil {
		detailedError := fmt.Errorf(
			"Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		processor.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	processor.dumpToFile(&result)
	processor.ProcUtil.MessageLog.Info("Processed %s", result.GenericFile.Identifier)
	return nil
}

func (processor *FailedFixityProcessor) dumpToFile(result *bagman.FixityResult) error {
	outdir := path.Join(processor.ProcUtil.Config.LogDirectory, "trouble")
	if _, err := os.Stat(outdir); os.IsNotExist(err) {
		err := os.Mkdir(outdir, 0766)
		if err != nil {
			panic(err)
		}
	}
	filename := fmt.Sprintf(
		strings.Replace(result.GenericFile.Identifier, "/", "-", -1))
	json, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(path.Join(outdir, filename), json, 0644)
	if err != nil {
		panic(err)
	}
	return nil
}
