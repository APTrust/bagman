// bagrestorer restores bags to partners' restoration buckets
package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/nsqio/go-nsq"
	"strings"
	"time"
)

// TODO: Change bagman.BagRestorer to RestoreHelper.
// It's confusing to have two classes with the same name.
// TODO: Merge RestoreObject struct below with bagman.RestoreHelper.

type BagRestorer struct {
	RestoreChannel chan *RestoreObject
	ResultsChannel chan *RestoreObject
	ProcUtil       *bagman.ProcessUtil
}

func NewBagRestorer(procUtil *bagman.ProcessUtil) (*BagRestorer) {
	bagRestorer := &BagRestorer {
		ProcUtil: procUtil,
	}
	workerBufferSize := procUtil.Config.RestoreWorker.Workers * 10
	bagRestorer.RestoreChannel = make(chan *RestoreObject, workerBufferSize)
	bagRestorer.ResultsChannel = make(chan *RestoreObject, workerBufferSize)
	for i := 0; i < procUtil.Config.RestoreWorker.Workers; i++ {
		go bagRestorer.logResult()
		go bagRestorer.doRestore()
	}
	return bagRestorer
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (bagRestorer *BagRestorer) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	object := RestoreObject{
		NsqMessage: message,
		Retry: true,
	}

	// Deserialize the NSQ JSON message into object.ProcessStatus
	err := json.Unmarshal(message.Body, &object.ProcessStatus)
	if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		bagRestorer.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	// If this item has already been restored, or is in process
	// of being restored, just finish the message and return.
	items, err := bagRestorer.ProcUtil.FluctusClient.RestorationItemsGet(object.ProcessStatus.ObjectIdentifier)
	if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Could not get current status of object %s from Fluctus: %v.",
			object.ProcessStatus.ObjectIdentifier, err)
		bagRestorer.ProcUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	// How can we know if another process is currently restoring the same object?
	// addToSyncmap below will tell us if this process is already working on it.
	if len(items) == 0 {
		bagRestorer.ProcUtil.MessageLog.Info("Marking %s as complete because Fluctus says it "+
			"has been restored, or restoration should not be retried",
			object.Key())
		message.Finish()
		return nil
	}

	// Make a note that we're working on this item
	err = bagRestorer.ProcUtil.RegisterItem(object.Key(), message.ID)
	if err != nil {
		bagRestorer.ProcUtil.MessageLog.Info("Marking %s as complete because the file is already "+
			"being restored under another message id.", object.Key())
		message.Finish()
		return nil
	}

	// Get the IntellectualObject from Fluctus & build a BagRestorer
	intelObj, err := bagRestorer.ProcUtil.FluctusClient.IntellectualObjectGetForRestore(object.Key())
	if err != nil {
		object.ErrorMessage = fmt.Sprintf("Cannot retrieve IntellectualObject %s from Fluctus: %v",
			object.Key(), err)
		bagRestorer.ResultsChannel <- &object
		return nil
	} else {
		object.BagRestorer, err = bagman.NewBagRestorer(
			intelObj,
			bagRestorer.ProcUtil.Config.RestoreDirectory,
			bagRestorer.ProcUtil.Config.RestoreToTestBuckets)
		if err != nil {
			object.ErrorMessage = fmt.Sprintf("Cannot create BagRestorer for %s: %v",
				object.Key(), err)
			bagRestorer.ResultsChannel <- &object
			return nil
		}
		object.BagRestorer.SetLogger(bagRestorer.ProcUtil.MessageLog)
		if bagRestorer.ProcUtil.Config.CustomRestoreBucket != "" {
			object.BagRestorer.SetCustomRestoreBucket(bagRestorer.ProcUtil.Config.CustomRestoreBucket)
		}
	}

	// Make sure we have enough disk space to build this item.
	err = bagRestorer.ProcUtil.Volume.Reserve(uint64(intelObj.TotalFileSize() * 2))
	if err != nil {
		// Not enough room on disk
		bagRestorer.ProcUtil.MessageLog.Warning("Requeueing %s - not enough disk space", object.Key())
		object.ErrorMessage = err.Error()
		bagRestorer.ResultsChannel <- &object
		return nil
	}

	// Mark all ProcessedItems related to this object as started
	err = bagRestorer.ProcUtil.FluctusClient.RestorationStatusSet(object.ProcessStatus.ObjectIdentifier,
		bagman.StageRequested, bagman.StatusStarted, "Restoration in process", false)
	if err != nil {
		detailedError := fmt.Errorf("Cannot register restoration start with Fluctus for %s: %v",
			object.Key(), err)
		object.ErrorMessage = detailedError.Error()
		bagRestorer.ResultsChannel <- &object
		return detailedError
	}

	// Now put the object into the channel for processing
	bagRestorer.RestoreChannel <- &object
	bagRestorer.ProcUtil.MessageLog.Info("Put %s into restore channel", object.Key())
	return nil
}


func (bagRestorer *BagRestorer) logResult() {
	for object := range bagRestorer.ResultsChannel {
		// Mark item as resolved in Fluctus & tell the queue what happened.
		var status bagman.StatusType = bagman.StatusSuccess
		var stage bagman.StageType = bagman.StageResolve
		note := ""
		if object.ErrorMessage != "" {
			status = bagman.StatusFailed
			stage = bagman.StageRequested
			note = object.ErrorMessage
		} else {
			note = fmt.Sprintf("Object restored to %s", strings.Join(object.RestorationUrls, ", "))
		}
		err := bagRestorer.ProcUtil.FluctusClient.RestorationStatusSet(object.ProcessStatus.ObjectIdentifier,
			stage, status, note, false)
		if err != nil {
			// Do we really want to go through the whole process
			// of restoring this again?
			bagRestorer.ProcUtil.MessageLog.Error("Requeueing %s because attempt to update status in Fluctus failed: %v",
				object.Key(), err)
			object.NsqMessage.Requeue(1 * time.Minute)
			bagRestorer.ProcUtil.IncrementFailed()
		} else if object.ErrorMessage != "" {
			bagRestorer.ProcUtil.MessageLog.Error("Requeueing %s: %s", object.Key(), object.ErrorMessage)
			object.NsqMessage.Requeue(1 * time.Minute)
			bagRestorer.ProcUtil.IncrementFailed()
		} else {
			bagRestorer.ProcUtil.MessageLog.Info("Restore of %s succeeded: %s", object.Key(), object.RestoredBagUrls())
			object.NsqMessage.Finish()
			bagRestorer.ProcUtil.IncrementSucceeded()
		}
		// No longer working on this
		bagRestorer.ProcUtil.UnregisterItem(object.Key())
		bagRestorer.ProcUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d", bagRestorer.ProcUtil.Succeeded(), bagRestorer.ProcUtil.Failed())
	}
}


func (bagRestorer *BagRestorer) doRestore() {
	for object := range bagRestorer.RestoreChannel {
		bagRestorer.ProcUtil.MessageLog.Info("Restoring %s", object.Key())
		if object.NsqMessage != nil {
			// Touch to prevent timeout. PivotalTracker #93237522
			object.NsqMessage.Touch()
		}
		urls, err := object.BagRestorer.RestoreAndPublish(object.NsqMessage)
		if object.NsqMessage != nil {
			// Touch to prevent timeout. PivotalTracker #93237522
			object.NsqMessage.Touch()
		}
		if err != nil {
			object.ErrorMessage = fmt.Sprintf("An error occurred during the restoration process: %v",
				err)
		} else {
			object.RestorationUrls = urls
		}
		bagRestorer.ResultsChannel <- object
	}
}

type RestoreObject struct {
	BagRestorer     *bagman.BagRestorer
	ProcessStatus   *bagman.ProcessStatus
	NsqMessage      *nsq.Message
	ErrorMessage    string
	Retry           bool
	RestorationUrls []string
	key             string
}

func (object *RestoreObject) Key() (string) {
	if object.ProcessStatus == nil {
		return ""
	}
	if object.key == "" {
		key := fmt.Sprintf("%s/%s", object.ProcessStatus.Institution, object.ProcessStatus.Name)
		object.key = key[0:len(key) - 4]  // remove ".tar" extension
	}
	return object.key
}

func (object *RestoreObject) RestoredBagUrls() (string) {
	if object.RestorationUrls == nil {
		return ""
	}
	return strings.Join(object.RestorationUrls, ", ")
}
