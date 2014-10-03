package main
import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/processutil"
	"github.com/bitly/go-nsq"
	"strings"
	"time"
)

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

type Channels struct {
	RestoreChannel chan *RestoreObject
	ResultsChannel chan *RestoreObject
}

// Global vars.
var channels *Channels
var procUtil *processutil.ProcessUtil

func main() {
	requestedConfig := flag.String("config", "", "Configuration to run. Options are in config.json file. REQUIRED")
	customEnvFile := flag.String("env", "", "Absolute path to file containing custom environment vars. OPTIONAL")
	flag.Parse()
	procUtil = processutil.NewProcessUtil(requestedConfig)
	bagman.LoadCustomEnvOrDie(customEnvFile, procUtil.MessageLog)

	procUtil.MessageLog.Info("Restore started")
	err := procUtil.Config.EnsureFluctusConfig()
	if err != nil {
		procUtil.MessageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
	}

	initChannels()
	initGoRoutines()

	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(procUtil.Config.MaxRestoreAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "180m")
	consumer, err := nsq.NewConsumer(procUtil.Config.RestoreTopic,
		procUtil.Config.RestoreChannel, nsqConfig)
	if err != nil {
		procUtil.MessageLog.Fatalf(err.Error())
	}

	handler := &RestoreProcessor{}
	consumer.SetHandler(handler)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}

// Set up the channels.
func initChannels() {
	workerBufferSize := procUtil.Config.RestoreWorkers * 10
	channels = &Channels{}
	channels.RestoreChannel = make(chan *RestoreObject, workerBufferSize)
	channels.ResultsChannel = make(chan *RestoreObject, workerBufferSize)
}

// Set up our go routines. We want to limit the number of
// go routines so we do not have 1000+ simultaneous connections
// to Fluctus. That would just cause Fluctus to crash.
func initGoRoutines() {
	for i := 0; i < procUtil.Config.RestoreWorkers; i++ {
		go logResult()
		go doRestore()
	}
}


type RestoreProcessor struct {
}

// MessageHandler handles messages from the queue, putting each
// item into the pipleline.
func (*RestoreProcessor) HandleMessage(message *nsq.Message) error {
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
		procUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	// If this item has already been restored, or is in process
	// of being restored, just finish the message and return.
	items, err := procUtil.FluctusClient.RestorationItemsGet(object.ProcessStatus.ObjectIdentifier)
	if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Could not get current status of object %s from Fluctus: %v.",
			object.ProcessStatus.ObjectIdentifier, err)
		procUtil.MessageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	// How can we know if another process is currently restoring the same object?
	// addToSyncmap below will tell us if this process is already working on it.
	if len(items) == 0 {
		procUtil.MessageLog.Info("Marking %s as complete because Fluctus says it "+
			"has been restored, or restoration should not be retried",
			object.Key())
		message.Finish()
		return nil
	}

	// Make a note that we're working on this item
	err = procUtil.RegisterItem(object.Key(), message.ID)
	if err != nil {
		procUtil.MessageLog.Info("Marking %s as complete because the file is already "+
			"being restored under another message id.", object.Key())
		message.Finish()
		return nil
	}

	// Get the IntellectualObject from Fluctus & build a BagRestorer
	intelObj, err := procUtil.FluctusClient.IntellectualObjectGet(object.Key(), true)
	if err != nil {
		object.ErrorMessage = fmt.Sprintf("Cannot retrieve IntellectualObject %s from Fluctus: %v",
			object.Key(), err)
		channels.ResultsChannel <- &object
		return nil
	} else {
		object.BagRestorer, err = bagman.NewBagRestorer(intelObj, procUtil.Config.RestoreDirectory)
		if err != nil {
			object.ErrorMessage = fmt.Sprintf("Cannot create BagRestorer for %s: %v",
				object.Key(), err)
			channels.ResultsChannel <- &object
			return nil
		}
		object.BagRestorer.SetLogger(procUtil.MessageLog)
		if procUtil.Config.CustomRestoreBucket != "" {
			object.BagRestorer.SetCustomRestoreBucket(procUtil.Config.CustomRestoreBucket)
		}
	}

	// Make sure we have enough disk space to build this item.
	err = procUtil.Volume.Reserve(uint64(intelObj.TotalFileSize() * 2))
	if err != nil {
		// Not enough room on disk
		procUtil.MessageLog.Warning("Requeueing %s - not enough disk space", object.Key())
		object.ErrorMessage = err.Error()
		channels.ResultsChannel <- &object
		return nil
	}

	// Mark all ProcessedItems related to this object as started
	err = procUtil.FluctusClient.RestorationStatusSet(object.ProcessStatus.ObjectIdentifier,
		bagman.StageRequested, bagman.StatusStarted, "Restoration in process", false)
	if err != nil {
		detailedError := fmt.Errorf("Cannot register restoration start with Fluctus for %s: %v",
			object.Key(), err)
		object.ErrorMessage = detailedError.Error()
		channels.ResultsChannel <- &object
		return detailedError
	}

	// Now put the object into the channel for processing
	channels.RestoreChannel <- &object
	procUtil.MessageLog.Info("Put %s into restore channel", object.Key())
	return nil
}


func logResult() {
	for object := range channels.ResultsChannel {
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
		err := procUtil.FluctusClient.RestorationStatusSet(object.ProcessStatus.ObjectIdentifier,
			stage, status, note, false)
		if err != nil {
			// Do we really want to go through the whole process
			// of restoring this again?
			procUtil.MessageLog.Error("Requeueing %s because attempt to update status in Fluctus failed: %v",
				object.Key(), err)
			object.NsqMessage.Requeue(1 * time.Minute)
			procUtil.IncrementFailed()
		} else if object.ErrorMessage != "" {
			procUtil.MessageLog.Error("Requeueing %s: %s", object.Key(), object.ErrorMessage)
			object.NsqMessage.Requeue(1 * time.Minute)
			procUtil.IncrementFailed()
		} else {
			procUtil.MessageLog.Info("Restore of %s succeeded: %s", object.Key(), object.RestoredBagUrls())
			object.NsqMessage.Finish()
			procUtil.IncrementSucceeded()
		}
		// No longer working on this
		procUtil.UnregisterItem(object.Key())
		procUtil.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d", procUtil.Succeeded(), procUtil.Failed())
	}
}


func doRestore() {
	for object := range channels.RestoreChannel {
		procUtil.MessageLog.Info("Restoring %s", object.Key())
		urls, err := object.BagRestorer.RestoreAndPublish()
		if err != nil {
			object.ErrorMessage = fmt.Sprintf("An error occurred during the restoration process: %v",
				err)
		} else {
			object.RestorationUrls = urls
		}
		channels.ResultsChannel <- object
	}
}
