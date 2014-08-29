package main
import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/fluctus/client"
	"github.com/bitly/go-nsq"
	"github.com/op/go-logging"
	"os"
	"strings"
	"sync/atomic"
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
var config bagman.Config
var messageLog *logging.Logger
var succeeded = int64(0)
var failed = int64(0)
var volume *bagman.Volume
var fluctusClient *client.Client
var syncMap *bagman.SynchronizedMap

func main() {

	loadConfig()
	messageLog.Info("Restore started")
	err := config.EnsureFluctusConfig()
	if err != nil {
		messageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
	}

	fluctusClient, err = client.New(
		config.FluctusURL,
		config.FluctusAPIVersion,
		os.Getenv("FLUCTUS_API_USER"),
		os.Getenv("FLUCTUS_API_KEY"),
		messageLog)
	if err != nil {
		messageLog.Fatalf("Cannot initialize Fluctus Client: %v", err)
	}

	initVolume()
	initChannels()
	initGoRoutines()

	syncMap = bagman.NewSynchronizedMap()

	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(config.MaxRestoreAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "60m")
	consumer, err := nsq.NewConsumer(config.RestoreTopic,
		config.RestoreChannel, nsqConfig)
	if err != nil {
		messageLog.Fatalf(err.Error())
	}

	handler := &RestoreProcessor{}
	consumer.SetHandler(handler)
	consumer.ConnectToNSQLookupd(config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}

func loadConfig() {
	// Load the config or die.
	requestedConfig := flag.String("config", "", "configuration to run")
	flag.Parse()
	config = bagman.LoadRequestedConfig(requestedConfig)
	messageLog = bagman.InitLogger(config)
}

// Set up the volume to keep track of how much disk space is
// available. We want to avoid downloading large files when
// we know ahead of time that the volume containing the tar
// directory doesn't have enough space to accomodate them.
func initVolume() {
	var err error
	volume, err = bagman.NewVolume(config.RestoreDirectory, messageLog)
	if err != nil {
		panic(err.Error())
	}
}


// Set up the channels.
func initChannels() {
	workerBufferSize := config.Workers * 10
	channels = &Channels{}
	channels.RestoreChannel = make(chan *RestoreObject, workerBufferSize)
	channels.ResultsChannel = make(chan *RestoreObject, workerBufferSize)
}

// Set up our go routines. We want to limit the number of
// go routines so we do not have 1000+ simultaneous connections
// to Fluctus. That would just cause Fluctus to crash.
func initGoRoutines() {
	for i := 0; i < config.Workers; i++ {
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
		messageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}

	// If this item has already been restored, or is in process
	// of being restored, just finish the message and return.
	items, err := fluctusClient.RestorationItemsGet(object.ProcessStatus.ObjectIdentifier)
	if err != nil {
		detailedError := fmt.Errorf(
			"[ERROR] Could not get current status of object %s from Fluctus: %v.",
			object.ProcessStatus.ObjectIdentifier, err)
		messageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	// How can we know if another process is currently restoring the same object?
	// addToSyncmap below will tell us if this process is already working on it.
	if len(items) == 0 {
		messageLog.Info("Marking %s as complete because Fluctus says it "+
			"has been restored, or restoration should not be retried",
			object.Key())
		message.Finish()
		return nil
	}

	// Make a note that we're working on this item
	err = addToSyncMap(&object)
	if err != nil {
		messageLog.Info("Marking %s as complete because the file is already "+
			"being restored under another message id.", object.Key())
		message.Finish()
		return nil
	}

	// Get the IntellectualObject from Fluctus & build a BagRestorer
	intelObj, err := fluctusClient.IntellectualObjectGet(object.Key(), true)
	if err != nil {
		object.ErrorMessage = fmt.Sprintf("Cannot retrieve IntellectualObject %s from Fluctus: %v",
			object.Key(), err)
		channels.ResultsChannel <- &object
		return nil
	} else {
		object.BagRestorer, err = bagman.NewBagRestorer(intelObj, config.RestoreDirectory)
		if err != nil {
			object.ErrorMessage = fmt.Sprintf("Cannot create BagRestorer for %s: %v",
				object.Key(), err)
			channels.ResultsChannel <- &object
			return nil
		}
		object.BagRestorer.SetLogger(messageLog)
		if config.CustomRestoreBucket != "" {
			object.BagRestorer.SetCustomRestoreBucket(config.CustomRestoreBucket)
		}
	}

	// Make sure we have enough disk space to build this item.
	err = volume.Reserve(uint64(intelObj.TotalFileSize() * 2))
	if err != nil {
		// Not enough room on disk
		messageLog.Warning("Requeueing %s - not enough disk space", object.Key())
		object.ErrorMessage = err.Error()
		channels.ResultsChannel <- &object
		return nil
	}

	// Mark all ProcessedItems related to this object as started
	err = fluctusClient.RestorationStatusSet(object.ProcessStatus.ObjectIdentifier,
		bagman.StageRequested, bagman.StatusStarted, false)
	if err != nil {
		detailedError := fmt.Errorf("Cannot register restoration start with Fluctus for %s: %v",
			object.Key(), err)
		object.ErrorMessage = detailedError.Error()
		channels.ResultsChannel <- &object
		return detailedError
	}

	// Now put the object into the channel for processing
	channels.RestoreChannel <- &object
	messageLog.Info("Put %s into restore channel", object.Key())
	return nil
}

func addToSyncMap(object *RestoreObject) (error) {
	// Don't start working on a message that we're already working on.
	messageId := make([]byte, nsq.MsgIDLength)
	for i := range messageId {
		messageId[i] = object.NsqMessage.ID[i]
	}
	key := object.Key()
	if syncMap.HasKey(key) && syncMap.Get(key) != string(messageId) {
		return fmt.Errorf("Bag restorer is already working on this message")
	} else {
		// Make a note that we're processing this file.
		syncMap.Add(key, string(messageId))
	}
	return nil
}

func logResult() {
	for object := range channels.ResultsChannel {
		// Mark item as resolved in Fluctus & tell the queue what happened.
		// Last param (false) sets retry to false, so we don't try to restore again.
		err := fluctusClient.RestorationStatusSet(object.ProcessStatus.ObjectIdentifier,
			bagman.StageResolve, bagman.StatusSuccess, false)
		if err != nil {
			// Do we really want to go through the whole process
			// of restoring this again?
			messageLog.Error("Requeueing %s because attempt to update status in Fluctus failed: %v",
				object.Key(), err)
			object.NsqMessage.Requeue(1 * time.Minute)
			atomic.AddInt64(&failed, 1)
		} else if object.ErrorMessage != "" {
			messageLog.Error("Requeueing %s: %s", object.Key(), object.ErrorMessage)
			object.NsqMessage.Requeue(1 * time.Minute)
			atomic.AddInt64(&failed, 1)
		} else {
			messageLog.Info("Restore of %s succeeded: %s", object.Key(), object.RestoredBagUrls())
			object.NsqMessage.Finish()
			atomic.AddInt64(&succeeded, 1)
		}
		// No longer working on this
		syncMap.Delete(object.Key())
		messageLog.Info("**STATS** Succeeded: %d, Failed: %d", succeeded, failed)
	}
}


func doRestore() {
	for object := range channels.RestoreChannel {
		messageLog.Info("Restoring %s", object.Key())
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
