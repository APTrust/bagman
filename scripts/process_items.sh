#!/bin/bash

echo "Getting rid of old logs and data files"
rm -r ~/tmp/*

echo "Starting NSQ"
cd ~/go/src/github.com/APTrust/bagman/nsq
go run service.go -config ~/go/src/github.com/APTrust/bagman/nsq/nsqd.apd4n.config &>/dev/null &
NSQ_PID=$!
sleep 3

# Wait for this one to finish
echo "Starting bucket reader"
cd ~/go/src/github.com/APTrust/bagman/bucket_reader
go run bucket_reader.go -config apd4n

echo "Starting bag_processor"
cd ~/go/src/github.com/APTrust/bagman/bag_processor
go run bag_processor.go -config apd4n &
PROCESSOR_PID=$!

echo "Waiting 20 seconds to start metarecord"
sleep 20

echo "Starting metarecord"
cd ~/go/src/github.com/APTrust/bagman/metarecord
go run metarecord.go -config apd4n &
METARECORD_PID=$!

echo "Starting trouble processor"
cd ~/go/src/github.com/APTrust/bagman/trouble
go run trouble.go -config apd4n &
TROUBLE_PID=$!

# This one exits on its own
echo "Starting cleanup reader"
cd ~/go/src/github.com/APTrust/bagman/cleanup_reader
go run cleanup_reader.go -config apd4n

echo "Starting cleanup processor"
cd ~/go/src/github.com/APTrust/bagman/cleanup
go run cleanup.go -config apd4n &
CLEANUP_PID=$!

kill_all()
{
    echo "Shutting down cleanup processor"
    kill -s SIGINT $CLEANUP_PID

    echo "Shutting down trouble processor"
    kill -s SIGINT $TROUBLE_PID

    echo "Shutting down metarecord processor"
    kill -s SIGINT $METARECORD_PID

    echo "Shutting down bag processor"
    kill -s SIGINT $PROCESSOR_PID

    echo "Shutting down NSQ"
    kill -s SIGINT $NSQ_PID
}

trap kill_all SIGINT

wait $NSQ_PID
