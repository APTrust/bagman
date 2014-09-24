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
cd ~/go/src/github.com/APTrust/bagman/apps/bucket_reader
go run bucket_reader.go -config apd4n

echo "Starting apt_prepare"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_prepare
go run apt_prepare.go -config apd4n &
PREPARE_PID=$!

echo "Waiting 20 seconds to start metarecord"
sleep 20

echo "Starting apt_record"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_record
go run apt_record.go -config apd4n &
RECORD_PID=$!

echo "Starting trouble processor"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_trouble
go run apt_trouble.go -config apd4n &
TROUBLE_PID=$!

# This one exits on its own
echo "Starting cleanup reader"
cd ~/go/src/github.com/APTrust/bagman/apps/cleanup_reader
go run cleanup_reader.go -config apd4n

echo "Starting cleanup processor"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_cleanup
go run apt_cleanup.go -config apd4n &
CLEANUP_PID=$!

kill_all()
{
    echo "Shutting down cleanup processor"
    kill -s SIGINT $CLEANUP_PID

    echo "Shutting down trouble processor"
    kill -s SIGINT $TROUBLE_PID

    echo "Shutting down metarecord processor"
    kill -s SIGINT $RECORD_PID

    echo "Shutting down bag processor"
    kill -s SIGINT $PREPARE_PID

    echo "Shutting down NSQ"
    kill -s SIGINT $NSQ_PID
}

trap kill_all SIGINT

wait $NSQ_PID
