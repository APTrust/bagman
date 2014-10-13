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

echo "Waiting 20 seconds to start apt_store"
sleep 20

echo "Starting apt_store"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_store
go run apt_store.go -config apd4n &
STORE_PID=$!

echo "Waiting 20 seconds to start apt_record"
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
cd ~/go/src/github.com/APTrust/bagman/apps/apt_bag_delete
go run apt_bag_delete.go -config apd4n &
CLEANUP_PID=$!

kill_all()
{
    echo "Shutting down cleanup worker"
    kill -s SIGINT $CLEANUP_PID

    echo "Shutting down trouble worker"
    kill -s SIGINT $TROUBLE_PID

    echo "Shutting down record worker"
    kill -s SIGINT $RECORD_PID

    echo "Shutting down storage worker"
    kill -s SIGINT $STORE_PID

    echo "Shutting down prepare"
    kill -s SIGINT $PREPARE_PID

    echo "Shutting down NSQ"
    kill -s SIGINT $NSQ_PID
}

trap kill_all SIGINT

wait $NSQ_PID
