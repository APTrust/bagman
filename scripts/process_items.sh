#!/bin/bash

echo "Getting rid of old logs and data files"
rm -r ~/tmp/*

echo "Starting NSQ"
cd ~/go/src/github.com/APTrust/bagman/nsq
go run service.go -config ~/go/src/github.com/APTrust/bagman/nsq/nsqd.dev.config &>/dev/null &
NSQ_PID=$!
sleep 3

# Wait for this one to finish
echo "Starting bucket reader"
cd ~/go/src/github.com/APTrust/bagman/apps/bucket_reader
go run bucket_reader.go -config dev

echo "Starting apt_prepare"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_prepare
go run apt_prepare.go -config dev &
PREPARE_PID=$!

echo "Waiting 20 seconds to start apt_store"
sleep 20

echo "Starting apt_store"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_store
go run apt_store.go -config dev &
STORE_PID=$!

echo "Waiting 20 seconds to start apt_record"
sleep 20

echo "Starting apt_record"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_record
go run apt_record.go -config dev &
RECORD_PID=$!

echo "Starting trouble processor"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_trouble
go run apt_trouble.go -config dev &
TROUBLE_PID=$!

# This one exits on its own
echo "Starting cleanup reader"
cd ~/go/src/github.com/APTrust/bagman/apps/cleanup_reader
go run cleanup_reader.go -config dev

echo "Starting cleanup processor"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_bag_delete
go run apt_bag_delete.go -config dev &
CLEANUP_PID=$!

echo "Waiting 20 seconds to start apt_replicate"
sleep 20

echo "Starting apt_replicate"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_replicate
go run apt_replicate.go -config dev &
REPLICATION_PID=$!

echo "Starting apt_failed_replication"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_failed_replication
go run apt_failed_replication.go -config dev &
FAILED_REPLICATION_PID=$!


kill_all()
{
    echo "Shutting down replication worker"
    kill -s SIGINT $FAILED_REPLICATION_PID

    echo "Shutting down replication worker"
    kill -s SIGINT $REPLICATION_PID

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
