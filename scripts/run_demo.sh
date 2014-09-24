#!/bin/bash

echo "Starting NSQ"
cd ~/go/src/github.com/APTrust/bagman/nsq
go run service.go -config ~/go/src/github.com/APTrust/bagman/nsq/nsqd.demo.config &
NSQ_PID=$!
sleep 3

# Wait for this one to finish
echo "Starting bucket reader"
cd ~/go/src/github.com/APTrust/bagman/apps/bucket_reader
go run bucket_reader.go -config demo

echo "Starting apt_prepare"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_prepare
go run apt_prepare.go -config demo &
PREPARE_PID=$!

echo "Waiting 20 seconds to start apt_store"
sleep 20

echo "Starting apt_store"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_store
go run apt_store.go -config demo &
STORE_PID=$!

echo "Waiting 20 seconds to start apt_record"
sleep 20

echo "Starting apt_record"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_record
go run apt_record.go -config demo &
RECORD_PID=$!

echo "Starting trouble processor"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_trouble
go run apt_trouble.go -config demo &
TROUBLE_PID=$!

# Wait for this one to finish
echo "Starting request reader"
cd ~/go/src/github.com/APTrust/bagman/apps/request_reader
go run request_reader.go -config apd4n

echo "Starting cleanup processor"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_cleanup
go run apt_cleanup.go -config demo &
CLEANUP_PID=$!

echo "Starting bag restorer"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_restore
go run apt_restore.go -config apd4n &
RESTORE_PID=$!

echo "Starting generic file deleter"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_delete
go run apt_delete.go -config apd4n &
DELETE_PID=$!


kill_all()
{
    echo "Shutting down restore worker"
    kill -s SIGINT $RESTORE_PID

    echo "Shutting down delete worker"
    kill -s SIGINT $DELETE_PID

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
