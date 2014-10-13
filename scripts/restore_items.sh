#!/bin/bash

echo "Getting rid of old logs and data files"
rm -r ~/tmp/*

echo "Starting NSQ"
cd ~/go/src/github.com/APTrust/bagman/nsq
go run service.go -config ~/go/src/github.com/APTrust/bagman/nsq/nsqd.apd4n.config &>/dev/null &
NSQ_PID=$!
sleep 3

# Wait for this one to finish
echo "Starting request reader"
cd ~/go/src/github.com/APTrust/bagman/apps/request_reader
go run request_reader.go -config apd4n

echo "Starting bag restorer"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_restore
go run apt_restore.go -config apd4n &
RESTORE_PID=$!

echo "Starting generic file deleter"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_file_delete
go run apt_file_delete.go -config apd4n &
DELETE_PID=$!

kill_all()
{
    echo "Shutting down deleter"
    kill -s SIGINT $DELETE_PID

    echo "Shutting down bag restorer"
    kill -s SIGINT $RESTORE_PID

    echo "Shutting down NSQ"
    kill -s SIGINT $NSQ_PID
}

trap kill_all SIGINT

wait $NSQ_PID
