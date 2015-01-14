#!/bin/bash

echo "Getting rid of old logs and data files"
rm -r ~/tmp/*

echo "Starting NSQ"
cd ~/go/src/github.com/APTrust/bagman/nsq
go run service.go -config ~/go/src/github.com/APTrust/bagman/nsq/nsqd.dev.config &>/dev/null &
NSQ_PID=$!
sleep 3

# Wait for this one to finish
echo "Starting fixity reader"
cd ~/go/src/github.com/APTrust/bagman/apps/fixity_reader
go run fixity_reader.go -config dev -date="2027-11-17T14:14:41Z"

echo "Starting fixity checker"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_fixity
go run apt_fixity.go -config dev &
FIXITY_PID=$!

echo "Starting failed fixity worker"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_failed_fixity
go run apt_failed_fixity.go -config dev &
FAILED_FIXITY_PID=$!

kill_all()
{
    echo "Shutting down failed fixity worker"
    kill -s SIGINT $FAILED_FIXITY_PID

    echo "Shutting down fixity checker"
    kill -s SIGINT $FIXITY_PID

    echo "Shutting down NSQ"
    kill -s SIGINT $NSQ_PID
}

trap kill_all SIGINT

wait $NSQ_PID
