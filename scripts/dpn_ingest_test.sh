#!/bin/bash
#

ORIGINAL_DIR=`pwd`
BAGMAN_HOME=~/go/src/github.com/APTrust/bagman
BAGMAN_BIN=${BAGMAN_HOME}/bin

echo "----------------------------------------------------"
echo "Be sure Fluctus is running at http://localhost:3000"
echo "and that it has the expected test data loaded."
echo "The instructions for that are in the comments in the"
echo "top of scripts/process_items.sh"
echo "----------------------------------------------------"

if [ ! -d ${BAGMAN_BIN} ]; then
	mkdir ${BAGMAN_BIN}
else
    echo "Removing old dpn_ingest_devtest binary from bin directory"
	rm ${BAGMAN_BIN}/dpn_ingest_devtest
	rm ${BAGMAN_BIN}/dpn_trouble
fi

echo "Starting NSQ"
cd ~/go/src/github.com/APTrust/bagman/nsq
go run service.go -config ~/go/src/github.com/APTrust/bagman/nsq/nsqd.dev.config &>/dev/null &
NSQ_PID=$!
sleep 3

echo "Adding one test item to the trouble queue"
curl -d '{"BagIdentifier": "12345-XYZ", "ErrorMessage": "Testing..."}' 'http://127.0.0.1:4151/put?topic=dpn_trouble_topic'
echo ""

echo "building dpn_ingest_devtest"
cd "${BAGMAN_HOME}/apps/dpn_ingest_devtest"
go build -o ${BAGMAN_BIN}/dpn_ingest_devtest dpn_ingest_devtest.go

echo "building dpn_trouble"
cd "${BAGMAN_HOME}/apps/dpn_trouble"
go build -o ${BAGMAN_BIN}/dpn_trouble dpn_trouble.go

echo "copying bagbuilder_config.json into bin directory"
cp ${BAGMAN_HOME}/dpn/bagbuilder_config.json ${BAGMAN_BIN}/

echo "running dpn ingest test and dpn trouble processor"
cd ${BAGMAN_BIN}
./dpn_ingest_devtest -config=dev &
INGEST_PID=$!
./dpn_trouble -config=dev &
TROUBLE_PID=$!
cd ${ORIGINAL_DIR}


kill_all()
{
    echo "Shutting down ingest worker"
    kill $INGEST_PID

    echo "Shutting down trouble worker"
    kill $TROUBLE_PID

    echo "Shutting down NSQ"
    kill -s SIGINT $NSQ_PID
}

trap kill_all SIGINT

wait $NSQ_PID
