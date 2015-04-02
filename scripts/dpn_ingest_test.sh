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
fi

echo "building dpn_ingest_devtest"
cd "${BAGMAN_HOME}/apps/dpn_ingest_devtest"
go build -o ${BAGMAN_BIN}/dpn_ingest_devtest dpn_ingest_devtest.go

echo "copying bagbuilder_config.json into bin directory"
cp ${BAGMAN_HOME}/dpn/bagbuilder_config.json ${BAGMAN_BIN}/

echo "running dpn ingest test"
cd ${BAGMAN_BIN}
./dpn_ingest_devtest -config=dev
cd ${ORIGINAL_DIR}
