#!/bin/bash
#
# Build bagman binaries for AWS servers.
# The binaries MUST be built on the target server.
# Although go does support cross-compiling, you cannot
# currently cross-compile applications that use cgo.
# Bagman uses the cgo magic mime libraries to parse file
# mime types.
#
# Because we're building on the target server, the target
# server must have all of the libraries required for
# building. We don't want to automatically call go get
# on the target server because that gets the latest master
# branch of all libraries, some of which have breaking
# API changes.

BAGMAN_HOME=~/go/src/github.com/APTrust/bagman
BAGMAN_BIN=${BAGMAN_HOME}/bin

if [ ! -d ${BAGMAN_DIR} ]; then
    mkdir ${BAGMAN_DIR}
else
    echo "cleaning out bin directory"
    rm ${BAGMAN_BIN}/*
fi

echo "building apt_nsq_service"
cd "${BAGMAN_HOME}/nsq"
go build -o ${BAGMAN_BIN}/apt_nsq_service service.go

echo "building apt_prepare"
cd "${BAGMAN_HOME}/apps/apt_prepare"
go build -o ${BAGMAN_BIN}/apt_prepare apt_prepare.go

echo "building apt_store"
cd "${BAGMAN_HOME}/apps/apt_store"
go build -o ${BAGMAN_BIN}/apt_store apt_store.go

echo "building apt_record"
cd "${BAGMAN_HOME}/apps/apt_record"
go build -o ${BAGMAN_BIN}/apt_record apt_record.go

echo "building apt_replicate"
cd "${BAGMAN_HOME}/apps/apt_replicate"
go build -o ${BAGMAN_BIN}/apt_replicate apt_replicate.go

echo "building apt_trouble"
cd "${BAGMAN_HOME}/apps/apt_trouble"
go build -o ${BAGMAN_BIN}/apt_trouble apt_trouble.go

echo "building apt_bag_delete"
cd "${BAGMAN_HOME}/apps/apt_bag_delete"
go build -o ${BAGMAN_BIN}/apt_bag_delete apt_bag_delete.go

echo "building apt_restore"
cd "${BAGMAN_HOME}/apps/apt_restore"
go build -o ${BAGMAN_BIN}/apt_restore apt_restore.go

echo "building apt_file_delete"
cd "${BAGMAN_HOME}/apps/apt_file_delete"
go build -o ${BAGMAN_BIN}/apt_file_delete apt_file_delete.go

echo "building bucket_reader"
cd "${BAGMAN_HOME}/apps/bucket_reader"
go build -o ${BAGMAN_BIN}/bucket_reader bucket_reader.go

echo "building request_reader"
cd "${BAGMAN_HOME}/apps/request_reader"
go build -o ${BAGMAN_BIN}/request_reader request_reader.go

echo "building cleanup_reader"
cd "${BAGMAN_HOME}/apps/cleanup_reader"
go build -o ${BAGMAN_BIN}/cleanup_reader cleanup_reader.go

echo "building fixity_reader"
cd "${BAGMAN_HOME}/apps/fixity_reader"
go build -o ${BAGMAN_BIN}/fixity_reader fixity_reader.go

echo "building apt_retry"
cd "${BAGMAN_HOME}/apps/apt_retry"
go build -o ${BAGMAN_BIN}/apt_retry apt_retry.go

echo "building apt_fixity"
cd "${BAGMAN_HOME}/apps/apt_fixity"
go build -o ${BAGMAN_BIN}/apt_fixity apt_fixity.go

echo "building apt_failed_fixity"
cd "${BAGMAN_HOME}/apps/apt_failed_fixity"
go build -o ${BAGMAN_BIN}/apt_failed_fixity apt_failed_fixity.go

echo "building apt_failed_replication"
cd "${BAGMAN_HOME}/apps/apt_failed_replication"
go build -o ${BAGMAN_BIN}/apt_failed_replication apt_failed_replication.go

echo "building apt_download"
cd "${BAGMAN_HOME}/partner-apps/apt_download"
go build -o ${BAGMAN_BIN}/apt_download apt_download.go

echo "building apt_list"
cd "${BAGMAN_HOME}/partner-apps/apt_list"
go build -o ${BAGMAN_BIN}/apt_list apt_list.go

echo "building apt_upload"
cd "${BAGMAN_HOME}/partner-apps/apt_upload"
go build -o ${BAGMAN_BIN}/apt_upload apt_upload.go

echo "building apt_validate"
cd "${BAGMAN_HOME}/partner-apps/apt_validate"
go build -o ${BAGMAN_BIN}/apt_validate apt_validate.go

echo "copying config files into bin directory"
cp ${BAGMAN_HOME}/config/config.json ${BAGMAN_BIN}/
cp ${BAGMAN_HOME}/nsq/*.config ${BAGMAN_BIN}/
