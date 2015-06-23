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

if [ ! -d ${BAGMAN_BIN} ]; then
	mkdir ${BAGMAN_BIN}
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

echo "building dpn_check_requests"
cd "${BAGMAN_HOME}/apps/dpn_check_requests"
go build -o ${BAGMAN_BIN}/dpn_check_requests dpn_check_requests.go

echo "building dpn_copy"
cd "${BAGMAN_HOME}/apps/dpn_copy"
go build -o ${BAGMAN_BIN}/dpn_copy dpn_copy.go

echo "building dpn_package"
cd "${BAGMAN_HOME}/apps/dpn_package"
go build -o ${BAGMAN_BIN}/dpn_package dpn_package.go

echo "building dpn_record"
cd "${BAGMAN_HOME}/apps/dpn_record"
go build -o ${BAGMAN_BIN}/dpn_record dpn_record.go

echo "building dpn_store"
cd "${BAGMAN_HOME}/apps/dpn_store"
go build -o ${BAGMAN_BIN}/dpn_store dpn_store.go

echo "building dpn_sync"
cd "${BAGMAN_HOME}/apps/dpn_sync"
go build -o ${BAGMAN_BIN}/dpn_sync dpn_sync.go

echo "building dpn_trouble"
cd "${BAGMAN_HOME}/apps/dpn_trouble"
go build -o ${BAGMAN_BIN}/dpn_trouble dpn_trouble.go

echo "building dpn_validate"
cd "${BAGMAN_HOME}/apps/dpn_validate"
go build -o ${BAGMAN_BIN}/dpn_validate dpn_validate.go

echo "building dpn_ingest_devtest"
cd "${BAGMAN_HOME}/apps/dpn_ingest_devtest"
go build -o ${BAGMAN_BIN}/dpn_ingest_devtest dpn_ingest_devtest.go

echo "building apt_download -tags='partners'"
cd "${BAGMAN_HOME}/partner-apps/apt_download"
go build -tags='partners' -o ${BAGMAN_BIN}/apt_download apt_download.go

echo "building apt_list -tags='partners'"
cd "${BAGMAN_HOME}/partner-apps/apt_list"
go build -tags='partners' -o ${BAGMAN_BIN}/apt_list apt_list.go

echo "building apt_upload -tags='partners'"
cd "${BAGMAN_HOME}/partner-apps/apt_upload"
go build -tags='partners' -o ${BAGMAN_BIN}/apt_upload apt_upload.go

echo "building apt_validate -tags='partners'"
cd "${BAGMAN_HOME}/partner-apps/apt_validate"
go build -tags='partners' -o ${BAGMAN_BIN}/apt_validate apt_validate.go

echo "building apt_delete -tags='partners'"
cd "${BAGMAN_HOME}/partner-apps/apt_delete"
go build -tags='partners' -o ${BAGMAN_BIN}/apt_delete apt_delete.go

echo "copying config files into bin directory"
cp ${BAGMAN_HOME}/config/config.json ${BAGMAN_BIN}/
cp ${BAGMAN_HOME}/nsq/*.config ${BAGMAN_BIN}/
cp ${BAGMAN_HOME}/dpn/dpn_config.json ${BAGMAN_BIN}/
