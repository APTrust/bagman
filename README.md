# bagman

Server side code for managing BagIt bags sent to and managed by APTrust.

## Prerequisites

Install libmagic first. If you're on a Mac, do it this way:

```
	brew install libmagic
```

If you're on Linux, use apt-get or yum to install libmagic-dev:

```
	sudo apt-get install libmagic-dev
```

Install these prerequisites as well:

```
	go get launchpad.net/goamz
	go get github.com/nu7hatch/gouuid
	go get github.com/bitly/go-nsq
	go get github.com/rakyll/magicmime
	go get github.com/op/go-logging
	go get github.com/APTrust/bagins
```

## Installation

It's as easy as:

```
	go get github.com/APTrust/bagman
```

## Configuration

You must have the environment variables AWS_ACCESS_KEY_ID and
AWS_SECRET_ACCESS_KEY in your environment for bagman to connect
to S3.

You can create a named custom configuration in config.json, and
then run bagman with that named configuration using:

```
	cd cli
	go run cli.go -config=dev
```

... or whatever named configuration you want. If you dont specify a
configuration, the cli program will print a list of available
configurations.

You can run bagman locally with the following command. This will pull
down files <= 200k and process them. You will likely have to change
the TarDirectory and LogDirectory in the config.json file to paths
that actually exist on your system.

At the moment, bagman retrieves files from some hard-coded bucket
names that you won't be able to access unless you are using the
APTrust AWS account.

## Testing

You can run basic unit tests by changing into the bagman/bagman
directory and running this:

```
	go test
```

... but because bagman has to work with NSQ, S3 and a Rails app called
Fluctus, the integration tests are much more useful and
informative. See the [Testing Documentation](Testing.md) for more
information about unit tests and integration tests.

## Running Locally with NSQ

See the run_demo.sh script in the scripts directory. This runs all of
the services required for bag processing, using the "demo"
configuration settings in the config.json file. To run everything
locally, you can set up your own config secion in config.json, then
run one of the scripts in the scripts directory, using your newly
created config.

The config settings you'll need to change to run locally are
TarDirectory, LogDirectory and RestoreDirectory. These should all
point to existing directories in which you have read/write access. You
should also set MaxFileSize to a relatively low value (100000 -
250000), so that you only fetch and process relatively small files
from the S3 receiving buckets. If MaxFileSize is set too high, or if
it's set to zero, which means no limit, you will wind up downloading
and processing an enormous amount of data.

The scripts directory also includes process_items.sh and
restore_items.sh. These are good for running local end-to-end
integration tests. To run these on your own machine, you'll have to
change the config setting in those scripts from dev to whatever
config name you've set up. You also *must* have Fluctus running
locally (or at whatever URL the FluctusURL setting points to).

The process_items.sh script runs the bucket reader to look for items
in the receiving buckets, then ingests them. The restore_items.sh
script looks for items marked for delete and restore, and deletes or
restores them.

A good way to run end-to-end tests locally is to run process_items.sh,
then go to http://localhost:3000 and mark a few intellectual objects
for delete and a few for restore. Then kill process_items.sh and run
restore_items.sh. Both scripts print occasional **STATS** lines that
show the number of items that have processed successfully and the
number that have failed.

## NSQ

The nsq folder contains service.go, which is a wrapper around NSQ. It
also contains config files with settings for the NSQ demon. See the
README in the nsq directory for more information.

## APTrust Apps

Bag processing, deletion and restoration requires a number of
apps. Here's a breakdown of the apps and what they do:

### apt_bag_delete - Delete Bags After Ingest

*apps/bag_delete* deletes bags from partners' receiving buckets after
 the bags have been successfully ingested.

### apt_failed_fixity - Record Failed Fixity Events

*apps/failed_fixity* records information about failed attempts to run
 fixity checks on items in the preservation bucket.

### apt_failed_replication - Record Failed Replication Events

*apps/failed_replication* records information about failed attempts to
 copy files from the preservation bucket in the US East region to the
 replication bucket in the US West 2 region.

### apt_file_delete - Delete Generic Files from Preservation

*apps/apt_file_delete deletes files from the perservation bucket. This is
 done only at the request of the institution that owns the files.

### apt_fixity - Run Fixity Checks

*apps/apt_fixity runs periodic fixity checks on files in the
 preservation storage bucket.

### apt_prepare - Prepare Bags for Ingest

*apps/apt_prepare* handles the first stages on ingest. It reads from
 NSQ's prepare_channel, which often contains duplicate
 entries. apt_prepare throws out duplicates, then downloads tar files
 from the receiving buckets, unpacks them, and validates them. It
 makes an entry for each valid bag in NSQ's store_topic. Invalid bags
 are recorded in Fluctus' Processed Items list as invalid. apt_prepare
 deletes the original tar file from the local file system when it
 finishes, but it leaves the untarred files for apt_store to work
 with.

### apt_record - Record Items in Fluctus

*apps/apt_record* reads from NSQ's metadata_channel, which contains
 information about where all of a bag's files have been stored. It
 records this data in Fluctus (Fedora), creating or updating the
 Intellectual Object, the Generic Files and Premis Events. apt_record
 puts successfully ingested items into NSQ's cleanup_topic.

### apt_replicate - Copy Ingested Files To Oregon

*apps/apt_replicate* copies items from the preservation bucket in
 Virginia to the replication bucket in Oregon.

### apt_restore - Restore Intellectual Objects

*apps/apt_restore* reassembles Intellectual Objects into APTrust bags
 and copies them to the restoration bucket of the partner who owns the
 object.

### apt_retry - Re-attempt Recording in Fluctus

*apps/apt_retry* is a manually-run app that uses JSON data dumped from
 the trouble queue to re-record ingest data in Fluctus. It can create
 complete, consistent records in Fluctus about any ingested bag whose
 Fluctus data was recorded incompletely or not at all. This tool is
 rarely needed. A typical use case occurs when a bag exposes a new bug
 in Fluctus that prevents proper recording of metadata. Once the bug
 is fixed, the object's metadata can be rebuilt in Fluctus from the
 JSON data in the trouble queue.

### apt_store - Store Ingested Files in the Preservation Bucket

*apps/apt_store* reads from the store_channel and stores the generic
 files unpacked by apt_prepare in the S3 preservation bucket (aka
 long-term storage). If it's not able to store all the files in S3, it
 records an error in Fluctus' Processed Items list. Otherwise, it
 creates an entry in NSQ's metadata_topic so that data about the bag
 can be recorded in Fluctus. apt_store deletes all of the untarred
 contents of the bag from the local file system when it finishes.

### apt_trouble - Record Information About Failed Ingest

*apps/apt_trouble* dumps information about failed ingest attempts into
 JSON files inside the log directory of the Go server. This data is
 useful for diagnosing bugs and for the apt_retry utility described
 above.

### Bucket Reader

*apps/bucket_reader* runs as a cron job on the Go utility server. It
 scans all of the partner intake buckets, then checks the Processed
 Items list in Fluctus to see which of these items have not yet
 started the ingest process. It adds unprocessed tar files to the
 prepare_topic in NSQ.

### Downloader

*apps/downloader* is a simple command-line utility for downloading
 files from any APTrust bucket to your local machine. You must have
 the proper APTrust AWS keys set in your environment to use this.

### Cleanup Reader

*apps/cleanup_reader* is a cron job that gathers information from
 Fluctus about what bags have been ingested recently. It adds
 successfully-ingested bags to NSQ's bag_delete topic, so that
 apt_bag_delete can remove them from the receiving buckets.

### Fixity Reader

*apps/fixity_reader* is a cron job that checks Fluctus once a day for
 files that need a fixity check. It copies information about these
 files into NSQ's fixity_topic.

### Multiclean

*apps/multiclean* finds and deletes fragments of failed multi-part S3
 uploads. We run this periodically to avoid having to pay S3 storage
 charges on inaccessible file fragments.

### Read Test

*apps/readtest* reads an APTrust bag from the local file system and
 prints out information about what's in it (files, tags, etc.) This
 can be useful for examining bags and diagnosing problems. See also
 the partner app called apt_validate.

### Request Reader

*apps/request_reader* runs as a cron job on the Go utility server. It
 checks the Processed Items list in Fluctus for 1) intellectual
 objects that users want to restore, and 2) generic files that users
 want to delete. It puts these items into NSQ's restore_topic and
 delete_topic.

### Restoration

*apps/apt_restore* reads from NSQ's restore_channel and restores
 intellectual objects. It fetches all of the object's generic files
 from the preservation bucket, reassembles them into a bag (a tar
 file), and puts them into the owning institution's restoration
 bucket. (If the CustomRestoreBucket option in config.json is set to
 some other bucket name, the object will be restored to that bucket.)
 Multi-part bags will not be divided into the same units as the
 original upload, but the entire bag, once reassembled from the parts
 will be complete. (E.g. A bag may have been uploaded in 100 parts,
 and it may be restored in 20 parts. None of the restored parts will
 match the original parts, but the restored whole will match the
 original whole.)

## APTrust Partner Apps

The partner-apps directory contains apps intended for use by APTrust
partners. Note that these apps can access only the partner's own
receiving and restoration buckets. These apps require a config file,
which is described by providing the -h flag to any of the apps.

The apps include the following:

### apt_delete

*partner-apps/apt_delete* enables partners to download restored bags
 from their restoration buckets.

### apt_download

*partner-apps/apt_download* enables partners to download restored bags
 from their restoration buckets.

### apt_list

*partner-apps/apt_list* enables partners to list the contents of their
 receiving and restoration buckets.

### apt_upload

*partner-apps/apt_upload* enables partners to upload bags to their
 receiving buckets for ingest.

### apt_validate

*partner-apps/apt_validate* enables partners to validate bags (tarred
 or untarred) before uploading them for ingest.
