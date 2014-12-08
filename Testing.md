# Testing bagman'

## Unit Tests and Simple Integration Tests

You can run basic unit tests by changing into the bagman/bagman
directory and running this:

```
	go test
```

Those unit tests conver only the most basic units, and do not attempt
to access any outside services.

If the environment variables AWS_ACCESS_KEY_ID and
AWS_SECRET_ACCESS_KEY are set, the "go test" suite will attempt to run
all S3-related tests. Those tests should pass if your AWS credentials
are correct, and your config files are correct. (More on config files
and testing below.) Some of the S3 tests require you to have APTrust
credentials, since they access buckets that APTrust partners cannot
access (such as the test.edu buckets).

If you specify a FluctusURL in your config.json file, and there
appears to be a Fluctus server running at that URL, "go test" will run
Fluctus integration tests as well. You will need to specify the
environment variables FLUCTUS_API_USER and FLUCTUS_API_KEY in order
for Fluctus tests to work. The Fluctus API account will need admin
privileges for all the tests to complete successfully, since testing
covers a number of admin-only API calls.

## Testing the Server Apps

The scripts directory includes three bash scripts that will let you
run end-to-end tests on all of the server apps. To run these, you must
do the following:

1. Set the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
2. Set the environment variables FLUCTUS_API_USER and FLUCTUS_API_KEY
3. Set the FluctusURL in config.json to point to a running instance of
Fluctus where your Fluctus user and API key are valid.
4. If you have access to the Fluctus instance (and you're probably
running it on localhost), run the rake task "rake fluctus:reset_data"
to clear all objects and files from your Fluctus installation. The
rake task will preserve all institutions and user accounts.

*process_items.sh* will start a local instance of NSQ and each of the
server apps required for ingest. It will then ingest all of the bags
in the S3 bucket aptrust.receiving.test.test.edu. This process will
take several minutes, and all logging output will go to both the log
files (specified in config.json) and to stderr.

You'll know that process_items is done when it stops printing out log
data. You should see a line at the bottom of the output showing how
many items succeeded and how many failed.

Error messages are printed in red on stderr. If you scroll back up
through the output, you should see that both the README file and one
bag from Virginia failed the ingest process. That is expected, since
they are not valid bags, and we want to test that processing continues
after failures.

Once the process is complete, you can review all the objects and files
in Fluctus. If you're running Fluctus locally, just go to
http://localhost:3000. Note that in most cases, you don't really need
to look at Fluctus, because the logs will show messages (in red) about
any items that failed.

Use Control-C in the terminal to shut down NSQ and all of the servers
processes that process_items.sh started.

*fixity_check.sh* starts a local NSQ server and all of the processes
required to run fixity checks. It will run checks on all of the files
you ingested when you ran process_items.sh. This process usually
completes in a minute or two, depending on the speed of your internet
connection.

If you look again at the files in the Fluctus instance you're testing
against, you will see that each generic file has an additional Premis
event called fixity_check after running fixity_check.sh. Generic files
get one md5 and one sha256 fixity check events on ingest. They get
additional fixity_check events with sha256 checksums each time you run
the fixity_check.sh script.

Again, use Control-C to shut down NSQ and all the fixity_check workers.

*restore_items.sh* restores intellectual objects AND deletes generic
files. Before you run this, go to the Fluctus instance you're testing
against and mark at least one item for restoration and at least one
for deletion.

When you run restore_items.sh, the log output to stderr (and to the
log files) will show the outcomes of all the restore and delete
operations. It should not include any failures.

Use Control-C to end restore_items.sh

These three scripts together run every app and every cron job and call
every function used in production. They provide a good sanity check
for new and refactored code, and have identified a number of bugs that
simple unit tests could not catch.

## Configuration for Testing

Each of the scripts described above uses the hard-coded config
"apd4n". You will want to create your own configuration section in
config/config.json and then use that for local testing.

To create a new config section, copy an existing section and give it a
new name. You can leave almost all of the copied config unchanged,
except for the items noted below.

These four directories should be set to paths that exist on your local
machine:

*TarDirectory* - Bags downloaded from S3 will be stored here
 temporarily during ingest.

*LogDirectory* - All of the server apps will write their logs in this
 directory. NSQ will also put some files there.

*RestoreDirectory* - Restored bags will be written to this directory.

*ReplicationDirectory* - Holds files being replicated from S3 servers
 in Virginia to Oregon.

The following settings may also need adjustment:

*MaxFileSize* - The maximum bag size, in number of bytes, to
 process. When running local tests, you don't want to accidentally
 download and process a 250GB bag. 1000000 is a suitable value for
 testing.

*SkipAlreadyProcessed* - If this is true, the scripts will not
 re-ingest bags if those bags have already been ingested and their
 ETags have not changed. You will usually want this to be false for
 local testing.

*DeleteOnSuccess* - Set this to false, so we don't delete the test
 fixtures from aptrust.receiving.test.test.edu after ingesting them.

*LogToStderr* - Set to true to print log statements to stderr in
 addition to printing to log files.

*LogLevel* - Set this to 4 to see debug output. Set it lower to see less.

*FluctusURL* - This is important! Set this to the URL of the Fluctus
 server you will test against. That's usually http://localhost:3000.

Once you have your config set up, you can use it in your local
end-to-end test by replacing "-config apd4n" with "-config
<your_config>" in the bash scripts.

*TODO: The config name "apd4n" should not be hard-coded into the bash
scripts. It should be read from the command line.*

## Testing the Partner Apps

To test the partner apps, you will need to set the environment
variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, as described
above.

Run scripts/build.sh to build all of the server apps and partner
apps. You'll notice that the partner apps are built with the
"partners" tag. That prevents the binaries from relying on some C
libraries that partners probably don't have.

After running the build script, cd into the bin directory and run the
following.

### Testing apt_download

```
    ./apt_download --config=../testdata/partner_config_download_test.conf \
    ncsu.1840.16-10.tar virginia.edu.uva-lib_2141114.tar
```

To test apt_download with md5 checksums:

```
    ./apt_download --config=../testdata/partner_config_download_test.conf --checksum=md5 \
     ncsu.1840.16-10.tar virginia.edu.uva-lib_2141114.tar
```

To test apt_download with sha256 checksums:

```
    ./apt_download --config=../testdata/partner_config_download_test.conf --checksum=sha256 \
     ncsu.1840.16-10.tar virginia.edu.uva-lib_2141114.tar
```

Please don't test apt_download with the --delete flag, unless you're
testing against a file you put into aptrust.receiving.test.test.edu,
or if you will replace the file you delete.

### Testing apt_list

List up to 10 items from the receiving bucket:

```
    ./apt_list -config=../testdata/partner_config_integration_test.conf --bucket=receiving --limit=10
```

List up to 10 items from the restoration bucket:

```
    ./apt_list -config=../testdata/partner_config_download_test.conf --bucket=restoration --limit=10
```

### Testing apt_upload

```
    ./apt_upload -config=../testdata/partner_config_integration_test.conf ../testdata/*.tar
```

## Testing apt_validate

```
./apt_validate ../testdata/*
```
