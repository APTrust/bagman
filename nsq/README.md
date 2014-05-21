# NSQ

NSQ serves as the task queue for bagman.

## Installation

Download the latest build from http://nsq.io/deployment/installing.html.

Note that the builds are for 64-bit Mac and Linux platforms only. If
you want a 32-bit build, or a build for another platform, you will
have to build it yourself. The page above includes instructions for
source builds.

After downloading and extracting, copy all of the binaries to a
directory in your PATH.

## Quick Start

The primary quick start instructions are at
http://nsq.io/overview/quick_start.html.

You can also run the three main nsq services using service.go, like so:

```
    go run service.go -config=$GOPATH/src/github.com/APTrust/bagman/nsq/nsqd.apd4n.config
```

This requires a config file. You'll find examples of config files in
this directory. The naming convention is nsqd.<name>.config.

## Config Files

To be determined... See the available options for nsqd here:

http://nsq.io/components/nsqd.html

And note that we want to set -mem-queue-size to zero, so that all
queue items are persisted to disk. We should also set -data-path to
something sensible, so we can choose a disk with sufficient space, and
so we know where to find the queue items in the event of a crash.

In addition, nsqd should be configured to listen only on the internal
AWS interface (the 10.x.x.x subnet), and we should prevent systems
outside of APTrust from accessing the queues.

Config options for nsqlookupd are here:

http://nsq.io/components/nsqlookupd.html

The nsqlookupd service should only accept requests from other APTrust
hosts.

And for nsqadmin are here:

http://nsq.io/components/nsqadmin.html

The nsqadmin interface should accept connections from outside AWS, so
that APTrust developers can access it. However, access should be
restricted to authorized users.