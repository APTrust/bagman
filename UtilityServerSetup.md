# Utility Server

Here's how to set up a utility server to test out bagman. These
instructions descibe the setup process for a small server, which is
good for test runs because 1) it's cheap, and 2) it comes with 160GB
of attached storage, which is handy because bagman will be working
with some fairly large files.

Assuming you've already spun up an EC2 small running a 32-bit Ubuntu
12.04 or 14.04 AMI, follow the instructions below.

Install an editor if you don't like working with vim. If you like vim,
just kick back and relax.

```
    sudo apt-get install emacs23-nox golang-mode
```

The "go get" command uses git, bazaar, and mercurial to download
packages.  Install those, along with libmagic-dev, which we need
for determining file mime types. We'll need gcc to build the
magicmime go package later.

```
        sudo apt-get install git bzr mercurial
        sudo apt-get install gcc libmagic-dev
```

Install go, but don't use apt-get. That installs go 1.0, which blows
up when you try to run go get. Do an manual install instead. Note that
the command below is for a 32-bit Linux OS, which is all you can run
on an EC2 small instance. If you're running on a 64-bit instance, use
the package go1.2.1.linux-amd64.tar.gz instead.

```
    curl "https://go.googlecode.com/files/go1.2.1.linux-386.tar.gz" > go1.2.1.linux-386.tar.gz
    sudo tar -C /usr/local -xzf go1.2.1.linux-386.tar.gz
```

Create a GOHOME directory:

```
    mkdir ~/go
```

Create ~/.bash_profile with the following:

```
    export EDITOR=emacs
    export AWS_ACCESS_KEY_ID="Our access key id"
    export AWS_SECRET_ACCESS_KEY="Our secret key"
    export GOPATH=$HOME/go
    export PATH="$GOPATH/bin:$PATH:/usr/local/go/bin"
```

Run this to load the bash profile:

```
    source ~/.bash_profile
```

Now if you run this:

```
    go version
```

You should see this output:

```
    go version go1.2.1 linux/386
```

Make a directory to hold the tar/bag files we're going to
download. Most EC2 instances include a volume attached at /mnt. A
small instance's /mnt has 160GB of disk space.

```
    sudo mkdir /mnt/apt_data
    sudo mkdir /mnt/apt_logs
    sudo chown ubuntu.ubuntu /mnt/apt_data/
    sudo chown ubuntu.ubuntu /mnt/apt_logs/
```

Install the go packages we'll need:

```
    go get launchpad.net/goamz
    go get github.com/nu7hatch/gouuid
    go get github.com/rakyll/magicmime
    go get github.com/APTrust/bagins
    go get github.com/APTrust/bagman
```

You can also copy a version of bagman from your local machine, like
so:

```
    scp -r ~/go/src/github.com/APTrust/bagman/ ubuntu@apt-util:go/src/github.com/APTrust/
```

Note that this assumes you have an entry in your local ~/.ssh/config
like this:

```
    Host apt-util
        User ubuntu
        Port 22
        IdentityFile ~/.ssh/MyPrivateKey.pem
        TCPKeepAlive yes
        IdentitiesOnly yes
        HostName ec2-54-85-73-179.compute-1.amazonaws.com
```

And remember to check the AWS console for the *actual* public IP
address of your EC2 instance.

Bagman has a ‘dev’ configuration and a ‘test’ configuration. Use the
dev configuration when running on your local machine. This will
retrieve only smaller files (<200k) from S3. Use the ‘test’
configuration when running on an EC2 instance. That will retrieve all
files in all the S3 buckets.

You can now run the test code like this:

```
    cd ~/go/src/github.com/APTrust/apmanager/test
    go run test.go -config=test
```

Bagman will print out some information about its configuration and it
will tell you where it’s writing its log files. You can tail the logs
if you want to watch its progress.

You'll see a json log, which is thorough but not very readable, and
a messages log, which prints simple info and error messages. That
one will give you an overview of what the program is actually
doing.

The JSON log, by the way, records the actual results of every
significant operation the program performed during its run. Someday
soon, there will be code that uses the JSON log to report on results,
and possibly to reconstruct any data that did not make it into
fluctus.

After running the test program, make sure it cleaned up after itself:

```
    ls /mnt/apt_data/
```

That listing should show an empty directory.

The log should be in /mnt/apt_logs. You will have to delete that
manually.
