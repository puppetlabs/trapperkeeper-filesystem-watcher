# trapperkeeper-filesystem-watcher

This library includes a [Trapperkeeper](https://github.com/puppetlabs/trapperkeeper)
service which provides an API for watching paths on the filesystem for changes
and doing something when they change.

## Usage

Add the following dependency to your project.clj file:

[![Clojars Project](https://img.shields.io/clojars/v/puppetlabs/trapperkeeper-filesystem-watcher.svg)](https://clojars.org/puppetlabs/trapperkeeper-filesystem-watcher)

## Warning

There is a [bug](https://bugs.openjdk.java.net/browse/JDK-8145981) in most
released versions of OpenJDK on Linux that affect the behavior of this library.

#### [tl;dr](http://blog.omega-prime.co.uk/?p=161)
Registering new watch paths while receiving incoming events can cause the event
path to be misreported.

We encourage users to register all watch paths at start up prior to any
expected events occuring. Currently only recursive file watching is supported,
this means we will register any directory created by the user within an
existing watch path. Thus creating a directory and then immediately creating
a file or directory within the new directory will often trigger this issue.

See the above linked ticket and blog post for comprehensive info, as well as
[TK-387](https://tickets.puppetlabs.com/browse/TK-387) for our discussion
around this topic.

## Development

[![Build Status](https://travis-ci.org/puppetlabs/trapperkeeper-filesystem-watcher.svg?branch=master)](https://travis-ci.org/puppetlabs/trapperkeeper-filesystem-watcher)

The service implementation in this repository is based on java.nio's
[`WatchService`](https://docs.oracle.com/javase/8/docs/api/java/nio/file/WatchService.html).
The default implementation of that interface varies drastically in behavior
between certain platforms, especially Mac OSX and Linux.  The Trapperkeeper
service protocol in this repository is consciously written in a
platform-agnostic way.  When working on this code, it is often a good idea to
run the tests on multiple platforms as part of development - say, if you write
code on a Mac, run the tests on a Linux VM as well.  This is can save you the
pain of finding out that things don't work as expected due to platform-specific
differences in the implementation of the JDK's `WatchService` once your new
code gets into CI.

## Maintainence

### Maintainers
* Kevin Corcoran <kevin.corcoran@puppet.com>
* Matthaus Owens <matthaus@puppet.com>


## Tickets

https://tickets.puppetlabs.com/browse/TK
