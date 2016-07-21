# trapperkeeper-filesystem-watcher

This library includes a [Trapperkeeper](https://github.com/puppetlabs/trapperkeeper)
service which provides an API for watching paths on the filesystem for changes
and doing something when they change.

## Usage

Add the following dependency to your project.clj file:

[![Clojars Project](https://img.shields.io/clojars/v/puppetlabs/trapperkeeper-filesystem-watcher.svg)](https://clojars.org/puppetlabs/trapperkeeper-filesystem-watcher)

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
