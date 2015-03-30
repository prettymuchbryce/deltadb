deltadb
====

> A proof-of-concept time-series database

![delta](https://s3-us-west-1.amazonaws.com/prettymuchbryce/delta.png)

## What is it ?

deltadb is a proof-of-concept document store. Deltadb maintains history of every action which mutates the state of the database. Deltadb allows for examining the database at a previous state or rolling back to a previous state in a fashion similar to a git repository. Deltadb maintains the concept of a single-branch immutable history tree where even rollbacks themselves are nodes in this tree. Deltadb also experiments with the idea of storing `messages` and `authors` for each mutable change in the database in order to provide more insight into how and why a particular operation in the past was performed.

## Why is it ?

I thought it would be interesting to build a database wherein saving past state was not optional, and was implemented at the database level rather than the application level. This could be a useful safety net for building systems which have a requirement of maintaining an accurate history of certain changes. Furthermore the concept of `messages`, and `authors` might make it easier to pin down which particular system or user was responsible for making any one particular change. For example, if you found a problematic write in the database you could see that the `author` was some particular service in your application stack.

## Status

This is first and foremost a learning project for me to learn more about databases and Go. You should not use it for anything important.

## Usage

1. Install [go](http://golang.org/)
2. `go get github.com/prettymuchbryce/deltadb`
3. Configure the `locations` directory in deltad.conf (This is where the data will live)
4. `make run`
5. (in a new terminal) `make cli`
6. in the cli `connect;`
7. Type `help;` to see a list of all commands

### TODO
- Scaling past 1 core
- Scaling past 1 machine
- Implementing locking for concurrency
- Reducing or eliminating the memory footprint of the appendix file (bloom filter or b-tree)
- Users / Auth
- Encrypted data
- go client library
- Per-document and per-collection rollbacks
