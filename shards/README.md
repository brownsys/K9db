# Pelton Sharded DB

This directory contains source code for the underlying data store used in
Pelton.

The database is built on top of sqlite3. This source code implements an
intermediate layer / adapter, that sits in between pelton (and host
applications) on one hand, and sqlite3 on the other hand.

However, instead of using a single sqlite3 database for all the data, our
intermediate layer rewrites schema as well as data insertions and queries, so
they are compatible with our internal sharded DB organization.

Every "user" is given an independent shard, respresented as a standalone "mini"
sqlite3 database. This shard contains exactly that user's data. A "user" is
identified as a row in a table containing some PII columns. The content of a
user shard (e.g. tables and their data) are automatically discovered by our
implementation by analyzing the foreign key relations in the original schema.

A shard contains some subset of the tables that exists in the original schema.
Furthermore, such tables may be modified (e.g. have some columns dropped) and
may have their names modified to avoid duplication. The content of a logical
table from the original schema may span several shards (vertically). Regardless,
our intermediate layer provides an interface that matches the original unsharded
schema, which constitutes a contract between host application and our layer, and
all sharding and schema manipulation is considered hidden and internal
implementation details.

## Components

*shards.h:* a c++ API for host applications to use to interact with our adapter,
it mimics the sqlite3 c++ API.

*cli.cc:* a command line interace that mimics the command line interface of
sqlite3, built around shards.h|cc.

## Building

From the root directory of pelton, you can build via bazel by running:
```bash
bazel build //shards/...
```

## Dependencies

All most all dependencies are managed and built by bazel, with one exception.

This code requires that the sqlite3 c++ API be available as a system dependency,
such that the linker is able to resolve *-lsqlite3*. This API usually comes
built-in by default with sqlite3, depending on your system and configurations.
You can find instructions for downloading the API at
https://www.sqlite.org/quickstart.html .

## Running and Examples.

After building, you can run the CLI as follows:
```bash
# From root of Pelton
./bazel-bin/shards/cli <db directory>
```

Make sure that <db directory> points to the directory where you want the DB
files to be created. You must create this directory yourself if it does not
exist.

Running using the above command opens an interactive SQLite-like REPL, where
you can input commands and view outputs interactively.

Alternatively, you can feed in SQL statements for the CLI binary to execute from
a file via stdin.
```bash
# From root of pelton
./bazel-bin/shards/cli <db directory> < <sql file>
# For example
./bazel-bin/shards/cli test < shards/examples/social_chat.sql
```

**NOTE:** The CLI assumes that SQL statements span a single line by default, to
have a multi-line statement, place \ before every line break to escape it. Also,
make sure that the statement ends with ; followed by a line break directly.
