# Pelton Sharded DB

This directory contains source code for the underlying data store used in
Pelton.

The database is built on top of sqlite3. This source code implements an
intermediate layer / adapter, that sits in between pelton (and host
applications) on one hand, and sqlite3 on the other hand.

Instead of using a single sqlite3 database for all the data, our
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

## Dependencies

All most all dependencies are managed and built by bazel, with one exception.

This code requires that the sqlite3 c++ API be available as a system dependency,
such that the linker is able to resolve *-lsqlite3*. This API usually comes
built-in by default with sqlite3, depending on your system and configurations.
You can find instructions for downloading the API at
https://www.sqlite.org/quickstart.html .

Depending on your system, it may be sufficient to execute this (or a similar)
command:
```bash
sudo apt-get install libsqlite3-dev
```

## Running and Examples.

The `<pelton_repo>/bin/` directory contains the source for a CLI exposing
our sharding DB as if it is an SQLite3 CLI. It also contains a sample c++
host application that uses our API to interact with a sharded DB.

Additionally, `<pelton_repo>/bin/examples` contains example SQL files that can
be run via the CLI, containing various SQL statements for schema creation,
data updating and insertion, and various queries.

You can run these examples using:
```bash
# From the root of Pelton
bazel build //bin/...

# Run sample c++ host application
rm -rf db-dir && mkdir db-dir
./bazel-bin/bin/example db-dir

# Or run the CLI
rm -rf cli-db-dir && mkdir cli-db-dir
./bazel-bin/bin/cli cli-db-dir

# Or run the CLI with the provided examples
rm -rf social-db && mkdir social-db
./bazel-bin/bin/cli social-db < bin/examples/social_chat.sql
./bazel-bin/bin/cli social-db < bin/examples/social_chat.cntd.sql
```

**NOTE:** The CLI assumes that SQL statements span a single line by default, to
have a multi-line statement, place \ before every line break to escape it. Also,
make sure that the statement ends with ; followed by a line break directly.
