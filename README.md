# K9db

A MySQL-compatible database for privacy compliance-by-construction.

## Overview

K9db is a MySQL-like database that provides similar capabilities to MySQL,
while providing applications with a correct-by-construction built-in mechanism
to comply with subject access requests (SARs), as required by privacy
legislations, such as GDPR.

K9db support two types of SARs: data access and data deletion. The former allows
data subjects (i.e. human users) to access a copy of data related to them, while
the later allows them to request removal of that data. Applications must handle
these two types of SARs correctly to comply with the GDPR right's to access and
erasure (the right to be forgotten).

Internally, K9db tracks association between each row of data stored in it and
users that have rights to it. K9db uses this information to automatically and
correctly handle SARs, and to also ensure that regular application queries and
updates maintain compliance (e.g. do not create dangling or orphaned data).
K9db achieves this while maintaing performance comparable to MySQL, by relying
on various design decisions and optimizations, including a new physical storage
layout organized by data subjects. Refer to our upcoming OSDI paper for details.

K9db provides an integrated and compliant in-memory cache to speed up expensive
queries. K9db automatically ensures this cache is up-to-date with respect to
SARs, as well as regular application updates. Unlike demand filled caching, such
as with Memcached, K9db's caches rely on incremental data flow processing to
keep the cache always up-to-date with no invalidations.

To use K9db, applications need to add annotations to their SQL schema to express
the ownership relationships between datasubjects and data in the various tables.
Furthermore, developers may need to combine several related operations into
a compliance transaction if these operations temporary create orphaned.

K9db runs each query as a single-statement transaction with REPEATABLE_READS
isolation. K9db enforces PK uniqueness and FK referential integrity for FKs with
ownership annotations.

K9db was previously known as Pelton.

## Requirements

First, you must clone this repo to some directory of your choosing. We will
call this `<K9DB_DIR>`. You should also initialize its git submodules.
```bash
git submodule init
git submodule update
```

After cloning, you have multiple options for building
and running K9DB:
1. By using our provided docker container (recommended for trying out K9db).
2. By installing the requirements and building K9db yourself.
3. By using our provided scripts if you are running on google cloud (recommend
   if you are trying to reproduce the results from our paper). Consult the
   experiments section for more details on this.

Currently, you *cannot* build K9db directly on MacOS. You must instead use the
docker container. If your machine has *M1/M2* processors, K9db may *not* build
even using the container.

### Docker container

You will need to install Docker on your machine. You then need to build the
docker container image. This step may take anywhere between 5-30 minutes,
depending on your system and internet connection.
```bash
cd <K9DB_DIR>  # navigate to repo root directory
docker build -t k9db/latest .  # build image and name it k9db/latest
```

After the image is built, you can start it the first time using.
This will also perform some one-time configurations needed to build k9db.
```bash
docker run --mount type=bind,source=$(pwd),target=/home/k9db --name k9db -d -p 3306:3306 -p 10001:10001 -t k9db/latest
```

You can check that the one-time configuration is done by looking at the
container logs, and looking for the message `Configuration done!`.
```bash
docker logs k9db
```

After configuration is done, you can open a terminal inside your container.
```bash
docker exec -it k9db /bin/bash
$ # now you are inside the container
$ # validate container is setup properly
$ cd /home/k9db
$ ls  # you should see the contents of this repo
$ ...
$ exit  # exit the container when you are done.  
```

You can stop the container and restart it as you wish using.
```bash
docker stop k9db
docker start k9db
```

Note: the container mounts the repo from your host filesystem rather than copies
it. You can think of this as making a symbolic link to its content. Any changes
you make to the contents of the repo inside the container will be visible in
your host filesystem and vice-versa.

We tested this using `Docker version 20.10.19, build d85ef84`.

### Installing the requirements yourself

If you do not wish to use the docker container. You can install our requirements
yourself using your favorite package manager. We built k9db on ubuntu 20 and 22,
but similar systems should also work.

Core requirements:
1. GCC-11 (e.g. `apt-get install gcc-11 g++-11`).
2. openjdk-11 (e.g. `apt-get install openjdk-11-jdk`).
3. rustc and cargo 1.56.0.
4. bazel-4.0.0 [https://docs.bazel.build/versions/4.0.0/install.html](https://docs.bazel.build/versions/4.0.0/install.html).
5. libffi-dev, libssl-dev, zlib1g-dev, libncurses5-dev.
6. cargo-raze (e.g. cargo install cargo-raze).
7. A MySQL command line client to connect and interact with k9db after running it.
   We recommend using `mariadb-client-10.6`.

After installing these requirements, ensure that they are the default binaries
on your system.

You will need to run cargo raze *once* to fetch the rust dependencies from cargo
and turn them to bazel dependencies.
```bash
mkdir -p /tmp/cargo-raze/doesnt/exist/  # may be needed for some versions of raze.
cd <K9DB_DIR>/k9db/proxy && cargo raze
cd <K9DB_DIR>/experiments/lobsters && cargo raze
cd <K9DB_DIR>/experiments/ownCloud && cargo raze
cd <K9DB_DIR>/experiments/vote && cargo raze
```

The following are not required to build or run K9DB. But they may be required
to run some of our experiments from the paper including the baselines.
1. MariaDB server with MyRocks (`mariadb-server-10.6` `mariadb-plugin-rocksdb`).
2. MariaDB C++ connector (`libmariadb3` `libmariadb-dev`).
3. flex, bison, libevent-dev.

After installing MariaDB server, you will need to configure it once to load
the MyRocks extension. You can do so using:
```bash
mariadb -u root
> # Now connected to mariadb server.
> CREATE USER 'k9db'@'%' IDENTIFIED BY 'password';
> GRANT ALL PRIVILEGES ON *.* TO 'k9db'@'%' IDENTIFIED BY 'password';
> FLUSH PRIVILEGES;
> INSTALL SONAME 'ha_rocksdb';
```

## Building, Testing, and Running

If using the docker container, open a terminal inside it and navigate to
`/home/k9db`. Otherwise, naviagte to `<k9DB_DIR>` on your system.

You can build K9db using bazel.
```bash
# This may take anywhere from 5-50 minutes, depending on your system.
bazel build :k9db
```

To run all tests, run `bazel test ...`

K9db's main entry point is its mysql proxy, which acts as a MySQL server.
You can run this using:
```bash
bazel run :k9db --config opt
```
```

You can pass multiple command line flags to K9db to configure it.
```bash
bazel run :k9db -- --db_path=[/tmp] \
                   --db_name=[k9db] \
                   --hostname=[127.0.0.1:10001] \
                   --logtostderr=([0]|1)
```

You can stop K9db at any time by using `ctrl+c`. K9db will always try to
shutdown cleanly and wait for any open clients to disconnect before shutting
down. You can start K9db again using the above command at any point, and it
will reload the database state as it was prior to shutdown. To destroy the
database, you will need to remove it from the filesystem after shutting K9db
down.
```bash
ctrl+c               # shutdown K9db.
rm -rf /tmp/k9db_*   # clear all databases in default location
bazel run :k9db      # fresh start with an empty database.
```

By default, bazel build and runs in debug mode which is suitable for
getting started with K9db. For optimal performance while reproducing our
experiments, you will need to run using optimized mode.

```bash
bazel run :k9db --config opt
```

## Tutorial

Coming soon.

## Reproducing Our Results

Coming soon.

## Limitations and Known Issues.

K9db is a prototype proof-of-concept software validating that
compliance-by-construction is practical and achievable.

Our prototype has several limitations, some of which are in the works:
1. We use determinstic encryption for both keys and values. This has leakage.
   Keys must be encrypted in a consistent way to enable consistent lookups.
   However, values can be encrypted with randomized nonces.
   We are working on several improvements to our encryption schemes.
2. We do not support SQL transactions.
3. We do not support multi-column primary keys, multi-column foreign keys, and
   foreign keys targeting a non-primary key column.
4. We do not enforce UNIQUE and NOT NULL constraints.
5. We have limited support for DATETIME.
6. We support the basic core of SQL. Using unsupported syntax or features will
   result in an unsupported error.

Furthermore, we are currently aware of two bugs of significance:
1. Our planner creates incorrect plans for cached queries where the key is
   projected out (e.g. `SELECT col2 FROM table WHERE col1 = ?`).
2. `OWNS` foreign keys forming chains of 2 or longer are sometimes mishandled
   during schema creation.

## Contributing

We welcome issues and PRs. Please check the [contributions guide](contributing.md).
