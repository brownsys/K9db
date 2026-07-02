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

## Installing, Building, and Running K9db

The requirements for building and running K9db are listed in our [wiki](https://github.com/brownsys/K9db/wiki/Requirements).

You can either [install our dependencies yourself](https://github.com/brownsys/K9db/wiki/Requirements%3A-Ubuntu-and-similar-distros)
or use our [provided Docker container](https://github.com/brownsys/K9db/wiki/Requirements%3A-Using-Docker) (see the section below).

You can then use bazel to [build and run K9db](https://github.com/brownsys/K9db/wiki/Building,-Testing,-and-Running).

K9db does not currently build on Mac machines with M1/M2 processors, even when using the Docker container.

## Using Docker

The repository provides two Docker images.

### Running K9db (release image)

`Dockerfile.release` packages K9db into a small self-contained image: an
intermediate stage builds K9db from source, and only the compiled server and
its runtime dependencies are shipped in the final image.

```bash
docker build -f Dockerfile.release -t k9db .
docker run -d --name k9db -p 10001:10001 -v k9db-data:/var/lib/k9db k9db
```

K9db then accepts MySQL-compatible connections on port 10001:

```bash
mysql --host=127.0.0.1 --port=10001
```

The database files are stored in the `k9db-data` volume and thus persist
across containers (e.g. when upgrading the image). Arguments passed after the
image name replace the default flags
(`--hostname=0.0.0.0:10001 --db_path=/var/lib/k9db/`).

### Developing K9db (dev image)

`Dockerfile.dev` contains everything needed to build K9db from source, run the
tests, and run the experiments (including the mariadb baselines and plotting).
The repository is bind-mounted into the container, so you can edit the code on
the host and build inside the container:

```bash
docker build -f Dockerfile.dev -t k9db/dev .
docker run --mount type=bind,source=$(pwd),target=/home/k9db --name k9db-dev -d -p 3306:3306 -p 10001:10001 -t k9db/dev
docker exec -it k9db-dev /bin/bash
# Inside the container:
cd /home/k9db && bazel build ... && bazel test ...
```

### Re-vendoring rust dependencies (devs)

The bazel BUILD files for the rust (cargo) dependencies of the proxy and of
the experiments are generated with cargo-raze and vendored (checked in) under
`**/cargo/`, so building K9db does not require cargo-raze. If you change rust
dependencies (any of the `Cargo.toml` files), re-vendor them by running the
following inside the dev container from the repo root:

```bash
./vendor-rust.sh
```

Then commit and push the regenerated files (when the repo is bind-mounted
into the container, the regenerated files are owned by root on the host, so
you may need to `chown` them first).

## Tutorial

Checkout our wiki for how to [run and connect to a K9db server](https://github.com/brownsys/K9db/wiki/Tutorial),
as well as for a tutorial on our [basic operations](https://github.com/brownsys/K9db/wiki/Basic-Operations).

Our wiki also covers more complex scenarios and features, including [shared data](https://github.com/brownsys/K9db/wiki/Shared-Data),
[accessorship](https://github.com/brownsys/K9db/wiki/Ownership-vs-Access), and [dynamic/variable ownership](https://github.com/brownsys/K9db/wiki/Variable-Ownership).

## Reproducing our OSDI'23 results.

You can find the commit corresponding to our OSDI 2023 paper and artifact
[here](https://github.com/brownsys/K9db/releases/tag/osdi2023).


Checkout our wiki for [how to setup and run the experiments](https://github.com/brownsys/K9db/wiki/Experiments/c720b085ca34edc16246f296991e623a29933f9b) from our OSDI paper.
The corresponding wiki commit hash is `c720b085ca34edc16246f296991e623a29933f9b`.

## Limitations and Known Issues.

K9db is a prototype proof-of-concept software validating that
compliance-by-construction is practical and achievable.

We have several limitations, including a couple of known bugs, and various enhancements in the works.
These are listed in our [wiki](https://github.com/brownsys/K9db/wiki/Limitations-and-Known-Issues), and in GitHub issues.
Feel free to open additional issues for feature requests or new bugs.

## Contributing

We welcome issues and PRs. Please check the [contributions guide](contributing.md).
