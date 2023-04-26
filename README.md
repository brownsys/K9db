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

The requirements for building and running K9db are listed in our [wiki](./wiki/Requirements).

You can either [install our dependencies yourself](./wiki/Requirements%3A-Ubuntu-and-similar-distros)
or use our [provided Docker container](./wiki/Requirements%3A-Using-Docker).

You can then use bazel to [build and run K9db](./wiki/Building,-Testing,-and-Running).

K9db does not currently build on Mac machines with M1/M2 processors, even when using the Docker container.

## Tutorial

Checkout our wiki for how to [run and connect to a K9db server](./wiki/Tutorial),
as well as for a tutorial on our [basic operations](./wiki/Basic-Operations).

Tutorials for more complex features and more complex scenarios are coming soon.

## Reproducing Our Results

Coming soon.

## Limitations and Known Issues.

K9db is a prototype proof-of-concept software validating that
compliance-by-construction is practical and achievable.

We have several limitations, including a couple of known bugs, and various enhancements in the works.
These are listed in our [wiki](./wiki/Limitations-and-Known-Issues), and in GitHub issues.
Feel free to open additional issues for feature requests or new bugs.

## Contributing

We welcome issues and PRs. Please check the [contributions guide](contributing.md).
