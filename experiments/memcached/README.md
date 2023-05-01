# Memcached (memory)

This experiment gives us the expected memory cost for a Memcached setup that caches
the results of complex/expensive lobsters queries against a pre-populated lobsters
database.

This setup caches only the queries that a reasonable developer would cached if using
MariaDB/mysql with lobsters (e.g. joins, aggregates, and order by).

## Running the experiment
To run, use `bazel run //memcached:memcached --database=(ip of MariaDB database)`.

The experiment expects that a memcached server is running on `localhost:11211`,
and a Mariadb server is listening on `<database>:3303` with username `k9db` and
password `password`.

Finally, the experiment expects the lobsters harness has been primed and run against
the mariadb server, and expects that server to have a `lobsters` database with tables
and data.
