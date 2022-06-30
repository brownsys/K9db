# Memcached (memory)

This experiment gives us the expected memory for two memcached setup with **lobsters**.

1. **ALL_QUERIES**: This setup caches the same queries that pelton uses views for.
2. **REAL_QUERIES**: This setup caches only the queries that a reasonable developer would cached if using mysql with lobsters (joins, aggregates, and order by).

## Running the experiment

The bazel target is `//memcached:memcached`.

Running takes no command line arguments.

To change the experiment setup, change the `#define QUERIES <...>` in `main.cc` to
either `REAL_QUERIES` or `ALL_QUERIES`.

The experiment expects that a memcached server is running on `localhost:11211`,
and a Mariadb server is listening on `localhost:3303` with username `pelton` and password
`password`. In case the server is listening on another interface (e.g. `127.0.0.1`),
the environment variable `LOCAL_IP` should be set to that interface.

Finally, the experiment expected the lobsters harness has been primed and run against
the mariadb server, and expects that server to have a `lobsters` database with tables
and data.
