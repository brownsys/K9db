# Pelton

A new dataflow processor.

## Requirements
Pelton only supports Ubuntu 20.04+ currently. MacOS might work but is not natively supported.
To build, you need a working [Bazel 4.0](https://docs.bazel.build/versions/4.0.0/install.html)
installation.

Bazel will pull some dependencies automatically. If not or you get errors, you may need to
install the following via your package manager:
 * [Abseil](https://abseil.io/) (v2020-02-25)
 * [googletest](https://github.com/google/googletest) (v1.10)
 * GCC-11 (`apt-get install gcc-11 g++-11`)
 * mysql8 and mysqlcppconn8 (the c++ mysql connector), follow instructions here
   to install for [ubuntu](https://dev.mysql.com/doc/mysql-apt-repo-quick-guide/en/).
   Make sure to afterwards run `apt-get install libmysqlcppconn8-2 libmysqlcppconn-dev` (the
   exact version is not important as long as it is 8).
 * OpenSSL (for md5 hashing) (libssl on ubuntu), works on version 1.1 but should work
   on other versions. Install on ubuntu via `apt-get install libssl-dev openssl`

You also need to make sure that the mysql server is running `sudo systemctl start mysql.service`.

Check out [.github/workflows/ubuntutest.yml](.github/workflows/ubuntutest.yml),
specifically the dependencies section, for a scripted way to install dependencies
on ubuntu 20.

# Docker container
We provide a docker container with all the dependencies.

To build the docker container, run
```bash
docker build -t pelton/latest .
```

The docker container runs mariadb as a daemon. You should run the docker
container in the background and then attach your shell to it or execute commands
in it using docker exec:
```bash
# Run the container and name it pelton-testing
# Mount pelton into the container.
docker run --mount type=bind,source=$(pwd),target=/home/pelton --name pelton-testing -d -t pelton/latest

# Wait a few seconds for mysqld to run successfully inside the container

# Run desired commands
docker exec -it pelton-testing /bin/bash
```

## Building

Run `bazel build ...`.

To list all available targets, use `bazel query //...`.

## Testing

To run all tests, run `bazel test ...`.

For debugging, you may want to use the `--config asan` option, which enables C++
sanitizers and provides more helpful output on memory errors (e.g., segfaults).

## Code style

We follow [the Google C++ style guide](https://google.github.io/styleguide/cppguide.html);
please run `clang-format` on your code before pushing.

# Build tooling

## Rust

Rust dependencies are also managed by bazel. When using Rust you can list your
dependencies as usual in a Cargo.toml file. Then you need to register it with
the workspace by adding the path to your Rust component to the Cargo.toml file
in the root directory. Lastly you run `cargo raze` in the root directory, which
will automatically generate bazel build rules for your Rust dependencies.

You will also need to define a BUILD.bazel file for your rust project. You can
use `mysql_proxy/BUILD.bazel` as inspiration of how that should look like.