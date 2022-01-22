# Pelton

A new dataflow processor.

## Requirements
Pelton only supports Ubuntu 20.04+ currently. MacOS might work but is not continously supported.
To build, you need a working [Bazel 4.0](https://docs.bazel.build/versions/4.0.0/install.html)
installation.

Bazel will pull some dependencies automatically. However, it relies on some dependencies being
installed system-wide via your system's package manager:
install the following via your package manager:
 * GCC-11 (`apt-get install gcc-11 g++-11`)
 * mysql8 and mysqlcppconn8 (the c++ mysql connector), follow instructions here
   to install for [ubuntu](https://dev.mysql.com/doc/mysql-apt-repo-quick-guide/en/).
   Make sure to afterwards run `apt-get install libmysqlcppconn8-2 libmysqlcppconn-dev` (the
   exact version is not important as long as it is 8).
 * OpenSSL (for md5 hashing) (libssl on ubuntu), works on version 1.1 but should work
   on other versions. Install on ubuntu via `apt-get install libssl-dev openssl`
   (specifically, we need `-lcrypto, -lssl`).
 * The memcached baseline relies on these packages `flex bison libevent-dev`.

Check out [.github/workflows/ubuntutest.yml](.github/workflows/ubuntutest.yml),
specifically the dependencies section, for a scripted way to install dependencies
on ubuntu 20.

## Rust
Rust dependencies are also managed by bazel. When using Rust you can list your
dependencies as usual in a Cargo.toml file, and expose them to bazel using `raze`.
You will need to install `raze` using `cargo install cargo-raze`.

We currently have 4 rust packages in pelton:
`pelton/proxy`
`experiments/lobsters`
`experiments/memcached`
`experiments/ownCloud`

Each of these packages contain a Cargo.toml file. When first installing pelton,
you need to run `cargo raze` in each of their directories.

If adding your own rust package to pelton, you will need to structure it similarly.
Make sure to add `raze` configurations to your Cargo.toml file. You will also need
to add a WORKSPACE.bazel and a BUILD.bazel file. You can use any of the above packages
as inspiration for what that may look like.

## Docker container
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

## Building, Testing, and Running
Run `bazel build ...`.

To list all available targets, use `bazel query //...`.

To run all tests, run `bazel test ...`.

Pelton's main entry point is its mysql proxy, which you can run using `bazel run //:pelton`.
You can run this in various mode using `./run.sh [dbg, opt, valgrind, asan, tsan]`.

## TSAN and ASAN:
The JVM introduces a variety of things that look like various leaks and races. These are
all JVM internals we have no control over and are likely correct. We suppress them when
running the JVM.

You need to configure bazel to use our supression files. These files must be provided
using their absolute path. You can configure bazel by creating your own `user.bazelrc`
file in the root directory of `pelton`. The docker file comes with this file provided
in. The content of the files should include:
```
test:asan --test_env LSAN_OPTIONS=suppressions=</path/to/pelton/>.lsan_jvm_suppress.txt
test:tsan --test_env LSAN_OPTIONS=suppressions=</path/to/pelton/>.lsan_jvm_suppress.txt
test:tsan --test_env TSAN_OPTIONS=suppressions=</path/to/pelton/>.tsan_jvm_suppress.txt
```

## Code style

We follow [the Google C++ style guide](https://google.github.io/styleguide/cppguide.html);
Please run `./format.sh` prior to pushing code. Run `cpplint.py` frequently to detect
semantic variations from the style guide.

We recommend adding this as a git pre-commit hook. You can do that by creating
`.git/hooks/pre-commit` with the following content:

```bash
#!/bin/bash
# Checks if source files have linting errors prior to commit.

# Only files with these extensions will be checked for linting errors.
CLANG_FORMAT_EXTENSIONS="cc|h|proto|inc"

# Run cpplint.
output=$(find . -not -path "./third_party/**" \
       -not -path "./.git/**" \
  | egrep "\.(${CLANG_FORMAT_EXTENSIONS})\$" \
  | xargs python2.7 cpplint.py --filter=-legal 2>&1)

if [ $(grep "Total errors found: 0" <<< "$output" | wc -l) -eq 1 ]; then
  echo "No Linting Errors..."
else
  echo "C++ Linting Errors, commit reject!"
  echo "$output"
  exit 1
fi

# (Dry)-run google java format.
bazel run //:format_java -- CHECK
```
