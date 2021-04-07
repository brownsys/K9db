# Pelton

A new dataflow processor.

## Requirements
Pelton only supports Ubuntu 20.04+ currently. MacOS might work but is not natively supported.
To build, you need a working [Bazel](https://docs.bazel.build/versions/3.5.0/install.html)
installation.

Bazel will pull some dependencies automatically. If not or you get errors, you may need to
install the following via your package manager:
 * [Abseil](https://abseil.io/) (v2020-02-25)
 * [googletest](https://github.com/google/googletest) (v1.10)
 * GCC-9 (`apt-get install gcc-9 g++-9`)
 * mysql8 and mysqlcppconn8 (the c++ mysql connector), follow instructions here
   to install for [ubuntu](https://dev.mysql.com/doc/mysql-apt-repo-quick-guide/en/).
   Make sure to afterwards run `apt-get install libmysqlcppconn8-2 libmysqlcppconn-dev` (the
   exact version is not important as long as it is 8).
## Building

Run `bazel build //dataflow`.

To list all available targets, use `bazel query //...`.

## Testing

To run all tests, run `bazel test ...`.

For debugging, you may want to use the `--config asan` option, which enables C++
sanitizers and provides more helpful output on memory errors (e.g., segfaults).

## Code style

We follow [the Google C++ style guide](https://google.github.io/styleguide/cppguide.html);
please run `clang-format` on your code before pushing.

