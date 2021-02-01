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
 * sqlite3 (`apt-get install libsqlite3-dev`)
## Building

Run `bazel build //dataflow`.

## Testing

To run the tests, run `bazel test //dataflow:dataflow-tests`.

For debugging, you may want to use the `--config asan` option, which enables C++
sanitizers and provides more helpful output on memory errors (e.g., segfaults).

## Code style

We follow [the Google C++ style guide](https://google.github.io/styleguide/cppguide.html);
please run `clang-format` on your code before pushing.



## IDE Setup
### Mac OS X
download VS Code (<https://code.visualstudio.com/docs/?dv=osx>), install there bazel and C++ plugin. Install also via brew following dependencies.
```
brew install bazel buildifier
```