# Contributing

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

## Using TSAN and ASAN

K9db invokes the JVM for some of our query planning code, which is written using
Apache Calcite. The JVM introduces a variety of things that look like various
leaks and races. These are all JVM internals we have no control over and are
likely correct. We suppress them when running with sanitizers.

You need to configure bazel to use our supression files. These files must be
provided using their absolute path. The docker container configures this
automatically. If you are not using the docker container, you can configure
bazel to use the suppression while running tests by creating your
own `user.bazelrc` file in `<K9DB_DIR>` with the following content:
```bash
test:asan --test_env LSAN_OPTIONS=suppressions=<K9DB_DIR>/.lsan_jvm_suppress.txt
test:tsan --test_env LSAN_OPTIONS=suppressions=<K9DB_DIR>/.lsan_jvm_suppress.txt
test:tsan --test_env TSAN_OPTIONS=suppressions=<K9DB_DIR>/.tsan_jvm_suppress.txt
```

If you are running K9db or any of its rules using `bazel run`, you will need
to provide these suppressions yourself in the run enviroment. For example:
```bash
LSAN_OPTIONS=suppressions=/home/bab/Documents/research/k9db/.lsan_jvm_suppress.txt \
bazel run //:k9db --config asan
```
## Disabling Encryption [not recommended]

K9db encrypts data at rest by default.
This can be turned off by passing `--encryption=off`
to any bazel command. For example:
```bash
bazel test ... --encryption=off
bazel run //:pelton --encryption=off
```

Warning: disabling encryption violates compliance with GDPR.

## Code Structure

This repo is organized as below.

```
──K9db
  ├── examples: contains example SQL schemas and queries.
  ├── experiments
  │   ├── explain
  │   │   └── shuup schemas for §8.3 in the paper.
  │   ├── GDPRbench
  │   │   └── fork of GDPRBench with a K9db compatible driver.
  │   ├── lobsters
  │   │   └── lobsters harness for figures 8 and 9 in the paper.
  │   ├── memcached
  │   │   └── harness to compare our lobsters memory overhead to memcached.
  │   ├── ownCloud
  │   │   └── ownCloud harness for figures 11 and 12 in the paper.
  │   └── vote
  │   │   └── experiment harness for figure 10 in the paper.
  ├── k9db (our source code)
  │   ├── dataflow
  │   │   └── our integrated in-memory cache and dataflow.
  │   ├── planner
  │   │   └── for planning dataflows for complex queries.
  │   ├── proxy
  │   │   └── MySQL proxy
  │   ├── shards
  │   │   └── SQL operations execution
  │   ├── sql
  │   │   └── our storage engine
  ├── scripts 
  │   └── helper scripts for running our experiments on gcloud.
  └── tests
      └── end-to-end tests
```

## MySQL Proxy

The proxy acts as a MySQL database while interfacing with K9db.
Queries are converted to C compatible types and sent to K9db.
Responses are converted to rust compatible types and returned.
Any application that uses a MySQL backend may connect to this proxy instead of a traditional database.

The flow of the proxy is essentially as follows:
Application <=> Mysql Proxy <=> FFI <=> K9db API (C++)

You can find more details in this
![design diagram](https://user-images.githubusercontent.com/47846691/142964390-7dc575e4-300e-4388-8006-1070fa82ad5d.png).
