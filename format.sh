#!/bin/bash
# Formats source files according to Google's style guide. Requires clang-format.

# Only files with these extensions will be formatted by clang-format.
CLANG_FORMAT_EXTENSIONS="cc|h|proto|inc"

# Display clang-format version.
clang-format --version

# Run clang-format.
find . -not -path "./third_party/**" \
       -not -path "./.git/**" \
       -not -path "./experiments/lobsters/noria/**" \
  | egrep "\.(${CLANG_FORMAT_EXTENSIONS})\$" \
  | xargs clang-format --verbose -style=google -i

# Run buildifier (Bazel file formatter).
bazel run //:buildifier

# Run google-java-format.
bazel build :google_java_format_binary
bazel run //:format_java
