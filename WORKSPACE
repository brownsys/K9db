workspace(name = "pelton")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Abseil C++ Libraries
http_archive(
  name = "com_google_absl",
  urls = ["https://github.com/abseil/abseil-cpp/archive/20200225.2.zip"],
  strip_prefix = "abseil-cpp-20200225.2",
  sha256 = "f342aac71a62861ac784cadb8127d5a42c6c61ab1cd07f00aef05f2cc4988c42",
)

# Google Test
http_archive(
  name = "gtest",
  urls = ["https://github.com/google/googletest/archive/release-1.10.0.tar.gz"],
  strip_prefix = "googletest-release-1.10.0",
  sha256 = "9dc9157a9a1551ec7a7e43daea9a694a0bb5fb8bec81235d8a1e6ef64c716dcb",
)

# C++ rules for Bazel.
http_archive(
  name = "rules_cc",
  urls = ["https://github.com/bazelbuild/rules_cc/archive/9e10b8a6db775b1ecd358d8ddd3dab379a2c29a5.zip"],
  strip_prefix = "rules_cc-9e10b8a6db775b1ecd358d8ddd3dab379a2c29a5",
  sha256 = "954b7a3efc8752da957ae193a13b9133da227bdacf5ceb111f2e11264f7e8c95",
)
