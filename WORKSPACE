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
  name = "com_google_googletest",
  urls = ["https://github.com/google/googletest/archive/10b1902d893ea8cc43c69541d70868f91af3646b.zip"],
  strip_prefix = "googletest-10b1902d893ea8cc43c69541d70868f91af3646b",
  sha256 = "7c7709af5d0c3c2514674261f9fc321b3f1099a2c57f13d0e56187d193c07e81",
)

# C++ rules for Bazel.
http_archive(
  name = "rules_cc",
  urls = ["https://github.com/bazelbuild/rules_cc/archive/9e10b8a6db775b1ecd358d8ddd3dab379a2c29a5.zip"],
  strip_prefix = "rules_cc-9e10b8a6db775b1ecd358d8ddd3dab379a2c29a5",
  sha256 = "954b7a3efc8752da957ae193a13b9133da227bdacf5ceb111f2e11264f7e8c95",
)
