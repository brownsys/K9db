load("@rules_cc//cc:defs.bzl", "cc_library")
load("//build:options.bzl", "PELTON_DEFAULT_COPTS")

cc_library(
    name = "dataflow",
    srcs = [
        "graph.cc",
        "operator.cc",
        "record.cc",
        "ops/identity.cc",
    ],
    hdrs = [
        "graph.h",
        "operator.h",
        "record.h",
        "ops/identity.h",
    ],
    copts = PELTON_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "@absl//absl/base:core_headers",
        "@absl//absl/container:flat_hash_map",
        "@absl//absl/container:node_hash_map",
        "@gtest//:gtest_prod",
    ],
)

cc_test(
    name = "dataflow-tests",
    srcs = [
        "record_unittest.cc",
        "graph_unittest.cc"
    ],
    copts = ["-Iexternal/gtest/googletest/include"],
    deps = [
        "@gtest//:gtest",
        "@gtest//:gtest_main",
        "//dataflow",
    ],
)
