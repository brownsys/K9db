load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "dataflow",
    srcs = [
        "edge.cc",
        "graph.cc",
        "operator.cc",
        "record.cc",
        "schema.cc",
        "ops/identity.cc",
        "ops/input.cc",
        "ops/matview.cc",
        "ops/filter.cc",
        "ops/union.cc",
        "ops/aggregate.cc",
    ],
    hdrs = [
        "edge.h",
        "graph.h",
        "key.h",
        "operator.h",
        "record.h",
        "record_utils.h",
        "schema.h",
        "types.h",
        "ops/identity.h",
        "ops/input.h",
        "ops/matview.h",
        "ops/filter.h",
        "ops/union.h",
        "ops/aggregate.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@absl//absl/base:core_headers",
        "@absl//absl/container:flat_hash_map",
        "@absl//absl/container:node_hash_map",
        "@glog//:glog",
        "@gtest//:gtest_prod",
    ],
)

cc_test(
    name = "dataflow-tests",
    srcs = [
        "record_unittest.cc",
        "graph_unittest.cc",
        "ops/filter_unittest.cc",
        "ops/union_unittest.cc",
        "ops/aggregate_unittest.cc",
        "ops/matview_unittest.cc",
        "ops/identity_unittest.cc",
        "ops/input_unittest.cc",
    ],
    copts = ["-Iexternal/gtest/googletest/include"],
    deps = [
        "@glog//:glog",
        "@gtest//:gtest",
        "@gtest//:gtest_main",
        "//dataflow",
    ],
)
