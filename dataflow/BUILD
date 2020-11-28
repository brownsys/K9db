load("@rules_cc//cc:defs.bzl", "cc_library")
load("//build:options.bzl", "PELTON_DEFAULT_COPTS")

cc_library(
    name = "dataflow",
    srcs = [
        #"edge.cc",
        #"graph.cc",
        "operator.cc",
        "record.cc",
        "schema.cc",
        "ops/identity.cc",
        "ops/input.cc",
        "ops/matview.cc",
        #"ops/filter.cc",
        #"ops/union.cc",
    ],
    hdrs = [
        "edge.h",
        "graph.h",
        "key.h",
        "operator.h",
        "record.h",
        "schema.h",
        "types.h",
        "ops/identity.h",
        "ops/input.h",
        "ops/matview.h",
        #"ops/filter.h",
        #"ops/union.h",
    ],
    copts = PELTON_DEFAULT_COPTS,
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
        #"graph_unittest.cc",
        #"ops/filter_unittest.cc",
        #"ops/union_unittest.cc",
    ],
    copts = ["-Iexternal/gtest/googletest/include"],
    deps = [
        "@glog//:glog",
        "@gtest//:gtest",
        "@gtest//:gtest_main",
        "//dataflow",
    ],
)
