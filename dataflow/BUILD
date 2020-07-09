load("@rules_cc//cc:defs.bzl", "cc_library")
load("//:build/options.bzl", "PELTON_DEFAULT_COPTS")

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
#    linkopts = ABSL_DEFAULT_LINKOPTS,
    visibility = ["//:__pkg__"],
#    deps = [
#        "//absl/base",
#        "//absl/base:config",
#        "//absl/base:core_headers",
#        "//absl/memory",
#        "//absl/strings",
#        "//absl/synchronization",
#    ],
)
