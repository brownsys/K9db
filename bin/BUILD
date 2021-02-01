load("@rules_cc//cc:defs.bzl", "cc_library")

cc_binary(
  name = "hello_world",
  deps = [
      "//dataflow",
      "@absl//absl/flags:flag",
      "@absl//absl/flags:parse",
      "@glog//:glog",
      "@com_github_gflags_gflags//:gflags"
  ],
  srcs = ["main.cc"],
)

cc_binary(
  name = "ubench",
  deps = [
      "//dataflow",
      "@absl//absl/flags:flag",
      "@absl//absl/flags:parse",
      "@glog//:glog",
      "@com_github_gflags_gflags//:gflags",
      "@gbenchmark//:benchmark"
  ],
  srcs = ["ubench.cc"],
)
