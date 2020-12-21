cc_binary(
  name = "hello_world",
  deps = [
      "//dataflow",
      "@absl//absl/flags:flag",
      "@absl//absl/flags:parse",
      "@glog//:glog",
      "@com_github_gflags_gflags//:gflags"
  ],
  copts = PELTON_DEFAULT_COPTS,
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
  copts = PELTON_DEFAULT_COPTS,
  srcs = ["ubench.cc"],
)
