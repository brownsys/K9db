workspace(name = "pelton")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Abseil C++ Libraries
http_archive(
  name = "absl",
  urls = ["https://github.com/abseil/abseil-cpp/archive/20200923.2.zip"],
  strip_prefix = "abseil-cpp-20200923.2",
  sha256 = "306639352ec60dcbfc695405e989e1f63d0e55001582a5185b0a8caf2e8ea9ca",
)

# Google Test
http_archive(
  name = "gtest",
  urls = ["https://github.com/google/googletest/archive/release-1.10.0.tar.gz"],
  strip_prefix = "googletest-release-1.10.0",
  sha256 = "9dc9157a9a1551ec7a7e43daea9a694a0bb5fb8bec81235d8a1e6ef64c716dcb",
)

# Google GFlags
# Needed because GLog depends on this.
#
# NOTE(malte): do *not* change the name of this dependency; GLog's build
# infrastructure depends on it being "com_github_gflags_gflags"
http_archive(
  name = "com_github_gflags_gflags",
  strip_prefix = "gflags-2.2.2",
  urls = ["https://github.com/gflags/gflags/archive/v2.2.2.tar.gz"],
  sha256 = "34af2f15cf7367513b352bdcd2493ab14ce43692d2dcd9dfc499492966c64dcf",
)

# Google GLog
# Abseil will provide a similar logging API in the future, but for now
# we're tied to classic GLog and GFlags.
http_archive(
  name = "glog",
  urls = ["https://github.com/google/glog/archive/v0.4.0.tar.gz"],
  strip_prefix = "glog-0.4.0",
  sha256 = "f28359aeba12f30d73d9e4711ef356dc842886968112162bc73002645139c39c",
)

# Google Benchmark library for microbenchmarks
http_archive(
  name = "gbenchmark",
  urls = ["https://github.com/google/benchmark/archive/v1.5.2.tar.gz"],
  strip_prefix = "benchmark-1.5.2",
  sha256 = "dccbdab796baa1043f04982147e67bb6e118fe610da2c65f88912d73987e700c",
)

# C++ rules for Bazel.
http_archive(
  name = "rules_cc",
  urls = ["https://github.com/bazelbuild/rules_cc/archive/9e10b8a6db775b1ecd358d8ddd3dab379a2c29a5.zip"],
  strip_prefix = "rules_cc-9e10b8a6db775b1ecd358d8ddd3dab379a2c29a5",
  sha256 = "954b7a3efc8752da957ae193a13b9133da227bdacf5ceb111f2e11264f7e8c95",
)

#
# For the sharding system.
load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")

# ANTLR Runtime and Parser generator from grammar files.
http_archive(
    name = "rules_antlr",
    sha256 = "26e6a83c665cf6c1093b628b3a749071322f0f70305d12ede30909695ed85591",
    strip_prefix = "rules_antlr-0.5.0",
    urls = ["https://github.com/marcohu/rules_antlr/archive/0.5.0.tar.gz"],
)

load("@rules_antlr//antlr:repositories.bzl", "rules_antlr_dependencies")
load("@rules_antlr//antlr:lang.bzl", "CPP")
rules_antlr_dependencies("4.8", CPP)


# ANTLR grammars repository (non-bazel) which includes the sqlite grammar!
# TODO(babman): go back to using the git repo instead of the fork when my
# PR is approved.
new_git_repository(
    name = "grammars-v4",
    #remote = "https://github.com/antlr/grammars-v4",
    remote = "https://github.com/KinanBab/grammars-v4.git",
    #commit = "9514c04a33a0cf157853334105f32fc914e4f1de",  # My Fix on Oct 27 2020
    commit = "ead0c4b7ec6a41a602bf35c631bf398729268af1",  # My non-merged second fix from Nov 10 2020
    build_file_content = """
load("@rules_antlr//antlr:antlr4.bzl", "antlr")

# Compile the grammar with the antlrcpp generation tool
antlr(
    name = "sqlgrammar",
    srcs = [
        "sql/sqlite/SQLiteParser.g4",
        "sql/sqlite/SQLiteLexer.g4",
    ],
    no_visitor = False,
    visitor = True,
    language = "Cpp",
    package = "sqlparser",
)

# Expose compiled grammar as a Cpp library
cc_library(
    name = "sqlparser",
    srcs = [":sqlgrammar"],
    visibility = ["//visibility:public"],
    deps = [
        ":sqlgrammar",
        "@antlr4_runtimes//:cpp",
    ],
)
""",
)
