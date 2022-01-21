
load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")  # buildifier: disable=load
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")  # buildifier: disable=load
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")  # buildifier: disable=load
load("//cargo:crates.bzl", "raze_fetch_remote_crates")  # buildifier: disable=load

def cc_deps():
    # Google GFlags
    # Needed because GLog depends on this.
    #
    # NOTE(malte): do *not* change the name of this dependency; GLog's build
    # infrastructure depends on it being "com_github_gflags_gflags"
    maybe(
        http_archive,
        name = "com_github_gflags_gflags",
        sha256 = "34af2f15cf7367513b352bdcd2493ab14ce43692d2dcd9dfc499492966c64dcf",
        strip_prefix = "gflags-2.2.2",
        urls = ["https://github.com/gflags/gflags/archive/v2.2.2.tar.gz"],
    )

    maybe(
        http_archive,
        name = "glog",
        sha256 = "f28359aeba12f30d73d9e4711ef356dc842886968112162bc73002645139c39c",
        strip_prefix = "glog-0.4.0",
        urls = ["https://github.com/google/glog/archive/v0.4.0.tar.gz"],
    )
    
    maybe(
        http_archive,
        name = "rules_foreign_cc",
        sha256 = "33a5690733c5cc2ede39cb62ebf89e751f2448e27f20c8b2fbbc7d136b166804",
        strip_prefix = "rules_foreign_cc-0.5.1",
        url = "https://github.com/bazelbuild/rules_foreign_cc/archive/0.5.1.tar.gz",
    )
    
    maybe(
        http_archive,
        name = "libevent",
        build_file_content = """
load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

filegroup(
    name = "all",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

configure_make(
    name = "libevent",
    lib_source = ":all",
    visibility = ["//visibility:public"],
)

""",
        strip_prefix = "libevent-2.1.12-stable",
        type = "tar.gz",
        url = "https://github.com/libevent/libevent/releases/download/release-2.1.12-stable/libevent-2.1.12-stable.tar.gz",
    )

    # Memcached (the binary).
    maybe(
        http_archive,
        name = "memcached",
        build_file_content = """
load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

filegroup(
    name = "all",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

configure_make(
    name = "memcached_build",
    lib_source = ":all",
    visibility = ["//visibility:public"],
    deps = [
        "@libevent//:libevent",
    ],
    out_binaries = ["memcached"],
    out_static_libs = [],
    out_shared_libs = [],
    configure_options = [ "CFLAGS='-Wno-error'" ],
)

filegroup(  # filters the output of memcached_build to only contain the binary
    name = "bin",
    srcs = [":memcached_build"],
    output_group = "memcached"
)

sh_binary(  # Runnable binary.
    name = "memcached",
    srcs = [":bin"],
)
""",
        strip_prefix = "memcached-1.6.10",
        type = "tar.gz",
        url = "https://memcached.org/files/memcached-1.6.10.tar.gz",
    )

    # Memcached C client.
    maybe(
        http_archive,
        name = "libmemcached",
        build_file_content = """
load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "all",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

cmake(
    name = "libmemcached",
    lib_source = ":all",
    visibility = ["//visibility:public"],
    linkopts = [
        "-ldl",
    ],
    out_shared_libs = [
        "libhashkit.so",
        "libhashkit.so.2",
        "libhashkit.so.2.0.0",
        "libmemcachedprotocol.so",
        "libmemcachedprotocol.so.0",
        "libmemcachedprotocol.so.0.0.0",
        "libmemcached.so",
        "libmemcached.so.11",
        "libmemcached.so.11.0.0",
        "libmemcachedutil.so",
        "libmemcachedutil.so.2",
        "libmemcachedutil.so.2.0.0",
    ],
    out_static_libs = [],
    out_binaries = [],
)
""",
        strip_prefix = "libmemcached-1.1.0",
        type = "zip",
        url = "https://github.com/awesomized/libmemcached/archive/refs/tags/1.1.0.zip",
    )

def rust_deps():
    cc_deps()

    # Rust toolchain
    maybe(
        http_archive,
        name = "rules_rust",
        sha256 = "accb5a89cbe63d55dcdae85938e56ff3aa56f21eb847ed826a28a83db8500ae6",
        strip_prefix = "rules_rust-9aa49569b2b0dacecc51c05cee52708b7255bd98",
        urls = [
            # Main branch as of 2021-02-19
            "https://github.com/bazelbuild/rules_rust/archive/9aa49569b2b0dacecc51c05cee52708b7255bd98.tar.gz",
        ],
    )
    raze_fetch_remote_crates()
        
def java_deps():
    cc_deps()

    # Java related tools (for java bindings)
    RULES_JVM_EXTERNAL_TAG = "2.5"
    RULES_JVM_EXTERNAL_SHA = "249e8129914be6d987ca57754516be35a14ea866c616041ff0cd32ea94d2f3a1"
    maybe(
        http_archive,
        name = "rules_jvm_external",
        sha256 = RULES_JVM_EXTERNAL_SHA,
        strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
        url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
    )
