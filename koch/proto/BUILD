load("@org_pubref_rules_protobuf//python:rules.bzl", "py_proto_library")

package(default_visibility = ["//visibility:public"])

py_proto_library(
    name = "timestamp_py_proto",
    protos = ["timestamp.proto"],
)

py_proto_library(
    name = "document_py_proto",
    protos = ["document.proto"],
    proto_deps = [
      ":timestamp_py_proto"
    ],
)

py_library(
    name = "util",
    srcs = ["util.py"],
    deps = [
      ":document_py_proto"
    ],
)

py_proto_library(
    name = "text_rank_py_proto",
    protos = ["text_rank.proto"],
)
