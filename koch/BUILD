
py_library(
    name = "db",
   srcs = ["db.py"],
)


py_binary(
    name = "pipeline",
    srcs = ["pipeline.py"],
    deps = [
      ":db",
    ],
)

py_binary(
    name = "sample",
    srcs = ["sample.py"],
    deps = [
      ":db",
      ":pipeline",
    ],
)

py_binary(
    name = "fetch",
    srcs = ["fetch.py"],
    deps = [
      ":db",
      ":pipeline",
      ":sample",
      "//koch/proto:document_py_proto",
    ],
)

py_binary(
    name = "parse",
    srcs = ["parse.py"],
    deps = [
      ":db",
      ":fetch",
      ":pipeline",
      "//koch/proto:document_py_proto",
    ],
)

py_binary(
    name = "eval",
    srcs = ["eval.py"],
    deps = [
      ":db",
      ":parse",
      ":pipeline",
      "//koch/proto:document_py_proto",
    ],
)
