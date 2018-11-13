
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
    ],
)

py_binary(
    name = "parse",
    srcs = ["parse.py"],
    deps = [
      ":db",
      ":pipeline",
      "//koch/proto:document_py_proto",
    ],
)
