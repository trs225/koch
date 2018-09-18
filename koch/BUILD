
py_library(
    name = "db",
   srcs = ["db.py"],
)

py_binary(
    name = "fetch",
    srcs = ["fetch.py"],
    deps = [
      ":db",
    ],
)

py_binary(
    name = "parse",
    srcs = ["parse.py"],
    deps = [
      ":db",
      "//koch/proto:document_py_proto",
    ],
)
