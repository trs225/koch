
py_library(
    name = "db",
    srcs = ["db.py"],
)


py_library(
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
    name = "extract",
    srcs = ["extract.py"],
    deps = [
      ":db",
      ":fetch",
      ":pipeline",
      "//koch/proto:document_py_proto",
    ],
)

py_binary(
    name = "parse",
    srcs = ["parse.py"],
    deps = [
      ":db",
      ":extract",
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
      "//koch/proto:util",
    ],
)

py_binary(
    name = "tf_idf",
    srcs = ["tf_idf.py"],
    deps = [
      ":db",
      ":parse",
      ":pipeline",
      "//koch/proto:document_py_proto",
      "//koch/proto:util",
    ],
)

py_binary(
    name = "text_rank",
    srcs = ["text_rank.py"],
    deps = [
      ":db",
      ":parse",
      ":pipeline",
      "//koch/proto:document_py_proto",
      "//koch/proto:text_rank_py_proto",
      "//koch/proto:util",
    ],
)

py_binary(
    name = "naive_bayes",
    srcs = ["naive_bayes.py"],
    deps = [
      ":db",
      ":pipeline",
      ":tf_idf",
      "//koch/proto:document_py_proto",
      "//koch/proto:util",
    ],
)
