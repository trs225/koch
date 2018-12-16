"""Samples data at random."""
from __future__ import absolute_import

from absl import app
from absl import flags

from koch import db
from koch import parse
from koch import pipeline
from koch.proto import document_pb2

FLAGS = flags.FLAGS
flags.DEFINE_string("eval_input", None, "Input raw html db to process.")
flags.DEFINE_string("eval_output", None, "Output path to write parsed html to.")

flags.DEFINE_boolean("eval_input_csv", False, "Whether to read from csv instead.")
flags.DEFINE_string("eval_input_key", None, "Name of the input csv key column.")
flags.DEFINE_string("eval_input_val", None, "Name of the input csv val column.")


class EvalPipeline(pipeline.Pipeline):
  
  def pipe(self, key, value):
    return key, value


def main(argv):
  eval_reader = None

  if FLAGS.eval_input_csv:
    eval_reader = db.CsvReader(
        FLAGS.eval_input,
        FLAGS.eval_input_key,
        FLAGS.eval_input_val)

  reader = db.JoiningReader(
      eval_reader, db.ProtoDbReader(
          document_pb2.HtmlElements, FLAGS.parse_output))

  EvalPipeline(reader, db.DebugWriter()).run()


if __name__ == "__main__":
  flags.mark_flag_as_required("eval_input")
  flags.mark_flag_as_required("eval_output")
  app.run(main)
