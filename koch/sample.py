"""Samples data at random."""
from __future__ import absolute_import

import random

from absl import app
from absl import flags

from koch import db
from koch import pipeline

FLAGS = flags.FLAGS
flags.DEFINE_string("sample_input", None, "Input raw html db to process.")
flags.DEFINE_string("sample_output", None, "Output path to write parsed html to.")

flags.DEFINE_integer("sample_number", None, "Number of items to sample at random.")

flags.DEFINE_boolean("sample_input_csv", False, "Whether to read from csv instead.")
flags.DEFINE_string("sample_input_key", None, "Name of the input csv key column.")
flags.DEFINE_string("sample_input_val", None, "Name of the input csv val column.")

flags.DEFINE_boolean("sample_output_csv", False, "Whether to output to csv instead.")
flags.DEFINE_string("sample_output_key", "url", "Name of the output csv key column.")
flags.DEFINE_string("sample_output_val", "sample", "Name of the output csv val column.")


def sample(item, k, out):  
  if random.random() < len(out) / float(k):
    i = random.randint(0, len(out) - 1)
    out[i] = item


class SamplingPipeline(pipeline.Pipeline):
  
  def __init__(self, n, reader, writer=None):
    super(SamplingPipeline, self).__init__(reader, writer)
    self.n = n

  def __iter__(self):
    out = []
    for i, (k, v) in enumerate(self.reader):
      if i < self.n:
        out.append((k, v))
      else:
        sample((k, v), i, out)

    for k, v in out:
      yield k, v


def main(argv):
  random.seed(0)
  reader = db.DbReader(FLAGS.sample_input)
  writer = db.DbWriter(FLAGS.sample_output)

  if FLAGS.sample_input_csv:
    reader = db.CsvReader(
        FLAGS.sample_input, FLAGS.sample_input_key, FLAGS.sample_input_val)
    
  if FLAGS.sample_output_csv:
    writer = db.CsvWriter(
        FLAGS.sample_output, FLAGS.sample_output_key, FLAGS.sample_output_val)

  SamplingPipeline(FLAGS.sample_number, reader, writer).run()


if __name__ == "__main__":
  flags.mark_flag_as_required("sample_input")
  flags.mark_flag_as_required("sample_output")
  app.run(main)
