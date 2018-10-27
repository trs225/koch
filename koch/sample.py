"""Samples data at random."""
from __future__ import absolute_import

import random

from absl import app
from absl import flags

from koch import db
from koch import pipeline

FLAGS = flags.FLAGS
flags.DEFINE_string("input", None, "Input raw html db to process.")
flags.DEFINE_string("output", None, "Output path to write parsed html to.")

flags.DEFINE_boolean("output_csv", False, "Whether to output to csv instead.")
flags.DEFINE_string("output_key", "url", "Name of the output csv key column.")
flags.DEFINE_string("output_val", "sample", "Name of the output csv val column.")

flags.DEFINE_integer("number", 100, "Number of items to sample at random.")


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
    with self:
      for i, (k, v) in enumerate(self.reader):
        if i < self.n:
          out.append((k, v))
        else:
          sample((k, v), i, out)

      for k, v in out:
        yield k, v


def main(argv):
  random.seed(0)

  if FLAGS.output_csv:
    writer = db.CsvWriter(FLAGS.output, FLAGS.output_key, FLAGS.output_val)
  else:
    writer = db.Writer(FLAGS.output)

  SamplingPipeline(FLAGS.number, db.Reader(FLAGS.input), writer).run()


if __name__ == "__main__":
  flags.mark_flag_as_required("input")
  flags.mark_flag_as_required("output")
  app.run(main)
