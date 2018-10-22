"""Samples data at random."""
from __future__ import absolute_import

import random

from absl import app
from absl import flags

from koch import db

FLAGS = flags.FLAGS
flags.DEFINE_string("input", None, "Input raw html db to process.")
flags.DEFINE_string("output", None, "Output path to write parsed html to.")

flags.DEFINE_integer("number", 100, "Number of items to sample at random.")


def sample(item, k, out):
  if random.random() < len(out) / float(k):
    i = random.randint(0, len(out) - 1)
    out[i] = item


def main(argv):
  random.seed(0)
  with db.Reader(FLAGS.input) as r:
    n = FLAGS.number
    out = []
    for i, (k, v) in enumerate(r):
      if i < n:
        out.append((k, v))
      else:
        sample((k, v), i, out)

  with db.Writer(FLAGS.output) as w:
    for k, v in out:
      w.write(k, v)


if __name__ == "__main__":
  flags.mark_flag_as_required("input")
  flags.mark_flag_as_required("output")
  app.run(main)
