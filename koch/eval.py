"""Compares parsed html against labeled data.

TODO:
 - add labeled document proto
 - normalize words before matching
"""
from __future__ import absolute_import

import re2

from absl import app
from absl import flags

from koch import db
from koch import parse
from koch import pipeline
from koch.proto import document_pb2
from koch.proto import util

FLAGS = flags.FLAGS
flags.DEFINE_string("eval_input", None, "Input raw html db to process.")
flags.DEFINE_string("eval_output", None, "Output path to write parsed html to.")

flags.DEFINE_boolean("eval_input_csv", False, "Whether to read from csv instead.")
flags.DEFINE_string("eval_input_key", None, "Name of the input csv key column.")
flags.DEFINE_string("eval_input_val", None, "Name of the input csv val column.")


def get_word_counts(string):
  out = {}
  for word in re2.split(r'\W', string):
    if word in out:
      out[word] += 1
    else:
      out[word] = 1
  return out


def intersect(pred_counts, label_counts):
  i = 0
  for key in pred_counts:
    if key in label_counts:
      i += min(pred_counts[key], label_counts[key])
  return i


def total_count(counts):
  return sum(counts.values())


def precision(pred, label):
  pred_counts = get_word_counts(pred)
  label_counts = get_word_counts(label)
  numerator = intersect(pred_counts, label_counts)
  denominator = float(total_count(pred_counts))
  return numerator / denominator


def recall(pred, label):
  pred_counts = get_word_counts(pred)
  label_counts = get_word_counts(label)
  numerator = intersect(pred_counts, label_counts)
  denominator = float(total_count(label_counts))
  return numerator / denominator


class EvalPipeline(pipeline.Pipeline):
  
  def pipe(self, key, value):
    # print 'KEY: %s' % key
    # print 'VAL: %s' % str(value)

    label, doc = value
    pred = util.GetText(doc)

    p = precision(pred, label)
    r = recall(pred, label)

    return key, "Precision: %f Recall: %f" % (p, r)


def main(argv):
  eval_reader = None

  if FLAGS.eval_input_csv:
    eval_reader = db.CsvReader(
        FLAGS.eval_input,
        FLAGS.eval_input_key,
        FLAGS.eval_input_val)

  reader = db.JoiningReader(
      eval_reader, db.ProtoDbReader(
          document_pb2.Document, FLAGS.parse_output))

  EvalPipeline(reader, db.DebugWriter()).run()


if __name__ == "__main__":
  flags.mark_flag_as_required("eval_input")
  flags.mark_flag_as_required("parse_output")
  flags.mark_flag_as_required("eval_output")
  app.run(main)
