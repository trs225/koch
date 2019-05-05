"""Finds prominent class keywords using mutual information."""
from __future__ import absolute_import

import math
import re2

from absl import app
from absl import flags
from absl import logging

from koch import db
from koch import naive_bayes
from koch import parse
from koch import pipeline
from koch import tf_idf
from koch.proto import document_pb2
from koch.proto import util

FLAGS = flags.FLAGS
flags.DEFINE_string(
    "mutual_info_output", None, "Output path to write naive bayse results to.")


class PriorPipeline(pipeline.CombiningPipeline):

  def __init__(self, label, classes, reader, rewriter):
    super(PriorPipeline, self).__init__(reader, rewriter)
    self.classes = classes
    self.label = label

  def pipe(self, key, value):
    doc = value
    label = naive_bayes.Label(doc, self.label, self.classes)
    for word in set(w.text for w in util.IterWords(doc)):
      keyword = document_pb2.Keyword()
      keyword.word = word
      keyword.doc_count = 1
      keyword.prior[label] = 1

      yield str(word), keyword

  def combine(self, value, old_value):
    keyword, old_keyword = value, old_value
    if not old_keyword.word:
      old_keyword = document_pb2.Keyword()
      old_keyword.word = keyword.word

      # Add 1 smoothing
      old_keyword.doc_count = len(self.classes)
      for c in self.classes:
        old_keyword.prior[c] += 1

    old_keyword.doc_count += keyword.doc_count
    for c in keyword.prior:
      old_keyword.prior[c] += keyword.prior[c]

    return old_keyword


class MutualInfoPipeline(pipeline.Pipeline):

  def __init__(self, class_priors, reader, writer=None):
    super(MutualInfoPipeline, self).__init__(reader, writer)
    self.total_doc_count = sum(v for k, v in class_priors.iteritems())
    self.class_priors = class_priors

  def pipe(self, key, value):
    keyword = value

    for c in self.class_priors:
      n = self.total_doc_count

      n_1 = keyword.doc_count
      n_0 = n - n_1

      n__1 = self.class_priors[c]
      n__0 = n - n__1

      n_11 = keyword.prior[c]
      n_10 = keyword.doc_count - keyword.prior[c]
      n_01 = self.class_priors[c] - keyword.prior[c]
      n_00 = n - n_11 - n_10 - n_01

      keyword.mutual_info[c] += n_11 * (
          math.log(n * n_11, 2) - math.log(n_1 * n__1, 2)) / n
      keyword.mutual_info[c] += n_01 * (
          math.log(n * n_01, 2) - math.log(n_0 * n__1, 2)) / n
      keyword.mutual_info[c] += n_10 * (
          math.log(n * n_10, 2) - math.log(n_1 * n__0, 2)) / n
      keyword.mutual_info[c] += n_00 * (
          math.log(n * n_00, 2) - math.log(n_0 * n__0, 2)) / n

    yield str(keyword.word), keyword


def main(argv):
  doc_reader = db.ProtoDbReader(document_pb2.Document, FLAGS.parse_output)
  doc_labels = naive_bayes.LabelPipeline(FLAGS.label, FLAGS.classes, doc_reader)

  class_priors = naive_bayes.GetClassPriors(FLAGS.label, FLAGS.classes, doc_labels)
  logging.info("Class priors: %s", class_priors)

  prior_rewriter = db.Rewriter(
      db.ProtoDbReader(document_pb2.Keyword, FLAGS.tmp_output),
      db.ProtoDbWriter(document_pb2.Keyword, FLAGS.tmp_output))
  PriorPipeline(FLAGS.label, FLAGS.classes, doc_labels, prior_rewriter).run()

  MutualInfoPipeline(
    class_priors,
    db.ProtoDbReader(document_pb2.Keyword, FLAGS.tmp_output),
    db.ProtoDbWriter(document_pb2.Keyword, FLAGS.mutual_info_output)).run()


if __name__ == "__main__":
  flags.mark_flag_as_required("label")
  flags.mark_flag_as_required("classes")
  flags.mark_flag_as_required("tmp_output")
  flags.mark_flag_as_required("mutual_info_output")
  app.run(main)
