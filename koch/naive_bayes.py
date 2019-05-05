"""Classifies documents using naive bayes.

TODO:
 - add check that doc doesn't have multiple labels
 - weight against words that don't appear in training
 - update main for out of sample prediction
"""
from __future__ import absolute_import

import math
import re2

from absl import app
from absl import flags
from absl import logging

from koch import db
from koch import pipeline
from koch import tf_idf
from koch.proto import document_pb2
from koch.proto import util

FLAGS = flags.FLAGS
flags.DEFINE_string("naive_bayes_input", None, "Input path to parsed docs.")
flags.DEFINE_string(
    "naive_bayes_output", None, "Output path to write naive bayse results to.")

flags.DEFINE_string("label", None, "Metadata field of labels.")
flags.DEFINE_multi_string("classes", None, "Label classifications.")


def Normalize(string):
  return string.strip().lower()


def GetValues(string):
  parts = (s for s in re2.split(r"\(.*\)", string) if s)
  return set(
      Normalize(s)
      for part in parts
      for s in part.split(','))


def Label(doc, label, classes):
  if label not in doc.metadata:
    return

  values = GetValues(doc.metadata[label])
  for c in classes:
    if c in values:
      return c


class LabelPipeline(pipeline.Pipeline):

  def __init__(self, label, classes, reader, writer=None):
    super(LabelPipeline, self).__init__(reader, writer)
    self.label = label
    self.classes = classes

  def pipe(self, key, val):
    doc = val
    label = Label(doc, self.label, self.classes)
    if label:
      yield key, doc


class PriorPipeline(pipeline.CombiningPipeline):

  def __init__(self, label, classes, reader, rewriter):
    super(PriorPipeline, self).__init__(reader, rewriter)
    self.n = tf_idf.GetCorpusSize(reader)  # UNUSED
    self.classes = classes
    self.label = label

  def pipe(self, key, value):
    doc = value
    label = Label(doc, self.label, self.classes)
    for word in set(k.word for k in doc.keywords):
      keyword = document_pb2.Keyword()
      keyword.word = word
      keyword.prior[label] = 1

      yield str(word), keyword

  def combine(self, value, old_value):
    keyword, old_keyword = value, old_value
    if not old_keyword.word:
      old_keyword = document_pb2.Keyword()
      old_keyword.CopyFrom(keyword)
      old_keyword.ClearField("prior")

      # Add 1 smoothing
      for c in self.classes:
        old_keyword.prior[c] += 1

    for c in keyword.prior:
      old_keyword.prior[c] += keyword.prior[c]

    return old_keyword


def GetClassPriors(label, classes, reader):
  out = {c: len(classes) for c in classes}  # Add 1 smoothing
  with reader:
    for key, doc in reader:
      out[Label(doc, label, classes)] += 1
  
  return out


class DocKeywordPipeline(pipeline.Pipeline):

  def pipe(self, key, value):
    doc = value
    for word in set(k.word for k in doc.keywords):
      new_doc = document_pb2.Document()
      new_doc.CopyFrom(doc)

      yield str(word), new_doc


class NaiveBayesPipeline(pipeline.CombiningPipeline):

  def __init__(self, class_priors, reader, rewriter):
    super(NaiveBayesPipeline, self).__init__(reader, rewriter)
    self.label_count = sum(v for k, v in class_priors.iteritems())
    self.class_priors = class_priors

  def weight(self, doc, word):
    return sum(1 for w in util.IterWords(doc) if w.text == word)

  def pipe(self, key, value):
    doc, keyword = value
    if not keyword.word:
      return

    for c in keyword.prior:
      weight = self.weight(doc, keyword.word)
      doc.classification[c] =  weight * math.log(
        keyword.prior[c] / self.class_priors[c])

    yield str(doc.url), doc

  def combine(self, value, old_value):
    doc, old_doc = value, old_value
    if not old_doc.url:
      old_doc = document_pb2.Document()
      old_doc.CopyFrom(doc)
      old_doc.ClearField("classification")
      for c in self.class_priors:
        old_doc.classification[c] = math.log(
          self.class_priors[c] / float(self.label_count))

    for c in doc.classification:
      old_doc.classification[c] += doc.classification[c]

    return old_doc


def main(argv):
  doc_reader = db.ProtoDbReader(document_pb2.Document, FLAGS.naive_bayes_input)

  doc_labels = LabelPipeline(FLAGS.label, FLAGS.classes, doc_reader)

  class_priors = GetClassPriors(FLAGS.label, FLAGS.classes, doc_labels)
  logging.info("Class priors: %s", class_priors)

  prior_rewriter = db.Rewriter(
      db.ProtoDbReader(document_pb2.Keyword, FLAGS.tmp_output),
      db.ProtoDbWriter(document_pb2.Keyword, FLAGS.tmp_output))
  PriorPipeline(FLAGS.label, FLAGS.classes, doc_labels, prior_rewriter).run()

  naive_bayes_rewriter = db.Rewriter(
    db.ProtoDbReader(document_pb2.Document, FLAGS.naive_bayes_output),
    db.ProtoDbWriter(document_pb2.Document, FLAGS.naive_bayes_output))
  NaiveBayesPipeline(
    class_priors,
    db.JoiningReader(
        DocKeywordPipeline(doc_reader),
        db.ProtoDbReader(document_pb2.Keyword, FLAGS.tmp_output)),
    naive_bayes_rewriter).run()


if __name__ == "__main__":
  flags.mark_flag_as_required("naive_bayes_input")
  flags.mark_flag_as_required("tmp_output")
  flags.mark_flag_as_required("naive_bayes_output")
  flags.mark_flag_as_required("label")
  flags.mark_flag_as_required("classes")
  app.run(main)
