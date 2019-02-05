"""Scores document keywords using tf-idf.

TODO:
 - look into data loss
 - take idf as side input
 - add functions to construct pipelines
"""
from __future__ import absolute_import

import math

from absl import app
from absl import flags

from koch import db
from koch import parse
from koch import pipeline
from koch.proto import document_pb2
from koch.proto import util

FLAGS = flags.FLAGS
flags.DEFINE_string("tmp_output", None, "Output path to write parsed html to.")
flags.DEFINE_string("tf_idf_output", None, "Output path to write tf-idf results to.")


class TfPipeline(pipeline.Pipeline):

  def pipe(self, key, value):
    doc = value
    for word in set(w.text for w in util.IterWords(doc)):
      new_doc = document_pb2.Document()
      new_doc.CopyFrom(doc)
      
      yield str(word), new_doc


def GetCorpusSize(reader):
  with reader:
    return sum(1 for _ in reader)


class IdfPipeline(pipeline.CombiningPipeline):

  def __init__(self, reader, rewriter):
    super(IdfPipeline, self).__init__(reader, rewriter)
    self.n = GetCorpusSize(self.reader)
  
  def pipe(self, key, value):
    doc = value
    for word in set(w.text for w in util.IterWords(doc)):
      keyword = document_pb2.Keyword()
      keyword.word = word
      keyword.doc_count = 1
      keyword.total_doc_count = self.n

      yield str(word), keyword

  def combine(self, value, old_value):
    keyword, old_keyword = value, old_value
    if not old_keyword.word:
      return keyword

    old_keyword.doc_count += keyword.doc_count
    return old_keyword


def GetTfIdfScore(term_count, doc_term_count, term_doc_count, doc_count):
  tf = term_count / float(doc_term_count)
  idf = math.log(doc_count / float(term_doc_count))

  return tf * idf


class TfIdfPipeline(pipeline.CombiningPipeline):
  
  def pipe(self, key, value):
    doc, keyword = value
    term_count = 0
    doc_term_count = 0
    for word in (w.text for w in util.IterWords(doc)):
      doc_term_count += 1
      if word == keyword.word:
        term_count += 1
        
    keyword.term_count = term_count
    keyword.tf_idf = GetTfIdfScore(
        term_count, doc_term_count, keyword.doc_count, keyword.total_doc_count)
    doc.keywords.extend([keyword])

    yield str(doc.url), doc

  def combine(self, value, old_value):
    doc, old_doc = value, old_value
    if not old_doc.url:
      return doc

    old_doc.keywords.extend(doc.keywords)
    return old_doc
  

def main(argv):
  parser = db.ProtoDbReader(document_pb2.Document, FLAGS.parse_output)

  idf_rewriter = db.Rewriter(
      db.ProtoDbReader(document_pb2.Keyword, FLAGS.tmp_output),
      db.ProtoDbWriter(document_pb2.Keyword, FLAGS.tmp_output))
  IdfPipeline(parser, idf_rewriter).run()

  tf_idf_rewriter = db.Rewriter(
      db.ProtoDbReader(document_pb2.Document, FLAGS.tf_idf_output),
      db.ProtoDbWriter(document_pb2.Document, FLAGS.tf_idf_output))
  TfIdfPipeline(
      db.JoiningReader(
          TfPipeline(parser),
          db.ProtoDbReader(document_pb2.Keyword, FLAGS.tmp_output)),
       tf_idf_rewriter).run()


if __name__ == "__main__":
  flags.mark_flag_as_required("parse_output")
  flags.mark_flag_as_required("tmp_output")
  flags.mark_flag_as_required("tf_idf_output")
  app.run(main)
