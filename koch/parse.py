"""Parses document text from article html.

TODO:
 - filter on blob length/position
 - sentence segmentation
 - remove bad words
"""
from __future__ import absolute_import

import nltk
import re2

from absl import app
from absl import flags

from koch import db
from koch import extract
from koch import pipeline
from koch.proto import document_pb2

FLAGS = flags.FLAGS
flags.DEFINE_string("parse_output", None, "Output path to write parsed html to.")

flags.DEFINE_boolean("parse_debug", False, "Whether to use the debug writer.")


def add_blob(doc, text, pos):
  blob = doc.blobs.add()
  blob.text = text
  blob.position.extend(pos)


def build_doc_helper(node, doc, pos):
  if node.text:
    add_blob(doc, node.text, pos)
  for i, child in enumerate(node.children):
    build_doc_helper(child, doc, pos + [i])
  if node.tail:
    add_blob(doc, node.tail, pos[:-1])


def build_doc(nodes):
  doc = document_pb2.Document()
  doc.url = nodes.url
  for i, node in enumerate(nodes.elements):
    build_doc_helper(node, doc, [i])
  return doc


class ParsingPipeline(pipeline.Pipeline):

  def __init__(self, reader, writer=None):
    super(ParsingPipeline, self).__init__(reader, writer)
    self.stopwords = set(nltk.corpus.stopwords.words("english"))
    self.wordnet = nltk.WordNetLemmatizer()
  
  def pipe(self, key, value):
    doc = build_doc(value)
    for blob in doc.blobs:
      tokens = (t.lower() for t in nltk.word_tokenize(blob.text))
      stopped = (t for t in tokens if t not in self.stopwords)
      normalized = (re2.sub(r"\W+", "", t) for t in stopped)
      lemmatized = (self.wordnet.lemmatize(t) for t in normalized if t)
      blob.words.extend(lemmatized)

    yield key, doc
  

def main(argv):
  reader = db.ProtoDbReader(document_pb2.RawHtml, FLAGS.extract_input)
  extractor = extract.ExtractionPipeline(reader)
  writer = db.ProtoDbWriter(document_pb2.Document, FLAGS.parse_output)

  if FLAGS.parse_debug:
    writer = db.DebugWriter()

  ParsingPipeline(extractor, writer).run()
 

if __name__ == "__main__":
  flags.mark_flag_as_required("extract_input")
  flags.mark_flag_as_required("parse_output")
  app.run(main)
