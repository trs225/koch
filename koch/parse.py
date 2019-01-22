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


def build_blobs(html_element, doc, pos):
  if html_element.text:
    add_blob(doc, html_element.text, pos)
  for i, child in enumerate(html_element.children):
    build_blobs(child, doc, pos + [i])
  if html_element.tail:
    add_blob(doc, html_element.tail, pos[:-1])


class ParsingPipeline(pipeline.Pipeline):

  def __init__(self, reader, writer=None, debug=False):
    super(ParsingPipeline, self).__init__(reader, writer)
    self.stopwords = set(nltk.corpus.stopwords.words("english"))
    self.wordnet = nltk.WordNetLemmatizer()
    self.debug = debug
  
  def pipe(self, key, value):
    doc = value
    for i, elm in enumerate(doc.content_html.elements):
      build_blobs(elm, doc, [i])

    for blob in doc.blobs:
      tokens = (t.lower() for t in nltk.word_tokenize(blob.text))
      stopped = (t for t in tokens if t not in self.stopwords)
      normalized = (re2.sub(r"\W+", "", t) for t in stopped)
      lemmatized = (self.wordnet.lemmatize(t) for t in normalized if t)
      blob.words.extend(lemmatized)

    if not self.debug:
      doc.ClearField("raw_html")
      doc.ClearField("parsed_html")
      doc.ClearField("content_html")

    yield key, doc
  

def main(argv):
  reader = db.ProtoDbReader(document_pb2.Document, FLAGS.extract_output)
  writer = db.ProtoDbWriter(document_pb2.Document, FLAGS.parse_output)

  if not FLAGS.parse_output:
    writer = db.DebugWriter()

  ParsingPipeline(reader, writer, FLAGS.parse_debug).run()
 

if __name__ == "__main__":
  flags.mark_flag_as_required("extract_output")
  flags.mark_flag_as_required("parse_output")
  app.run(main)
