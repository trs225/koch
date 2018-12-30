"""Parses document text from article html."""
from __future__ import absolute_import

import html5lib
import re2

from absl import app
from absl import flags

from koch import db
from koch import extract
from koch import pipeline
from koch.proto import document_pb2

FLAGS = flags.FLAGS
flags.DEFINE_string("parse_output", None, "Output path to write parsed html to.")


def build_doc_helper(node, doc):
  if node.text:
    doc.blobs.add().text = node.text
  for child in node.children:
    build_doc_helper(child, doc)
  if node.tail:
    doc.blobs.add().text = node.tail


def build_doc(nodes):
  doc = document_pb2.Document()
  for node in nodes.elements:
    build_doc_helper(node, doc)
  return doc


class ParsingPipeline(pipeline.Pipeline):
  
  def pipe(self, key, value):
    return key, build_doc(value)
  

def main(argv):
  reader = db.ProtoDbReader(document_pb2.RawHtml, FLAGS.extract_input)
  extractor = extract.ExtractionPipeline(reader)
  writer = db.ProtoDbWriter(document_pb2.Document, FLAGS.parse_output)

  ParsingPipeline(extractor, writer).run()
 

if __name__ == "__main__":
  flags.mark_flag_as_required("extract_input")
  flags.mark_flag_as_required("parse_output")
  app.run(main)
