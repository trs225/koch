"""Extracts main article body.

TODO:
 - build protos more efficiently
 - compare body extraction methods
 - retain all tail text
"""
from __future__ import absolute_import

import html5lib
import re2

from absl import app
from absl import flags

from koch import db
from koch import fetch
from koch import pipeline
from koch.proto import document_pb2

FLAGS = flags.FLAGS
flags.DEFINE_string("extract_output", None, "Output path to write parsed html to.")

flags.DEFINE_boolean("extract_debug", False, "Whether to use the debug writer.")


def text_len(string):
  if string.isspace():
    return 0
  else:
    return len(string)

 
def measure_pos(html_element):
  return text_len(html_element.text) + text_len(html_element.tail)


def measure_neg(html_element):
  return 2 * len(html_element.tag) + len(str(html_element.attrib or ""))


_positive = 'pos'
_negative = 'neg'


def weight_html_element(html_element):
  html_element.weight[_positive] = measure_pos(html_element)
  html_element.weight[_negative] = measure_neg(html_element)
  for child in html_element.children:
    weighted_child = weight_html_element(child)
    html_element.weight[_positive] += weighted_child.weight[_positive]
    html_element.weight[_negative] += weighted_child.weight[_negative]
  return html_element


def score_normalized(html_element, pos, neg):
  node_pos = html_element.weight[_positive] / pos
  node_neg = html_element.weight[_negative] / neg
  html_element.score = node_pos - 2 * node_neg
  for child in html_element.children:
    score_normalized(child, pos, neg)
  return html_element

 
def score(html_element):
  pos = html_element.weight[_positive] or 1
  neg = html_element.weight[_negative] or 1
  return score_normalized(html_element, pos, neg)


def find_best_elements(html_element):
  best = document_pb2.HtmlElements(
    elements=[html_element], score=html_element.score)
  for child in html_element.children:
    child_best = find_best_elements(child)
    if best.score < child_best.score:
      best = child_best
  return best


class ExtractionPipeline(pipeline.Pipeline):

  def __init__(self, reader, writer=None, debug=False):
    super(ExtractionPipeline, self).__init__(reader, writer)
    self.debug = debug
  
  def pipe(self, key, value):
    doc = value
    html_elements = find_best_elements(
        score(weight_html_element(doc.parsed_html)))

    doc.content_html.elements.extend(html_elements.elements)
    doc.content_html.score = html_elements.score

    if not self.debug:
      doc.ClearField("raw_html")
      doc.ClearField("parsed_html")

    yield key, doc
  

def main(argv):
  reader = db.ProtoDbReader(document_pb2.Document, FLAGS.fetch_output)
  writer = db.ProtoDbWriter(document_pb2.Document, FLAGS.extract_output)

  if not FLAGS.extract_output:
    writer = db.DebugWriter()

  ExtractionPipeline(reader, writer, FLAGS.fetch_debug).run()
 

if __name__ == "__main__":
  flags.mark_flag_as_required("fetch_output")
  flags.mark_flag_as_required("extract_output")
  app.run(main)
