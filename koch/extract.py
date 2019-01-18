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
flags.DEFINE_string("extract_input", None, "Input raw html db to process.")
flags.DEFINE_string("extract_output", None, "Output path to write parsed html to.")

flags.DEFINE_multi_string("extract_debug", None, "Input urls to debug.")


def is_valid(html):
  if callable(html.tag):
    return False
  elif html.tag in ("iframe", "noscript", "script", "style"):
    return False
  # elif re2.findall(
  #     "crumbs|links|sidebar|share|social",
  #     html.attrib.get("class", "") + html.attrib.get("id", "")):
  #   return False
  # elif re2.findall(
  #     "comments|disqus", html.attrib.get("id", "")):
  #   return False
  else:
    return True


def get_text(string):
  if string and not string.isspace():
    return re2.sub(r"\s+", " ", string)
  else:
    return ""


def to_html_element(html, proto=None):
  if not proto:
    proto = document_pb2.HtmlElement()
  proto.tag = html.tag
  proto.text = get_text(html.text)
  proto.tail = get_text(html.tail)
  for key, val in html.attrib.iteritems():
    if key != "style":
      proto.attrib[key] = val
  return proto


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


def build_html_element(html, proto=None):
  proto = to_html_element(html, proto)
  proto.weight[_positive] = measure_pos(proto)
  proto.weight[_negative] = measure_neg(proto)
  for child in html:
    if is_valid(child):
      parsed_child = build_html_element(child, proto.children.add())
      proto.weight[_positive] += parsed_child.weight[_positive]
      proto.weight[_negative] += parsed_child.weight[_negative]
  return proto


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
  
  def pipe(self, key, value):
    doc = value
    tree = html5lib.parse(
        doc.raw_html.html, treebuilder="etree", namespaceHTMLElements=False)
    html_elements = find_best_elements(score(build_html_element(tree)))
    doc.html_elements.elements.extend(html_elements.elements)
    doc.html_elements.score = html_elements.score
    doc.html_elements.url = doc.url

    yield key, doc
  

def main(argv):
  reader = db.ProtoDbReader(document_pb2.Document, FLAGS.fetch_output)
  writer = db.ProtoDbWriter(document_pb2.Document, FLAGS.extract_output)

  if FLAGS.extract_debug:
    reader = fetch.FetchingPipeline(
        db.DebugReader(FLAGS.extract_debug))
    writer = db.DebugWriter()

  ExtractionPipeline(reader, writer).run()
 

if __name__ == "__main__":
  flags.mark_flag_as_required("extract_input")
  flags.mark_flag_as_required("extract_output")
  app.run(main)
