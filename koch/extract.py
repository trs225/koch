"""Extracts main article body.

TODO:
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


def build_html_element(html, proto=None):
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

 
def measure_pos(node):
  return text_len(node.text) + text_len(node.tail)


def measure_neg(node):
  return 2 * len(node.tag) + len(str(node.attrib or ""))


_positive = 'pos'
_negative = 'neg'


def parse(html, proto=None):
  proto = build_html_element(html, proto)
  proto.weight[_positive] = measure_pos(proto)
  proto.weight[_negative] = measure_neg(proto)
  for child in html:
    if is_valid(child):
      parsed_child = parse(child, proto.children.add())
      proto.weight[_positive] += parsed_child.weight[_positive]
      proto.weight[_negative] += parsed_child.weight[_negative]
  return proto


def score_normalized(node, pos, neg):
  node_pos = node.weight[_positive] / pos
  node_neg = node.weight[_negative] / neg
  node.score = node_pos - 2 * node_neg
  for child in node.children:
    score_normalized(child, pos, neg)
  return node

 
def score(node):
  pos = node.weight[_positive] or 1
  neg = node.weight[_negative] or 1
  return score_normalized(node, pos, neg)


def find_best(node):
  best = document_pb2.HtmlElements(
    elements=[node], score=node.score)
  for child in node.children:
    child_best = find_best(child)
    if best.score < child_best.score:
      best = child_best
  return best


class ExtractionPipeline(pipeline.Pipeline):
  
  def pipe(self, key, value):
    tree = html5lib.parse(
        value.html, treebuilder="etree", namespaceHTMLElements=False)
    node = score(parse(tree))
    best = find_best(node)

    return key, best
  

def main(argv):
  reader = db.ProtoDbReader(document_pb2.RawHtml, FLAGS.extract_input)
  writer = db.ProtoDbWriter(document_pb2.HtmlElements, FLAGS.extract_output)

  if FLAGS.extract_debug:
    reader = fetch.FetchingPipeline(
        db.DebugReader(FLAGS.extract_debug))
    writer = db.DebugWriter()

  ExtractionPipeline(reader, writer).run()
 

if __name__ == "__main__":
  flags.mark_flag_as_required("extract_input")
  flags.mark_flag_as_required("extract_output")
  app.run(main)