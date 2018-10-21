"""Fetches raw html content from urls.

TODO:
 - chain module outputs
 - verify against heuristic ? 
"""
from __future__ import absolute_import

import collections
import html5lib
import re2

from absl import app
from absl import flags
from absl import logging

from koch import db
from koch.proto import document_pb2

FLAGS = flags.FLAGS
flags.DEFINE_string("input", None, "Input raw html db to process.")
flags.DEFINE_string("output", None, "Output path to write parsed html to.")


def is_valid(html):
  if callable(html.tag):
    return False
  elif html.tag in ("script", "style"):
    return False
  elif re2.findall('crumbs|links|sidebar|social', html.attrib.get('class', '')):
    return False
  else:
    return True


def get_text(unicode):
  if unicode:
    return re2.sub('\s+', ' ', unicode.encode("utf-8").decode("string_escape"))
  else:
    return ''


def build_html_element(html, proto=None):
  if not proto:
    proto = document_pb2.HtmlElement()
  proto.tag = html.tag
  proto.text = get_text(html.text)
  proto.tail = get_text(html.tail)
  for key, val in html.attrib.iteritems():
    if key != 'style':
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
  positive = node.weight[_positive] / pos
  negative = node.weight[_negative] / neg
  node.score = positive - negative
  for child in node.children:
    score_normalized(child, pos, neg)
  return node


def score(node):
  # total_positive = node.weight[_positive]
  # total_negative = node.weight[_negative]
  return score_normalized(node, 1, 1)


# TODO: simplify

def find_best(node): #, pos, neg):
  best = document_pb2.HtmlElements(
    elements=[node], score=node.score)
  best_arr_i = document_pb2.HtmlElements()
  best_arr = None
  for child in node.children:
    child_best = find_best(child)
    if best.score < child_best.score:
      best = child_best
#     if child.score + best_arr_i.score < child.score:
#       del best_arr_i.elements[:]
#       best_arr_i.elements.extend([child])
#       best_arr_i.score = child.score
#     else:
#       best_arr_i.elements.extend([child])
#       best_arr_i.score += child.score
#     if not best_arr or best_arr.score < best_arr_i.score:
#       best_arr = best_arr_i
#   if best_arr and best.score < best_arr.score:
#     best = best_arr
  return best


def print_node(node):
  print node.score
  if node.text:
    print node.text.encode('utf-8').decode('string_escape')
  for child in node.children:
    print_node(child)
  if node.tail:
    print node.tail.encode('utf-8').decode('string_escape')
  

def print_nodes(nodes):
  for node in nodes.elements:
    print_node(node)


# TODO: write output

def main(argv):
  with db.Reader(FLAGS.input) as r:
    for url, raw in r:
      tree = html5lib.parse(raw, treebuilder="etree", namespaceHTMLElements=False)
      node = score(parse(tree))
      best = find_best(node)
       
      print url
      print
      print_nodes(best)
      print
      print


if __name__ == "__main__":
  flags.mark_flag_as_required("input")
  flags.mark_flag_as_required("output")
  app.run(main)
