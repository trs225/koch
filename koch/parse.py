"""Fetches raw html content from urls.

TODO: chain module outputs
"""
from __future__ import absolute_import

import collections
import html5lib

from absl import app
from absl import flags
from absl import logging

from koch import db
from koch.proto import document_pb2

FLAGS = flags.FLAGS
flags.DEFINE_string("input", None, "Input raw html db to process.")
flags.DEFINE_string("output", None, "Output path to write parsed html to.")


# TODO: protos

Node = collections.namedtuple("Node", [
    "tag",
    "text",
    "tail",
    "attrib",
    "children",
    "score",
])

Nodes = collections.namedtuple("Nodes", [
    "nodes",
    "score",
])


def is_valid(html):
  if callable(html.tag):
    return False
  elif html.tag in ("script", "style"):
    return False
  else:
    return True


def text_len(unicode):
  if not unicode or unicode.encode("utf-8").decode("string_escape").isspace():
    return 0
  else:
    return len(unicode)


# TODO:
#  - try normalizing +/
#  - look into extra factor of 2
#  - consider dropping style attr

def measure(html):
  if not is_valid(html):
    return 0  # TODO: return None instead
  pos = text_len(html.text) + text_len(html.tail)
  neg = 2 * len(html.tag) + len(str(html.attrib or ""))
  return 2 * pos - neg


# TODO: write intermediate result

def parse(html):
  children = []
  score = measure(html)
  for child in html:
    if is_valid(child):
      parsed_child = parse(child)
      children.append(parsed_child)
      score += parsed_child.score
  return Node(
    tag=html.tag,
    text=html.text,
    tail=html.tail,
    attrib=html.attrib,
    children=children,
    score=score)


# TODO: simplify

def find_best(node):
  best = Nodes([node], node.score)
  best_arr = Nodes([], None)
  best_arr_i = Nodes([], 0)
  for child in node.children:
    child_best = find_best(child)
    if child_best.score > best.score:
      best = child_best
    if child.score > child.score + best_arr_i.score:
      best_arr_i = Nodes([child], child.score)
    else:
      best_arr_i = Nodes(
        best_arr_i.nodes + [child],
        best_arr_i.score + child.score)
    if best_arr_i.score > best_arr.score:
      best_arr = best_arr_i
  if best_arr.score > best.score:
    best = best_arr
  return best


def print_node(node):
    if node.text:
      print node.text.encode('utf-8').decode('string_escape')
    for child in node.children:
      print_node(child)
    if node.tail:
      print node.tail.encode('utf-8').decode('string_escape')
  

def print_nodes(nodes):
  for node in nodes.nodes:
    print_node(node)


# TODO: write output

def main(argv):
  with db.Reader(FLAGS.input) as r:
    for url, raw in r:
      tree = html5lib.parse(raw, treebuilder="etree", namespaceHTMLElements=False)
      node = parse(tree)
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
