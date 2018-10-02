"""Fetches raw html content from urls.

TODO: Allow consecutive children instead of full subtree.
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


def is_valid(tree):
  if callable(tree.tag):
    return False
  if tree.tag in ('script', 'style'):
    return False
  return True


def text_len(unicode):
  if not unicode or unicode.encode('utf-8').decode('string_escape').isspace():
    return 0
  else:
    return len(unicode)


def measure(tree):
  if not is_valid(tree):
    return 0  # TODO: return None instead
  pos = text_len(tree.text) + text_len(tree.tail)
  neg = 2 * len(tree.tag) + len(str(tree.attrib or ''))
  return pos - neg


class Best(object):
  
  def __init__(self, tree, val):
    self.tree = tree
    self.val = val


def find_best(tree, best):
  children = 0
  for child in tree:
    children += find_best(child, best)
  val = measure(tree) + children
  if val > best.val:
    best.tree = tree
    best.val = val
  return val


def walk(tree):
  if not is_valid(tree):
    return
  if tree.text:
    print tree.text.encode('utf-8').decode('string_escape')
  for child in tree:
    walk(child)
  if tree.tail:
    print tree.tail.encode('utf-8').decode('string_escape')
  

def parse(tree):
  pass


def main(argv):
  with db.Reader(FLAGS.input) as r:
    for url, raw in r:
      tree = html5lib.parse(raw, treebuilder="etree", namespaceHTMLElements=False)
      best = Best(None, None)
      find_best(tree, best)

      print url
      print
      walk(best.tree)
      print
      print


if __name__ == "__main__":
  flags.mark_flag_as_required("input")
  flags.mark_flag_as_required("output")
  app.run(main)
