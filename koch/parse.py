"""Fetches raw html content from urls."""
from __future__ import absolute_import

import html5lib

from absl import app
from absl import flags
from absl import logging

from koch import db
from koch.proto import document_pb2

FLAGS = flags.FLAGS
flags.DEFINE_string("input", None, "Input path to csv of urls to fetch.")
flags.DEFINE_string("output", None, "Output path to write fetched html to.")


def parse(raw):
  return html5lib.parse(raw, treebuilder="etree", namespaceHTMLElements=False)


_ALLOWED = set((
    'html', 'body', 'section', 'article', 'div', 'p',
    'h1', 'h2', 'h3', 'a', 'i', 'em', 'b'))

def walk(tree, level=0, out=[]):
  if tree.tag not in _ALLOWED:
    return
  if len(out) < level + 1:
    out.append(0)
  else:
    out[level] += 1
  if tree.text and not tree.text.encode('utf-8').decode('unicode_escape').isspace():
    print tree.tag, out[:level+1], tree.text
  for child in tree:
    walk(child, level=level+1, out=out)
  if tree.tail and not tree.tail.encode('utf-8').decode('unicode_escape').isspace():
    print tree.tag, out[:level+1], tree.tail


def main(argv):
  with db.Reader(FLAGS.input) as r:
    for url, raw in r:
      print
      print url
      print
      tree = parse(raw) or ''
      walk(tree)


if __name__ == "__main__":
  flags.mark_flag_as_required("input")
  flags.mark_flag_as_required("output")
  app.run(main)
