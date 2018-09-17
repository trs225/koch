"""Fetches raw html content from urls."""
from __future__ import absolute_import

import contextlib
import csv
import urllib2

from absl import app
from absl import flags
from absl import logging

from koch.proto import document_pb2

FLAGS = flags.FLAGS
flags.DEFINE_string("input", None, "Input path to csv of urls to fetch.")
flags.DEFINE_string("output", None, "Output path to write fetched html to.")

flags.DEFINE_string(
    "column", "URL for Content", "Name of url column in csv file.")

def fetch(url):
  try:
    with contextlib.closing(urllib2.urlopen(url)) as conn:
      logging.info("Fetching %s", url)
      return conn.read()
  except urllib2.URLError as e:
    logging.warning("Failed to open url %s: %s", url, str(e))


class FileManager(object):

  def __init__(self, path, mode):
    self.path = path
    self.mode = mode
    self.file = None

  def __enter__(self):
    self.file = open(self.path, self.mode)
    return self

  def __exit__(self, *args):
    self.file.close()
    self.file = None

  def check(self):
    if not self.file:
      raise ValueError("Expected file to be open: %s", self.path)


class Reader(FileManager):

  def __init__(self, path, col):
    super(Reader, self).__init__(path, "r")
    self.col = col

  def __iter__(self):
    self.check()
    reader = csv.DictReader(self.file)
    if self.col not in reader.fieldnames:
      raise ValueError("Expected column %s in %s", self.col, self.path)
    for row in reader:
      url = row[self.col]
      yield url


def _format(*args):
  return ",".join(str(i) for i in args) + "\n"


class Writer(FileManager):

  def __init__(self, path):
    super(Writer, self).__init__(path, "w")

  def write(self, *content):
    self.check()
    self.file.write(
        _format(*content))


def main(argv):
  with Reader(FLAGS.input, FLAGS.column) as r:
    with Writer(FLAGS.output) as w:
      i = 0
      for url in r:
        if i > 9:
          break
        html = fetch(url) or ''
        w.write(url, html.encode('string-escape'))        
        i += 1


if __name__ == "__main__":
  flags.mark_flag_as_required("input")
  flags.mark_flag_as_required("output")
  app.run(main)
