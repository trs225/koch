"""Fetches raw html content from urls.

TODO:
 - test default utf-8 encoding
 - try getting id_ archive url
 - side output for failed urls
"""
from __future__ import absolute_import

import contextlib
import csv
import html5lib
import pandas as pd
import random
import re2
import urllib2

from absl import app
from absl import flags
from absl import logging

from koch import db
from koch import pipeline
from koch import sample
from koch.proto import document_pb2

FLAGS = flags.FLAGS
flags.DEFINE_string("fetch_input", None, "Input path to csv of urls to fetch.")
flags.DEFINE_string("fetch_output", None, "Output path to write fetched html to.")
flags.DEFINE_multi_string("fetch_debug", None, "Input urls to debug.")

flags.DEFINE_string(
    "fetch_url_column", "URL for Content", "Name of url column in csv file.")
flags.DEFINE_string(
    "fetch_date_column", "Date of Content Posting", "Name of date column in csv file.")

_HEADERS = {
  "User-Agent": "Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11"
}


def fetch(url):
  try:
    r = urllib2.Request(url, headers=_HEADERS)
    with contextlib.closing(urllib2.urlopen(r)) as conn:
      encoding = conn.headers.getparam("charset") or "utf-8"
      logging.info("Fetching %s", url)
      return conn.read().decode(encoding)
  except Exception as e:
    logging.warning("Failed to open url %s: %s", url, str(e))


def to_epoch(string, format="%m/%d/%Y", unit="1s"):
  date = pd.to_datetime(string, format=format)
  return int((date - pd.Timestamp(0)) / pd.Timedelta(unit))


def is_valid(html):
  if callable(html.tag):
    return False
  elif html.tag in ("iframe", "noscript", "script", "style"):
    return False
  else:
    return True


def get_text(string):
  if string and not string.isspace():
    return re2.sub(r"\s+", " ", string)
  else:
    return ""


def to_html_element(html, proto):
  proto.tag = html.tag
  proto.text = get_text(html.text)
  proto.tail = get_text(html.tail)
  for key, val in html.attrib.iteritems():
    if key != "style":
      proto.attrib[key] = val
  return proto


def build_html_element(html, proto):
  proto = to_html_element(html, proto)
  for child in html:
    if is_valid(child):
      build_html_element(child, proto.children.add())
  return proto


class FetchingPipeline(pipeline.Pipeline):

  def __init__(self, url_column, date_column, reader, writer=None):
    super(FetchingPipeline, self).__init__(reader, writer)
    self.url_column = url_column
    self.date_column = date_column

  def pipe(self, key, value):
    url = value[self.url_column]
    date = value[self.date_column]

    doc = document_pb2.Document()
    doc.url = url
    doc.timestamp.seconds = to_epoch(date)
    doc.raw_html.url = value[self.url_column]
    doc.raw_html.html = fetch(url) or ""

    tree = html5lib.parse(
        doc.raw_html.html, treebuilder="etree", namespaceHTMLElements=False)
    build_html_element(tree, doc.parsed_html)

    yield url, doc


def main(argv):
  reader = db.CsvReader(FLAGS.fetch_input)
  writer = db.ProtoDbWriter(document_pb2.Document, FLAGS.fetch_output)
 
  if FLAGS.sample_number:
    random.seed(0)
    reader = sample.SamplingPipeline(FLAGS.sample_number, reader)

  if FLAGS.fetch_debug:
    reader = db.DebugReader(FLAGS.fetch_debug)

  if not FLAGS.fetch_output:
    writer = db.DebugWriter()

  FetchingPipeline(
      FLAGS.fetch_url_column, FLAGS.fetch_date_column, reader, writer).run()


if __name__ == "__main__":
  flags.mark_flag_as_required("fetch_input")
  flags.mark_flag_as_required("fetch_output")
  app.run(main)
