"""Fetches raw html content from urls.

TODO:
 - multithreaded requests
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
    "fetch_url_pattern", None, "URL pattern to restrict to.")
flags.DEFINE_string(
    "fetch_url_column", "URL for Content", "Name of url column in csv file.")
flags.DEFINE_string(
    "fetch_date_column", "Date of Content Posting", "Name of date column in csv file.")
flags.DEFINE_multi_string(
    "fetch_metadata_column", [], "Names of csv metadata columns to retain.")

_HEADERS = {
  "User-Agent": "Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11"
}


def fetch(url):
  try:
    r = urllib2.Request(url, headers=_HEADERS)
    with contextlib.closing(urllib2.urlopen(r)) as conn:
      html = conn.read()
      logging.info("Fetched url %s", url)
      encoding = conn.headers.getparam("charset") or "UTF-8"
      for en in (encoding, "ISO-8859-1", "Windows-1252", "ASCII"):
        try:
          return html.decode(en).encode("utf-8")
        except Exception as e:
          continue
      else:
        raise ValueError
  except Exception as e:
    logging.warning("Failed url %s: %s", url, str(e))


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


class UrlFilterPipeline(pipeline.Pipeline):

  def __init__(self, pattern, reader, writer=None):
    super(UrlFilterPipeline, self).__init__(reader, writer)
    self.pattern = re2.compile(pattern)

  def pipe(self, key, value):
    if key and self.pattern.match(key):
      yield key, value


class UrlRewritePipeline(pipeline.Pipeline):

  def __init__(self, reader, writer=None):
    super(UrlRewritePipeline, self).__init__(reader, writer)
    self.web_archive_pattern = re2.compile(r".*archive.org.*")
    self.id_pattern = re2.compile(r"/\d{14}/")
    self.id_string = "id_/"
    self.url_safe = ":/"

  def add_id(self, match):
    return match.string[match.start():match.end() - 1] + self.id_string

  def pipe(self, key, value):
    url = urllib2.quote(key, safe=self.url_safe)
    if self.web_archive_pattern.match(url):
      url = re2.sub(self.id_pattern, self.add_id, url, 1)

    yield url, value


class FetchingPipeline(pipeline.Pipeline):

  def __init__(
      self, date_column, metadata_columns, reader, writer=None):
    super(FetchingPipeline, self).__init__(reader, writer)
    self.date_column = date_column
    self.metadata_columns = metadata_columns

  def pipe(self, key, value):
    doc = document_pb2.Document()
    doc.url = key
    doc.timestamp.seconds = to_epoch(value[self.date_column])
    for col in self.metadata_columns:
      if value[col]:
        doc.metadata[col] = value[col]

    doc.raw_html.url = key
    doc.raw_html.html = fetch(key) or ""

    tree = html5lib.parse(
        doc.raw_html.html, treebuilder="etree", namespaceHTMLElements=False)
    build_html_element(tree, doc.parsed_html)

    yield key, doc


def main(argv):
  reader = UrlRewritePipeline(
      db.CsvReader(FLAGS.fetch_input, FLAGS.fetch_url_column))
  writer = db.ProtoDbWriter(document_pb2.Document, FLAGS.fetch_output)

  if FLAGS.fetch_url_pattern:
    reader = UrlFilterPipeline(FLAGS.fetch_url_pattern, reader)
 
  if FLAGS.sample_number:
    random.seed(0)
    reader = sample.SamplingPipeline(FLAGS.sample_number, reader)

  if FLAGS.fetch_debug:
    reader = db.DebugReader(FLAGS.fetch_debug)

  if not FLAGS.fetch_output:
    writer = db.DebugWriter()

  FetchingPipeline(
      FLAGS.fetch_date_column, FLAGS.fetch_metadata_column, reader, writer).run()


if __name__ == "__main__":
  flags.mark_flag_as_required("fetch_input")
  flags.mark_flag_as_required("fetch_output")
  app.run(main)
