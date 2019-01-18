"""Fetches raw html content from urls.

TODO:
 - test default utf-8 encoding
 - try getting id_ archive url
 - side output for failed urls
"""
from __future__ import absolute_import

import contextlib
import csv
import datetime
import random
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


def to_epoch_seconds(string, fmt="%m/%d/%Y"):
  date = datetime.datetime.strptime(string, fmt)
  return int((date - datetime.datetime.fromtimestamp(0)).total_seconds())


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
    doc.timestamp.seconds = to_epoch_seconds(date)
    doc.raw_html.url = value[self.url_column]
    doc.raw_html.html = fetch(url) or ""

    yield url, doc


def main(argv):
  reader = db.CsvReader(FLAGS.fetch_input)
  writer = db.ProtoDbWriter(document_pb2.Document, FLAGS.fetch_output)
 
  if FLAGS.sample_number:
    random.seed(0)
    reader = sample.SamplingPipeline(FLAGS.sample_number, reader)

  if FLAGS.fetch_debug:
    reader = db.DebugReader(FLAGS.fetch_debug)
    writer = db.DebugWriter()

  FetchingPipeline(
      FLAGS.fetch_url_column, FLAGS.fetch_date_column, reader, writer).run()


if __name__ == "__main__":
  flags.mark_flag_as_required("fetch_input")
  flags.mark_flag_as_required("fetch_output")
  app.run(main)
