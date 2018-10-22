"""Fetches raw html content from urls.

TODO:
 - try getting id_ archive url
 - side output for failed urls
"""
from __future__ import absolute_import

import contextlib
import csv
import urllib2

from absl import app
from absl import flags
from absl import logging

from koch import db

FLAGS = flags.FLAGS
flags.DEFINE_string("input", None, "Input path to csv of urls to fetch.")
flags.DEFINE_string("output", None, "Output path to write fetched html to.")

flags.DEFINE_string(
    "column", "URL for Content", "Name of url column in csv file.")


_HEADERS = {
  "User-Agent": "Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11"
}


def fetch(url):
  try:
    r = urllib2.Request(url, headers=_HEADERS)
    with contextlib.closing(urllib2.urlopen(r)) as conn:
      logging.info("Fetching %s", url)
      return conn.read()
  except Exception as e:
    logging.warning("Failed to open url %s: %s", url, str(e))


def main(argv):
  with db.CsvReader(FLAGS.input, FLAGS.column) as r:
    with db.Writer(FLAGS.output) as w:
      for url in r:
        html = fetch(url) or ""
        w.write(url, html.decode("string-escape"))


if __name__ == "__main__":
  flags.mark_flag_as_required("input")
  flags.mark_flag_as_required("output")
  app.run(main)
