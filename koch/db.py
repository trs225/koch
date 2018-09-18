"""Defines classes handling system i/o."""
from __future__ import absolute_import

import csv
import plyvel

from absl import logging


class Manager(object):

  defaults = {}

  def __init__(self, ctor, *args, **kwargs):
    self.ctor = ctor
    self.args = args
    self.kwargs = dict(self.defaults, **kwargs)
    self.db = None

  def __enter__(self):
    self.db = self.ctor(*self.args, **self.kwargs)
    return self

  def __exit__(self, *args):
    self.db.close()
    self.db = None

  def check(self):
    if not self.db:
      raise RuntimeError("Database is closed.")


class CsvReader(Manager):
  
  def __init__(self, path, col=None):
    super(CsvReader, self).__init__(open, path, 'r')
    self.col = col

  def __iter__(self):
    self.check()
    reader = csv.DictReader(self.db)
    if self.col and self.col not in reader.fieldnames:
      raise ValueError("Expected column %s in %s", self.col, self.path)
    for row in reader:
      yield row[self.col] if self.col else row


class Reader(Manager):
  
  def __init__(self, path, **kwargs):
    super(Reader, self).__init__(plyvel.DB, path, **kwargs)

  def __iter__(self):
    self.check()
    with self.db.iterator() as it:
      for key, value in it:
        yield key, value


class Writer(Manager):

  defaults = {
    'create_if_missing': True,
    'error_if_exists': True,
  }

  batch_size = 100

  def __init__(self, path, transaction=True, **kwargs):
    super(Writer, self).__init__(plyvel.DB, path, **kwargs)
    self.transaction = transaction
    self.batch = None
    self.n = None

  def __enter__(self):
    super(Writer, self).__enter__()
    self._start_batch()
    return self

  def __exit__(self, *args):
    self._end_batch(*args)
    super(Writer, self).__exit__(*args)

  def _start_batch(self):
    self.batch = self.db.write_batch(transaction=self.transaction)
    self.batch.__enter__()
    self.n = 0

  def _end_batch(self, *args):
    self.batch.__exit__(*args)
    self.batch = None
    self.n = None

  def _write_batch(self):
    self._end_batch(None, None, None)
    self._start_batch()

  def write(self, key, value):
    self.check()
    self.n += 1
    self.batch.put(key, value)
    if self.n > self.batch_size:
      logging.info("Writing %d keys.", self.n)
      self._write_batch()
