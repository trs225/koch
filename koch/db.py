"""Defines classes handling system i/o."""
from __future__ import absolute_import

import csv
import plyvel

from absl import logging


class Manager(object):

  def __init__(self, ctor, *args, **kwargs):
    self.ctor = ctor
    self.args = args
    self.kwargs = kwargs
    self.db = None

  def __enter__(self):
    self.db = self.ctor(*self.args, **self.kwargs)
    return self

  def __exit__(self, *args):
    if hasattr(self.db, 'close'):
      self.db.close()
    self.db = None

  def check(self):
    if not self.db:
      raise RuntimeError("Database is closed.")


class Reader(object):

  def __enter__(self):
    return self

  def __exit__(self, *args):
    return

  def __iter__(self):
    raise NotImplementedError

  def get(self, key):
    raise NotImplementedError

  def map(self, key, value):
    return key, value


class DbReader(Reader):
  
  def __enter__(self):
    return self.manager.__enter__()

  def __exit__(self, *args):
    return self.manager.__exit__()

  def __init__(self, path, **kwargs):
    self.manager = Manager(plyvel.DB, path, **kwargs)

  def __iter__(self):
    with self.manager:
      with self.manager.db.iterator() as it:
        for key, value in it:
          yield self.map(key, value)


class ProtoDbReader(DbReader):
  
  def __init__(self, proto, path, **kwargs):
    super(ProtoDbReader, self).__init__(path, **kwargs)
    self.proto = proto
  
  def map(self, key, value):
    proto = self.proto()
    proto.ParseFromString(value)
    return key, proto


class CsvReader(Reader):

  def __init__(self, path, key, val=None, **kwargs):
    self.manager = Manager(open, path, "r")
    self.key = key
    self.val = val

  def __iter__(self):
    with self.manager:
      for row in csv.DictReader(self.manager.db):
        yield row[self.key], row.get(self.val, row)


class DebugReader(Reader):

  def __init__(self, keys, values=None):
    self.keys = keys
    self.values = values or [None] * len(keys)

  def __iter__(self):
    for k, v in zip(self.keys, self.values):
      yield self.map(k, v)


class Writer(object):

  def __enter__(self):
    return self

  def __exit__(self, *args):
    return

  def map(self, key, value):
    return key, value

  def write(self, key, value):
    raise NotImplementedError


class DbWriter(Writer):

  defaults = {
    "create_if_missing": True,
    "error_if_exists": True,
    "write_buffer_size": 2 * 1024 * 1024,
  }

  def __init__(self, path, **kwargs):
    self.manager = Manager(
        plyvel.DB, path, **dict(DbWriter.defaults, **kwargs))

  def __enter__(self):
    super(DbWriter, self).__enter__()
    self.manager.__enter__()
    return self

  def __exit__(self, *args):
    self.manager.__exit__()
    super(DbWriter, self).__exit__()

  def write(self, key, value):
    self.manager.check()
    self.manager.db.put(*self.map(key, value))


class ProtoDbWriter(DbWriter):

  def __init__(self, proto, path, **kwargs):
    super(ProtoDbWriter, self).__init__(path, **kwargs)
    self.proto = proto

  def map(self, key, value):
    return key, value.SerializeToString()


class CsvWriter(Writer):

  def __init__(self, path, key, val, **kwargs):
    self.manager = Manager(open, path, "w")
    self.fieldnames = [key, val]
    self.key = key
    self.val = val

  def __enter__(self):
    super(CsvWriter, self).__enter__()
    self.manager.__enter__()
    self.writer = csv.DictWriter(
        self.manager.db, self.fieldnames)
    return self

  def __exit__(self, *args):
    self.writer = None
    self.manager.__exit__()
    super(CsvWriter, self).__exit__()

  def write(self, key, value):
    key, value = self.map(key, value)
    self.writer.writerow(
        {self.key: key, self.val: value})


class DebugWriter(Writer):

  def write(self, key, value):
    print self.map(key, value)
