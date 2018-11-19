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


class Reader(Manager):
  
  def __init__(self, path, **kwargs):
    super(Reader, self).__init__(plyvel.DB, path, **kwargs)

  def __iter__(self):
    with self:
      with self.db.iterator() as it:
        for key, value in it:
          yield key, self.map(value)

  def map(self, value):
    return value


class ProtoReader(Reader):
  
  def __init__(self, proto, path, **kwargs):
    super(ProtoReader, self).__init__(path, **kwargs)
    self.proto = proto
  
  def map(self, value):
    out = self.proto()
    out.ParseFromString(value)
    return out


class DebugReader(object):

  def __init__(self, keys, values=None):
    self.keys = keys
    self.values = values or [None] * len(keys)

  def __iter__(self):
    for k, v in zip(self.keys, self.values):
      yield k, v


class Writer(Manager):

  defaults = {
    "create_if_missing": True,
    "error_if_exists": True,
    "write_buffer_size": 2 * 1024 * 1024,
  }

  def __init__(self, path, **kwargs):
    super(Writer, self).__init__(plyvel.DB, path, **kwargs)

  def write(self, key, value):
    self.check()
    self.db.put(key, self.map(value))

  def map(self, value):
    return value


class ProtoWriter(Writer):

  def __init__(self, proto, path, **kwargs):
    super(ProtoWriter, self).__init__(path, **kwargs)
    self.proto = proto

  def map(self, value):
    return value.SerializeToString()


class DebugWriter(object):

  def __enter__(self):
    return self

  def __exit__(self, *args):
    return

  def write(self, key, value):
    if key and value:
      print key, value
    elif key:
      print key


class CsvManager(Manager):
  
  def __init__(self, csv_ctor, path, mode, **kwargs):
    super(CsvManager, self).__init__(open, path, mode)
    self.csv_ctor = csv_ctor
    self.mode = mode
    self.csv_kwargs = kwargs
    self.csv = None

  def __enter__(self):
    super(CsvManager, self).__enter__()
    self.csv = self.csv_ctor(self.db, **self.csv_kwargs)
    return self

  def __exit__(self, *args):
    super(CsvManager, self).__exit__(*args)
    self.csv = None


class CsvReader(CsvManager):

  def __init__(self, path, key, val=None, **kwargs):
    super(CsvReader, self).__init__(csv.DictReader, path, "r")
    self.key = key
    self.val = val

  def __iter__(self):
    with self:
      if self.key and self.val:
        key, val = self.key, self.val
        for row in self.csv:
          yield row[key], row[val]
      elif self.key:
        for row in self.csv:
          yield row[self.key], None
      else:
        raise ValueError("Expected csv column key.")
    

class CsvWriter(CsvManager):

  def __init__(self, path, key, val, **kwargs):
    super(CsvWriter, self).__init__(
        csv.DictWriter, path, "w", fieldnames=[key, val])
    self.key = key
    self.val = val

  def check(self):
    super(CsvWriter, self).check()
    if not self.key or not self.val:
      raise ValueError("Missing key, value fieldnames to write.")

  def write(self, key, value):
    self.check()
    self.csv.writerow({self.key: key, self.val: value})
