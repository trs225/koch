"""Defines classes handling system i/o."""
from __future__ import absolute_import

import csv
import plyvel


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
    if hasattr(self.db, "close"):
      self.db.close()
    self.db = None

  def check(self):
    if not self.db:
      raise RuntimeError("Database is closed.")


class Reader(object):

  def __init__(self, manager):
    self.manager = manager

  def __enter__(self):
    self.manager.__enter__()
    return self

  def __exit__(self, *args):
    self.manager.__exit__(*args)

  def __iter__(self):
    raise NotImplementedError

  def get(self, key):
    raise NotImplementedError

  def map(self, value):
    return value


class JoiningReader(Reader):

  def __init__(self, reader, other_reader):
    super(JoiningReader, self).__init__(None)
    self.reader = reader
    self.other_reader = other_reader

  def __enter__(self):
    self.reader.__enter__()
    self.other_reader.__enter__()
    return self

  def __exit__(self, *args):
    self.reader.__exit__(*args)
    self.other_reader.__exit__(*args)

  def __iter__(self):
    for key, value in self.reader:
      other_value = self.other_reader.get(key)
      yield key, (value, other_value)


class DbReader(Reader):
  
  def __init__(self, path, **kwargs):
    super(DbReader, self).__init__(
        Manager(plyvel.DB, path, **kwargs))

  def __iter__(self):
    with self.manager.db.iterator() as it:
      for key, value in it:
        yield key, self.map(value)

  def get(self, key):
    return self.map(self.manager.db.get(key))


class ProtoDbReader(DbReader):
  
  def __init__(self, proto, path, **kwargs):
    super(ProtoDbReader, self).__init__(path, **kwargs)
    self.proto = proto
  
  def map(self, value):
      proto = self.proto()
      if value:
        proto.ParseFromString(value)
      return proto


class CsvReader(Reader):

  def __init__(self, path, key, val=None, **kwargs):
    super(CsvReader, self).__init__(Manager(open, path, "r"))
    self.key = key
    self.val = val

  def __iter__(self):
    for row in csv.DictReader(self.manager.db):
      yield row[self.key], row.get(self.val, row)


class DebugReader(Reader):

  def __init__(self, keys, values=None):
    super(DebugReader, self).__init__(None)
    self.keys = keys
    self.values = values or [None] * len(keys)

  def __enter__(self):
    return self

  def __exit__(self, *args):
    return 

  def __iter__(self):
    for k, v in zip(self.keys, self.values):
      yield k, self.map(v)


class Writer(object):

  def __init__(self, manager):
    self.manager = manager

  def __enter__(self):
    self.manager.__enter__()
    return self

  def __exit__(self, *args):
    self.manager.__exit__(*args)

  def map(self, value):
    return value

  def write(self, key, value):
    raise NotImplementedError


class DbWriter(Writer):

  defaults = {
    "create_if_missing": True,
    "error_if_exists": True,
    "max_file_size": 2 << 20,
    "write_buffer_size": 2 << 20,
  }

  def __init__(self, path, **kwargs):
    super(DbWriter, self).__init__(
        Manager(plyvel.DB, path, **dict(
            DbWriter.defaults, **kwargs)))

  def write(self, key, value):
    self.manager.check()
    self.manager.db.put(key, self.map(value))


class ProtoDbWriter(DbWriter):

  def __init__(self, proto, path, **kwargs):
    super(ProtoDbWriter, self).__init__(path, **kwargs)
    self.proto = proto

  def map(self, value):
    return value.SerializeToString()


class CsvWriter(Writer):

  def __init__(self, path, key, val, **kwargs):
    super(CsvWriter, self).__init__(Manager(open, path, "w"))
    self.fieldnames = [key, val]
    self.key = key
    self.val = val

  def __enter__(self):
    super(CsvWriter, self).__enter__()
    self.writer = csv.DictWriter(
        self.manager.db, self.fieldnames)
    return self

  def __exit__(self, *args):
    self.writer = None
    super(CsvWriter, self).__exit__(*args)

  def write(self, key, value):
    self.writer.writerow(
        {self.key: key, self.val: self.map(value)})


class FakeWriter(Writer):

  def __init__(self):
    super(FakeWriter, self).__init__(None)

  def __enter__(self):
    return self

  def __exit__(self, *args):
    return

  def write(self, key, value):
    return


class DebugWriter(FakeWriter):

  def write(self, key, value):
    print key, self.map(value)


class Rewriter(Reader, Writer):

  def __init__(self, reader, writer):
    super(Rewriter, self).__init__(writer.manager)
    reader.manager = writer.manager
    self.reader = reader
    self.writer = writer

  def __iter__(self):
    for out in self.reader:
      yield out

  def get(self, key):
    return self.reader.get(key)

  def write(self, key, value):
    return self.writer.write(key, value)
