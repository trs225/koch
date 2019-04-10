"""Abstract pipeline representing processing step.

TODO:
 - enable writing from nested pipelines.
 - just use apache beam.
"""
from __future__ import absolute_import

from koch import db


class Pipeline(object):
  
  def __init__(self, reader, writer=None):
    self.reader = reader
    self.writer = writer or db.FakeWriter()

  def __enter__(self):
    self.reader.__enter__()
    self.writer.__enter__()
    return self

  def __exit__(self, *args):
    self.writer.__exit__(*args)
    self.reader.__exit__(*args)

  def __iter__(self):
    for k, v in self.reader:
      for key, val in self.pipe(k, v):
        yield key, val
      
  def run(self):
    with self:
      for key, val in self:
        self.writer.write(key, val)

  def pipe(self, key, value):
    return key, value


class CombiningPipeline(Pipeline):

  def __init__(self, reader, rewriter):
    super(CombiningPipeline, self).__init__(reader, rewriter)

  def __iter__(self):
    for k, v in self.reader:
      for piped in self.pipe(k, v):
        key, value = piped
        old_value = self.writer.get(key)
        new_value = self.combine(value, old_value)
        self.writer.write(key, new_value)

    for out in self.writer:
      yield out
      
  def run(self):
    with self:
      for out in self:
        pass

  def combine(self, value, old_value):
    raise NotImplementedError
