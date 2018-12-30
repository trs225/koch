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
      out = self.pipe(k, v)
      if out: yield out
      
  def run(self):
    with self:
      for out in self:
        self.writer.write(*out)

  def pipe(self, key, value):
    return key, value
