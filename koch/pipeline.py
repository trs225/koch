"""Abstract pipeline representing processing step.

TODO:
 - enable writing from nested pipelines.
"""
from __future__ import absolute_import

from koch import db


class Pipeline(object):
  
  def __init__(self, reader, writer=None):
    self.reader = reader
    self.writer = writer

  def __enter__(self):
    if self.writer:
      self.writer.__enter__()

  def __exit__(self, *args):
    if self.writer:
      self.writer.__exit__(*args)

  def __iter__(self):
    with self:
      for k, v in self.reader:
        yield self.pipe(k, v)
      
  def run(self):
    for out in self:
      if out and self.writer:
        self.writer.write(*out)

  def pipe(self, key, value):
    return key, value
