"""Abstract pipeline representing processing step.

TODO:
 - enable writing from nested pipelines.
 - just use apache beam.
"""
from __future__ import absolute_import

from koch import db


class Pipeline(object):
  
  def __init__(self, reader, writer):
    self.reader = reader
    self.writer = writer

  def __iter__(self):
    for k, v in self.reader:
      yield self.pipe(k, v)
      
  def run(self):
    with self.writer:
      for out in self:
        if out: self.writer.write(*out)

  def pipe(self, key, value):
    return key, value
