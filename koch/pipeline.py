"""Fetches raw html content from urls.

TODO:
 - threshold for examination
 - chain module outputs
 - verify against heuristic
"""
from __future__ import absolute_import

from koch import db


class Pipeline(object):
  
  def __init__(self, reader, writer=None):
    self.reader = reader
    self.writer = writer

  def __enter__(self):
    self.reader.__enter__()
    if self.writer:
      self.writer.__enter__()

  def __exit__(self, *args):
    self.reader.__exit__(*args)
    if self.writer:
      self.writer.__exit__(*args)

  def __iter__(self):
    with self:
      for k, v in self.r:
        yield self.pipe(k, v)
      
  def run(self):
    for out in self:
      if out and self.writer:
        self.writer.write(*out)

  def pipe(self, key, value):
    return key, value
