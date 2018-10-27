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
    self.r = None
    self.w = None

  def __enter__(self):
    self.r = self.reader.__enter__()
    if self.w:
      self.w = self.writer.__enter__()

  def __exit__(self, *args):
    self.reader.__exit__(*args)
    self.r = None
    if self.w:
      self.writer.__exit__(*args)
      self.w = None

  def __iter__(self):
    with self:
      for k, v in self.r:
        yield k, v
      
  def run(self):
    for k, v in self:
      out = self.pipe(k, v)
      if out and self.w:
        self.w.write(*out)

  def pipe(self, key, value):
    raise NotImplementedError
