from __future__ import absolute_import

from koch.proto import document_pb2


def GetText(document):
  return " ".join(blob.text for blob in document.blobs)


def IterWords(document):
  for blob in document.blobs:
    for word in blob.words:
      yield word
