from __future__ import absolute_import

from koch.proto import document_pb2


def get_text(document):
  return " ".join(blob.text for blob in document.blobs)
