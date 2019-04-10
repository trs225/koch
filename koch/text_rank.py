"""Uses TextRank to score document keywords.

TODO:
 - adjust edge builder
 - scores of isolated tokens
 - ranking evaluation
"""
from absl import app
from absl import flags
from absl import logging

from koch import db
from koch import parse
from koch import pipeline
from koch.proto import document_pb2
from koch.proto import text_rank_pb2


FLAGS = flags.FLAGS
flags.DEFINE_string(
    "text_rank_output", None, "Output path to write TextRank results to.")


def add_tokens(graph, document):
  tokens = {}
  for i, blob in enumerate(document.blobs):
    for word in blob.words:
      token = tokens.get(word.text) or tokens.setdefault(
          word.text, graph.tokens.add(text=word.text))
      token.mentions.add(token=word.index, blob=i)

  return tokens.values()


def get_adjacency_matrix(graph, edge_builder):
  """Returns a weighted adjacency matrix. Adds edges to graph."""
  index = range(len(graph.tokens))
  matrix = [[0 for j in index] for i in index]
  for i in index:
    for j in index:
      if i != j:
        from_token = graph.tokens[i]
        to_token = graph.tokens[j]
        weight = edge_builder(from_token, to_token)
        if weight:
          matrix[i][j] = weight
          graph.edges.add(
              from_token=from_token.text, weight=weight, to_token=to_token.text)

  return matrix


def get_text_rank_matrix(graph, edge_builder):
  """Returns an adjacency matrix reweighted by outgoing weights.

  r_{ij} = { w_{ij} / \sum_j w_{ij}, \sum_j w_{ij} > 0
           { 0,                      else
  """
  matrix = get_adjacency_matrix(graph, edge_builder)
  index = range(len(matrix))
  outgoing = [0.0 for j in index]
  for i in index:
    for j in index:
      outgoing[j] += matrix[i][j]

  for i in index:
    for j in index:
      matrix[i][j] = matrix[i][j] / outgoing[j] if outgoing[j] > 0 else 0

  return matrix


def text_rank(matrix, scores, damping_factor):
  """Returns a vector of new TextRank scores.

  s_i = (1 - d) + d * \sum_j w_{ij} * s_j
  """
  index = range(len(matrix))
  new_scores = [0.0 for j in index]
  for i in index:
    weight = 0.0
    for j in index:
      weight += matrix[i][j] * scores[j]
    new_scores[i] = (1 - damping_factor) / len(matrix) + damping_factor * weight

  return new_scores


def get_delta(scores, old_scores):
  """Returns the L2 norm of the difference of two vectors."""
  squares = 0
  for score, old_score in zip(scores, old_scores):
    squares += (score - old_score) ** 2

  return squares ** 0.5


def add_keywords(document, graph):
  for token in sorted(graph.tokens, key=lambda t: t.weight, reverse=True):
    document.keywords.add(word=token.text, text_rank=token.weight)


class TextRankPipeline(pipeline.Pipeline):

  def __init__(self, reader, writer=None):
    super(TextRankPipeline, self).__init__(reader, writer)
    self.convergence_threshold = 0.0001
    self.damping_factor = 0.85
    self.max_iterations = 100
    self.window = 10

  def pipe(self, key, value):
    doc = value
    graph = self.build_graph(doc)
    add_keywords(doc, graph)

    yield key, doc

  def build_graph(self, document):
    graph = text_rank_pb2.Graph()
    tokens = add_tokens(graph, document)
    matrix = get_text_rank_matrix(graph, self.build_edge)
    scores = [1.0 / len(matrix) for _ in range(len(matrix))]
    for i in range(self.max_iterations):
      new_scores = text_rank(matrix, scores, self.damping_factor)
      delta = get_delta(new_scores, scores)
      scores = new_scores
      if delta < self.convergence_threshold:
        break
    else:
      logging.warning("TextRank did not converge after %d iterations", i + 1)
      return graph

    for token, score in zip(tokens, scores):
      token.weight = score

    return graph

  def build_edge(self, from_token, to_token):
    weight = 0
    to_mentions = {m.blob: m for m in to_token.mentions}
    for from_mention in from_token.mentions:
      if from_mention.blob in to_mentions:
        distance = abs(from_mention.token - to_mentions[from_mention.blob].token)
        weight += 1.0 / distance if 0 < distance < self.window else 0

    return weight


def main(argv):
  parser = db.ProtoDbReader(document_pb2.Document, FLAGS.parse_output)
  writer = db.ProtoDbWriter(document_pb2.Document, FLAGS.text_rank_output)

  TextRankPipeline(parser, writer).run()


if __name__ == "__main__":
  flags.mark_flag_as_required("parse_output")
  flags.mark_flag_as_required("text_rank_output")
  app.run(main)
