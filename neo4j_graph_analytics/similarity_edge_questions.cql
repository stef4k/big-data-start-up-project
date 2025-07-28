// Documentation: https://neo4j.com/docs/graph-data-science/current/algorithms/node-similarity/

//Create projection
CALL gds.graph.project(
  'question-similarity-graph',
  ['Question', 'Keyword'],
  {
    HAS_KEYWORD: {
      type: 'HAS_KEYWORD',
      orientation: 'UNDIRECTED'
    }
  }
)

// Edge creation between similar nodes
CALL gds.nodeSimilarity.stream('question-similarity-graph', { similarityMetric: 'OVERLAP'})
YIELD node1, node2, similarity
WITH gds.util.asNode(node1) AS q1, gds.util.asNode(node2) AS q2, similarity
WHERE q1:Question AND q2:Question AND id(q1) < id(q2)  // avoid duplicates
  AND similarity >= 0.2 
CALL {
  WITH q1, q2, similarity
  MERGE (q1)-[s:IS_SIMILAR_QUESTION]-(q2)
  SET s.similarity = similarity
}
RETURN q1.title AS Question1, q2.title AS Question2, similarity
ORDER BY similarity DESC

