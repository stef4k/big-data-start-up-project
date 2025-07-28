CALL gds.graph.project(
  'user-similarity-graph',
  ['User', 'Topic'],
  {
    INTERESTED_IN: {
      type: 'INTERESTED_IN_TOPIC',
      orientation: 'UNDIRECTED'
    }
  }
)


CALL gds.nodeSimilarity.stream('user-similarity-graph', {similarityMetric: 'OVERLAP'})
YIELD node1, node2, similarity
WITH gds.util.asNode(node1) AS u1, gds.util.asNode(node2) AS u2, similarity
WHERE u1:User AND u2:User AND id(u1) < id(u2)  // avoid duplicates
  AND similarity >= 0.1
CALL {
  WITH u1, u2, similarity
  MERGE (u1)-[r:SIMILAR_USERS]-(u2)
  SET r.similarity = similarity
}
RETURN u1.name AS User1, u2.name AS User2, similarity
ORDER BY similarity DESC