// User community detection using Louvain 

CALL gds.graph.project(
  'user-community-similarity-graph',
  'User',
  {
    SIMILAR_USERS: {
      properties: 'similarity',
      orientation: 'UNDIRECTED'

    }
  }
)

CALL gds.louvain.write('user-community-similarity-graph', {
  writeProperty: 'communityId',
  relationshipWeightProperty: 'similarity'
})
YIELD communityCount, modularity, modularities



// Example query
MATCH (u:User)-[r:INTERESTED_IN_TOPIC]->(t:Topic)
return u,r, t
//RETURN t.topic, count(*) AS frequency
//ORDER BY frequency DESC