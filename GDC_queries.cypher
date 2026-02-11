// 1. Drop multiomics graph projection
CALL gds.graph.drop('multiomics')

// 2. Create multiomics graph projection
CALL gds.graph.project('multiomics', '*', '*')


// 3. KG PageRank (centrality)
CALL gds.pageRank.write('multiomics', {
  writeProperty: 'pagerank_KG'
})
YIELD nodePropertiesWritten

// 4. KG PageRank Results

MATCH (n)
WHERE n.pagerank_KG IS NOT NULL
WITH
  head(labels(n)) AS type,
  n.pagerank_KG AS rank,
  CASE
    WHEN 'Patient' IN labels(n) THEN n.patientId
    WHEN 'Sample' IN labels(n) THEN n.sampleId
    WHEN 'Gene' IN labels(n) THEN n.geneId
    WHEN 'Protein' IN labels(n) THEN n.proteinId
    WHEN 'Mutation' IN labels(n) THEN n.mutationId
    WHEN 'StructuralVariant' IN labels(n) THEN n.structuralVariantId
    ELSE id(n)
  END AS identifier
RETURN type, identifier, rank
ORDER BY rank DESC
LIMIT 100;

// 5. Identify Louvain communities

CALL gds.louvain.write('multiomics', {
    writeProperty: 'communityId'
})
YIELD communityCount;

// 6. Visualize communities found

MATCH (n)
WHERE n.communityId IS NOT NULL
RETURN n.communityId AS communityId, count(n) AS countNode
ORDER BY countNode DESC

// MATCH (n)
// WHERE n.communityId IS NOT NULL
// WITH
//   head(labels(n)) AS type,
//   n.communityId AS community,
//   CASE
//     WHEN 'Patient' IN labels(n) THEN n.patientId
//     WHEN 'Sample' IN labels(n) THEN n.sampleId
//     WHEN 'Gene' IN labels(n) THEN n.geneId
//     WHEN 'Protein' IN labels(n) THEN n.proteinId
//     WHEN 'Mutation' IN labels(n) THEN n.mutationId
//     WHEN 'StructuralVariant' IN labels(n) THEN n.structuralVariantId
//     ELSE id(n)
//   END AS identifier
// RETURN type, identifier, community
// ORDER BY community, type;

// 6.1 Graph view of found communities
MATCH (n)-[r]->(m)
WHERE n.communityId IS NOT NULL
RETURN n, r, m
LIMIT 1000;


// 7. Communities PageRank

MATCH (n)
WHERE n.communityId IS NOT NULL
WITH DISTINCT n.communityId as cid 

CALL {
    WITH cid
    // creo proiezione del sottografo della singola community
    CALL gds.graph.project.cypher(
        // nome proiezione
        'comm_' + toString(cid),
        // query nodi
        'MATCH (n) WHERE n.communityId = $cid RETURN id(n) AS id',
        // query relazioni
        'MATCH (n)-[r]->(m) WHERE n.communityId = $cid AND m.communityId = $cid RETURN id(n) AS source, id(m) AS target',
        {parameters: {cid: cid}}
    )
    YIELD graphName
    
    //PageRank dentro la community
    CALL gds.pageRank.write(graphName, {
        writeProperty: 'pagerank_comm'
    })
    YIELD nodePropertiesWritten

    // drop temp. graph
    CALL gds.graph.drop(graphName)
    YIELD graphName AS droppedGraph

    RETURN cid AS commId, nodePropertiesWritten
}

RETURN commId AS communityId, nodePropertiesWritten
ORDER BY communityId


// 7.1 Visualize communities PageRank
MATCH (n)
WHERE n.pagerank_comm IS NOT NULL
WITH
  head(labels(n)) AS type,
  n.communityId AS community,
  n.pagerank_comm AS rank,
  CASE
    WHEN 'Patient' IN labels(n) THEN n.patientId
    WHEN 'Sample' IN labels(n) THEN n.sampleId
    WHEN 'Gene' IN labels(n) THEN n.geneId
    WHEN 'Protein' IN labels(n) THEN n.proteinId
    WHEN 'Mutation' IN labels(n) THEN n.mutationId
    WHEN 'StructuralVariant' IN labels(n) THEN n.structuralVariantId
    ELSE id(n)
  END AS identifier
  RETURN community AS communityId, type, identifier, rank AS pageRankInCommunity
  ORDER BY community, rank DESC
  LIMIT 100

// 8. Average PageRank for each community
MATCH (n)
WHERE n.pagerank_comm IS NOT NULL
RETURN n.communityId AS communityId, count(n) AS nodeCount, avg(n.pagerank_comm) AS avgPageRankInCommunity
ORDER BY nodeCount DESC

// 9. Compare KG PageRank and Community PageRank
MATCH (n)
WHERE n.pagerank_comm IS NOT NULL AND n.pagerank_KG IS NOT NULL
WITH
head(labels(n)) AS type,
  n.communityId AS communityId,
  n.pagerank_comm AS pageRankInCommunity,
  n.pagerank_KG AS pageRankInKG,
  CASE
    WHEN 'Patient' IN labels(n) THEN n.patientId
    WHEN 'Sample' IN labels(n) THEN n.sampleId
    WHEN 'Gene' IN labels(n) THEN n.geneId
    WHEN 'Protein' IN labels(n) THEN n.proteinId
    WHEN 'Mutation' IN labels(n) THEN n.mutationId
    WHEN 'StructuralVariant' IN labels(n) THEN n.structuralVariantId
    ELSE id(n)
  END AS identifier
RETURN communityId, type, identifier, pageRankInKG, pageRankInCommunity
ORDER BY communityId, pageRankInKG DESC
LIMIT 500