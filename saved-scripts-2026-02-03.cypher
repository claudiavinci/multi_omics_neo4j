// 1. Identify mutated genes and their mRNA and protein expression in specific patients
MATCH (g:Gene)-[:HAS_ALTERATION]->(m:Mutation)-[:OBSERVED_IN]->(s:Sample)
MATCH (g)-[r1:HAS_EXPRESSION_IN]->(s)-[:FROM_PATIENT]->(p:Patient)
MATCH (g)-[:ENCODES]->(prot:Protein)
MATCH (prot)-[r2:HAS_EXPRESSION_IN]->(s)
//RETURN *
RETURN p.patientId, g.geneId, m.variantClassification AS MutationVariant, m.variantType AS VariantType, r1.mrnaExprValue AS mRNAExpression, r2.proteinExprValue AS ProteinExpression
ORDER BY p.patientId, g.geneId
LIMIT 100

// 10. Patients with several SVs and their effects on gene expression
MATCH (p:Patient)<-[:FROM_PATIENT]-(s:Sample)
MATCH (g:Gene)-[r:HAS_ALTERATION]->(sv:StructuralVariant)-[:OBSERVED_IN]->(s)
MATCH (g)-[r2:HAS_EXPRESSION_IN]->(s)
WITH p, g, s, sv, r2, count(DISTINCT sv) AS numSV
RETURN p.patientId AS Patient, sv.structuralVariantId as SV, g.geneId AS Gene, r2.mrnaExprValue AS mRNAExpression, s.sampleId AS Sample
ORDER BY numSV DESC
LIMIT 100

// 2. Analyze structural variants among different genes in the same sample
MATCH (g1:Gene)-[r1:HAS_ALTERATION]->(sv1:StructuralVariant)-[o1:OBSERVED_IN]->(s:Sample)
MATCH (g2:Gene)-[r2:HAS_ALTERATION]->(sv2:StructuralVariant)-[o2:OBSERVED_IN]->(s)
WHERE r1.site <> r2.site AND sv1 = sv2
//RETURN *
RETURN sv1 AS StructuralVariant, g1 as Gene1, g2 as Gene2, s.sampleId AS Sample  
LIMIT 100

// 3. Patients with multiple alterations: CNA, mutations and SV
MATCH (p:Patient)<-[FROM_PATIENT]-(s:Sample)
MATCH (s)<-[:OBSERVED_IN]-(m:Mutation)<-[:HAS_ALTERATION]-(g:Gene)
MATCH (s)<-[:OBSERVED_IN]-(sv:StructuralVariant)<-[:HAS_ALTERATION]-(g)
MATCH (s)<-[r:HAS_ALTERATION_CNA_IN]-(g)
WITH p, g,
     COUNT(DISTINCT m) AS NumMutations,
     COUNT(DISTINCT sv) AS NumSVs,
     COUNT(r) AS NumCNA
WHERE NumMutations + NumSVs + NumCNA > 1
RETURN p.patientId AS Patient, 
    g.geneId AS Gene, NumMutations, NumSVs, NumCNA
ORDER BY p.patientId, g.geneId
LIMIT 100

// 4. Get genes involved in a missense mutations in a certain sample/patient and their expression level
MATCH (g:Gene)-[r1:HAS_ALTERATION]->(m:Mutation)-[r2:OBSERVED_IN]->(s:Sample)-[r3:FROM_PATIENT]->(p:Patient)
MATCH (g)-[r4:HAS_EXPRESSION_IN]->(s)
WHERE p.age > 50 AND m.variantClassification = 'missense mutation'
RETURN g.geneId AS Gene, p.patientId AS Patient, s.sampleId AS Sample, r4.mrnaExprValue AS mRNAExpression
ORDER BY g.geneId, p.patientId
LIMIT 100

// 5. Correlation between mRNA and protein expression for the same gene in a sample
MATCH (g:Gene)-[r1:ENCODES]->(p:Protein)
MATCH (g)-[r2:HAS_EXPRESSION_IN]->(s:Sample)
MATCH (p)-[r3:HAS_EXPRESSION_IN]->(s)
RETURN g.geneId as Gene, s.sampleId as Sample, r2.mrnaExprValue as mRNAExpression, r3.proteinExprValue as ProteinExpression
ORDER BY g.geneId, s.sampleId
LIMIT 100

// 6. Get proteins encoded by a gene affected by a CNA amplification in all samples from patient TCGA-A1-A0SK
MATCH (p:Protein)<-[r1:ENCODES]-(g:Gene)
MATCH (g:Gene)-[r2:HAS_ALTERATION_CNA_IN]->(s:Sample)-[r3:FROM_PATIENT]->(pa:Patient {patientId: 'TCGA-A1-A0SK'})
OPTIONAL MATCH (p)-[r4:HAS_EXPRESSION_IN]->(s)
WHERE r2.cnaValue = 2
RETURN *

//7. Get highly expressed genes having a mutation in samples from patient TCGA-3C-AAAU
MATCH (p:Patient {patientId: "TCGA-3C-AAAU"})<-[r1:FROM_PATIENT]-(s:Sample)
MATCH (g:Gene)-[r2:HAS_ALTERATION]->(m:Mutation)-[r3:OBSERVED_IN]->(s)
MATCH (g)-[r4:HAS_EXPRESSION_IN]->(s)
//WHERE r4.mrnaExprValue > 2
RETURN p, s, g, m, r1, r2, r3

// 8. CNA effect on mRNA Expression
MATCH (g:Gene)-[r1:HAS_ALTERATION_CNA_IN]->(s:Sample)
MATCH (g)-[r2:HAS_EXPRESSION_IN]->(s)
//WHERE r1.cnaValue = 2
RETURN g.geneId AS Gene, r1.cnaValue AS CNAValue, r2.mrnaExprValue AS mRNAExpression
ORDER BY g.geneId
LIMIT 100

//9. Gene involved in a mutation and without expression 
MATCH (g:Gene)-[:HAS_ALTERATION]->(m:Mutation)-[:OBSERVED_IN]->(s:Sample)
MATCH (g)-[r:HAS_EXPRESSION_IN]->(s)
WHERE r.mrnaExprValue < -1
RETURN g.geneId AS Gene, s.sampleId AS Sample, r.mrnaExprValue AS mRNAExpression 
LIMIT 100
