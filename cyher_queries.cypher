// 1. Identify mutated genes and their mRNA and protein expression in specific patients
// Qui correlaiamo alterazioni genomiche (mutazioni) con livelli di espressione di mRNA e proteina nello stesso campione. Utile per studiare se una mutazione influisce sull’espressione del gene a livello trascrizionale e proteico.

MATCH (g:Gene)-[:HAS_ALTERATION]->(m:Mutation)-[:OBSERVED_IN]->(s:Sample)
MATCH (g)-[r1:HAS_EXPRESSION_IN]->(s)-[:FROM_PATIENT]->(p:Patient)
MATCH (g)-[:ENCODES]->(prot:Protein)
MATCH (prot)-[r2:HAS_EXPRESSION_IN]->(s)
//RETURN *
RETURN p.patientId, g.geneId, m.variantClassification AS MutationVariant, m.variantType AS VariantType, r1.mrnaExprValue AS mRNAExpression, r2.proteinExprValue AS ProteinExpression
ORDER BY p.patientId, g.geneId
LIMIT 100

// 11. Patient-centric multi-omics with clinical filters

MATCH (p:Patient)<-[:FROM_PATIENT]-(s:Sample)
OPTIONAL MATCH (s)<-[:OBSERVED_IN]-(m:Mutation)<-[:HAS_ALTERATION]-(g:Gene)
OPTIONAL MATCH (s)<-[:OBSERVED_IN]-(sv:StructuralVariant)<-[:HAS_ALTERATION]-(g)
OPTIONAL MATCH (s)<-[rCNA:HAS_ALTERATION_CNA_IN]-(g)
OPTIONAL MATCH (g)-[rMRNA:HAS_EXPRESSION_IN]->(s)
OPTIONAL MATCH (g)-[:ENCODES]->(prot:Protein)
OPTIONAL MATCH (prot)-[rProt:HAS_EXPRESSION_IN]->(s)

WHERE p.age >= 40 
  AND p.ajccPathologicTumorStage = 'II'
  AND p.radiationTherapy = true
  AND p.osStatus = 'alive'

RETURN 
  p.patientId AS PatientID,
  g.geneId AS Gene,
  s.sampleId AS SampleID,
  m.variantClassification AS Mutation,
  sv.structuralVariantId AS StructuralVariant,
  rCNA.cnaValue AS CNAValue,
  rMRNA.mrnaExprValue AS mRNAExpression,
  rProt.proteinExprValue AS ProteinExpression
ORDER BY p.patientId
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
WHERE r2.cnaValue = 2
OPTIONAL MATCH (p)-[r4:HAS_EXPRESSION_IN]->(s)
RETURN *

//9. Gene involved in a structural variant and without expression, related to encoded protein 
MATCH (g:Gene)-[r1:HAS_ALTERATION]->(sv:StructuralVariant)-[:OBSERVED_IN]->(s:Sample)
MATCH (g)-[r2:HAS_EXPRESSION_IN]->(s)
MATCH (g)-[r3:ENCODES]->(p:Protein)
MATCH (p)-[r4:HAS_EXPRESSION_IN]->(s)
WHERE r2.mrnaExprValue < -1
RETURN *
//RETURN g.geneId AS Gene, s.sampleId AS Sample, r2.mrnaExprValue AS mRNAExpression, r4.proteinExprValue AS ProteinExpression 
//LIMIT 100