# 🧬 Multi-Omics Knowledge Graph – TCGA-BRCA

A Knowledge Graph (KG) project for integrating and analyzing **multi-omics data** from the **TCGA-BRCA breast cancer dataset**.  
The graph-based approach connects genomic, transcriptomic, proteomic, and clinical information to support flexible exploration and discovery of biological relationships.

Project report: https://drive.google.com/file/d/1I-CtrcwzPNb3SlWiz6dRBLFwFnRwoEPR/view?usp=drive_link

## Overview

This project models heterogeneous omics data as a graph, where:
- biological and clinical entities → **nodes**
- interactions and associations → **relationships**

The Knowledge Graph enables complex queries and intuitive data exploration for cancer research and precision medicine applications.

## Workflow

1. **Data preprocessing**  
   Extraction and cleaning of key entities (genes, proteins, mutations, pathways, clinical samples)

2. **Graph construction**  
   Modeling and implementation of the KG in **Neo4j**

3. **Querying**  
   Example **Cypher** queries to explore interactions and biological patterns

## Technologies

- TCGA-BRCA dataset  
- Neo4j    
- Python  
- Docker

## Getting started
Run docker compose inside neo4j_container folder to get the Neo4j container
