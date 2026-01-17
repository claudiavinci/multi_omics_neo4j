# Multi-Omics Neo4j Codebase Guide

## Project Architecture

This project builds a Neo4j graph database from TCGA (The Cancer Genome Atlas) multi-omics data. The pipeline follows a 3-stage flow:

1. **Data Conversion** (`dataset_converter.py`): Transforms tab-separated genomic data (`.txt`) into CSV format using parallel processing with ThreadPoolExecutor
2. **Neo4j Connection** (`connection.py`): Manages the Neo4j driver lifecycle and executes Cypher queries
3. **Data Loading** (`neo4j_data.py`, `main.py`): Will parse CSVs and create graph entities/relationships

## Key Components & Data Flow

### Dataset Converter (`dataset_converter.py`)
- **Input**: TCGA_BRCA_sel/data/*.txt files (tab-separated, UTF-8 encoded)
- **Output**: TCGA_BRCA_sel/csv/*.csv files
- **Pattern**: Uses `ThreadPoolExecutor` with `N_FILES` workers for parallel I/O. Drops all-NA rows.
- **Critical Path**: `dataset_to_CSV()` - handles encoding errors with `low_memory=False` flag

### Neo4j Connection (`connection.py`)
- Singleton pattern: `neo4jConnection` class wraps neo4j.GraphDatabase
- Constructor starts driver; explicit `closeConnection()` required
- `executeQuery(query, db=None)`: Executes Cypher queries, handles session lifecycle
- **Important**: Always close sessions in finally block; returns empty list on error (no exception raised)

### Docker Integration (`main.py`)
- Expects running Neo4j container named 'neo4j' 
- Connection: bolt://localhost:7474 with credentials neo4j/admin1234
- **Note**: Docker client uses environment variables; ensure docker socket is available

## Data Model (Planned)

From code comments in `neo4j_data.py`:
- **Gene nodes**: Extracted from all data files using `Hugo_symbol` with chromosome/position properties
- **Relationship types**: Multi-omics data creates relationships between samples and genes (mutations, CNA, mRNA, protein)
- **CSV files**: 7 primary datasets (clinical_patient, clinical_sample, cna, mrna_seq, mutations, protein_quantification, sv)

## Development Patterns

### Parallel Processing
Both data conversion and planned node creation use `ThreadPoolExecutor.map()` for concurrent file processing. Keep worker count equal to file count.

### Error Handling
- Converter: Catches and prints errors, continues execution (resilient)
- Connection: No exceptions raised on query failures - always check returned None/empty list
- **Pattern**: Defensive null checks required after `executeQuery()`

### File Organization
- Raw data: TCGA_BRCA_sel/data/ (tab-separated)
- Converted data: TCGA_BRCA_sel/csv/ (CSV format)
- Metadata: TCGA_BRCA_sel/meta/ (defines column semantics)
- Normal samples: TCGA_BRCA_sel/normals/ (reference for normalization)

## Critical Workflows

### Running Data Conversion
```bash
python dataset_converter.py
```
Processes all 7 .txt files in parallel. Check console for "Saved file" messages to verify completion.

### Testing Database Connection
```bash
python main.py  # Requires running Neo4j container
```

### Adding New Data Processing
1. Add filename to `FILES` list in dataset_converter.py (update `N_FILES`)
2. Import CSV path from `dataset_converter.CSV_PATH` in neo4j_data.py
3. Use ThreadPoolExecutor pattern for parallel processing (see existing code)

## Dependencies & Versions

Key packages:
- `neo4j==5.28.3` - Graph database driver
- `pandas==2.0.3` - Data manipulation (low_memory=False for large files)
- `docker==7.1.0` - Container management
- No external service authentication needed (local docker socket assumed)

## Italian Code Conventions

Project uses Italian variable/comment names (e.g., "chiave" for key). Maintain consistency when extending.
