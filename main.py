import docker
from neo4j_connection import neo4jConnection
from dataset_handler import DatasetHandler
from neo4j_graph_builder import Neo4jGraphBuilder

N_FILES = 8
FILE_PATH = "./TCGA_BRCA_sel/data/"
RENAME_MAP = {
            "data_clinical_patient.txt": "patients",
            "data_clinical_sample.txt": "samples",
            "data_mutations.txt": "mutations",
            "data_protein_quantification_zscores.txt": "proteins",
            "data_cna.txt": "cna",
            "data_mrna_seq_v2_rsem_zscores_ref_all_samples.txt": "mrna",
            "data_sv.txt": "sv"
        }
NEO4J_IMPORT_PATH = "./../neo4j_container/.neo4j/import"

if __name__ == '__main__':
    
    # client = docker.from_env()
    # neo4jContainer = client.containers.get('neo4j')
    # neo4j = neo4jConnection(uri="bolt:localhost:7474", user="neo4j", password="admin1234")

    data_handler = DatasetHandler(files_path=FILE_PATH, n_files=N_FILES, rename_map=RENAME_MAP, savepath= NEO4J_IMPORT_PATH)
    all_data = data_handler.load_dataset()
    builder = Neo4jGraphBuilder(all_data)
    builder.build_graph()

    for e in builder.entities:
        print(builder.entities[e].head())
    for r in builder.relationships:
        print(builder.relationships[r].head())

    data_handler.save_CSV(builder.entities, "entities")
    data_handler.save_CSV(builder.relationships, "relationships")


