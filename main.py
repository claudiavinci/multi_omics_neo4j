from pathlib import Path

from neo4j_connection import neo4jConnection
from dataset_handler import DatasetHandler
from neo4j_graph_builder import Neo4jGraphBuilder
from time import sleep

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
NEO4J_IMPORT_PATH = "./import/"
NEO4J_CONT_IMP_PATH = "/var/lib/neo4j/import/"

if __name__ == '__main__': 

# Building entities and relationships
    data_handler = DatasetHandler(files_path=FILE_PATH, n_files=N_FILES, rename_map=RENAME_MAP, savepath=NEO4J_IMPORT_PATH)
    # all_data = data_handler.load_dataset()
    # builder = Neo4jGraphBuilder(all_data)
    # builder.build_graph()

    # data_handler.save_CSV(builder.entities, "entities",)
    # data_handler.save_CSV(builder.relationships, "relationships")
    # data_handler.copy_CSV_to_container()
    neo4j = neo4jConnection(uri="bolt:localhost:7687", user="neo4j", password="admin1234", import_path=NEO4J_IMPORT_PATH, import_cont_path=NEO4J_CONT_IMP_PATH)
    neo4j.import_database()




