from pathlib import Path
import docker
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
NEO4J_IMPORT_PATH = "./../neo4j_container/.neo4j/import/"

if __name__ == '__main__': 

# Building entities and relationships
    # data_handler = DatasetHandler(files_path=FILE_PATH, n_files=N_FILES, rename_map=RENAME_MAP, savepath= NEO4J_IMPORT_PATH)
    # all_data = data_handler.load_dataset()
    # builder = Neo4jGraphBuilder(all_data)
    # builder.build_graph()

    # data_handler.save_CSV(builder.entities, "entities",)
    # data_handler.save_CSV(builder.relationships, "relationships")
    
    entities = list(Path(f'{NEO4J_IMPORT_PATH}/entities/').glob("*.csv"))
    relationships = list(Path(f'{NEO4J_IMPORT_PATH}/relationships/').glob("*.csv"))
    entities_import = ' '.join([f'--nodes /import/entities/{entity.name}' for entity in entities])
    rel_import = ' '.join([f'--relationships /import/relationships/{rel.name}' for rel in relationships])
    full_import_string = f'neo4j-admin database import full --delimiter="," --array-delimiter="|" --overwrite-destination {entities_import} {rel_import}'

    client = docker.from_env()
    neo4jContainer = client.containers.get('neo4j')
    print("Stopping neo4j container before importing data...")
    neo4jContainer.exec_run('neo4j stop')
    print("Importing graph (nodes and relationships) into neo4j database...")
    neo4jContainer.exec_run(full_import_string)
    print("Starting neo4j container...")
    neo4jContainer.exec_run('neo4j start')
    print("Waiting 10 seconds before connecting...")
    sleep(10)

    neo4j = neo4jConnection(uri="bolt:localhost:7687", user="neo4j", password="admin1234", import_path=NEO4J_IMPORT_PATH)



