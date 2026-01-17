from concurrent.futures import ThreadPoolExecutor
import pandas as pd 
from dataset_converter import N_FILES, CSV_PATH

CSV_FILES = ["data_clinical_patient.csv", 
         "data_clinical_sample.csv", 
         "data_cna.csv", 
         "data_mrna_seq_v2_rsem_zscores_ref_all_samples.csv", 
         "data_mutations.csv", 
         "data_protein_quantification_zscores.csv",
         "data_sv.csv"]

# CREAZIONE DEI NODI -> args:
# 1. dizionario che conterrà le entità
# 2. chiave che identifica l'identità
# 3. 

# def read_CSV(path, csv_list):
#     return

# def createNode(entities, key: str, ):
#     return

# if __name__ == "__main__":
#     csv_list = {}
#     with ThreadPoolExecutor(max_workers=N_FILES) as executor:
#         executor.submit()

# per i geni: da tutti i file devo prende Hugo_symbol e le proprietà cromosoma e posizione
# per le relazioni dovrò rileggere tutti i file