from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd 
from dataset_converter import N_FILES, CSV_PATH
from pathlib import Path

# CREAZIONE DEI NODI -> args:
# 1. dizionario che conterrà le entità
# 2. chiave che identifica l'identità
# 3. 

def read_CSV(file):
    df = pd.read_csv(file, sep=",", encoding='utf-8', low_memory=False)
    print(f"Read file: {file.name} with {len(df)} records.")
    return file.name, df

if __name__ == "__main__":
    csv_all = {}
    csv_files = list(Path(CSV_PATH).glob("*.csv"))

    with ThreadPoolExecutor(max_workers=N_FILES) as executor:
        futures = [executor.submit(read_CSV, file) for file in csv_files]
        for future in as_completed(futures):
            file_name, df = future.result()
            csv_all[file_name] = df
    print(csv_all)

    # csv_all è un dizionario con chiave il nome del file e valore il dataframe corrispondente

# per i geni: da tutti i file devo prende Hugo_symbol e le proprietà cromosoma e posizione
# per le relazioni dovrò rileggere tutti i file