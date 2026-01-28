from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd 
from pathlib import Path 
import os

class DatasetHandler:
    def __init__(self, files_path: str, n_files: int, rename_map: dict, savepath: str):
        self.files_path = Path(files_path)
        self.n_files = n_files
        self.rename_map = rename_map
        self.savepath = savepath

    def read_file(self, file: Path):
        df = pd.read_csv(file, sep="\t", encoding='utf-8', low_memory=False, comment="#")
        df = df.dropna(how='all')
        print(f"Reading file: {file.name} with {len(df)} records.")
        return file.name, df

    def load_dataset(self):
        print("Loading dataset...")
        data_all = {}
        # data_all è un dizionario con chiave il nome del file e valore il dataframe corrispondente
        data_files = list(self.files_path.glob("*.txt"))
        with ThreadPoolExecutor(max_workers=self.n_files) as executor:
            futures = [executor.submit(self.read_file, file) for file in data_files]
            for future in as_completed(futures):
                file_name, df = future.result()
                key = self.rename_map.get(file_name, Path(file_name).stem)
                data_all[key] = df.replace([".", "-", "NA", "N/A", "", " "], pd.NA)
        return data_all
    
    def save_file(self, name, df, data_type: str):
        filename = os.path.join(self.savepath + data_type, f'{name}.csv')
        df.to_csv(filename, index=False)
        print(f"Saved '{name}' to {filename}")
        
    def save_CSV(self, data: dict, data_type: str):
        print(f"Saving {data_type}...")
        with ThreadPoolExecutor(max_workers=self.n_files) as executor:
            futures = [executor.submit(self.save_file, name, df, data_type) for name, df in data.items()]
        for future in as_completed(futures):
            future.result()

    
