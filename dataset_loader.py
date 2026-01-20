from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd 
from pathlib import Path 

class DatasetLoader:
    def __init__(self, files_path: str, n_files: int):
        self.files_path = Path(files_path)
        self.n_files = n_files

    def _read_file(self, file: Path):
        df = pd.read_csv(file, sep="\t", encoding='utf-8', low_memory=False, comment="#")
        df = df.dropna(how='all')
        print(f"Reading file: {file.name} with {len(df)} records.")
        return file.name, df

    def load_dataset(self):
        data_all = {}
        # data_all è un dizionario con chiave il nome del file e valore il dataframe corrispondente
        data_files = list(self.files_path.glob("*.txt"))
        with ThreadPoolExecutor(max_workers=self.n_files) as executor:
            futures = [executor.submit(self._read_file, file) for file in data_files]
            for future in as_completed(futures):
                file_name, df = future.result()
                data_all[file_name] = df
        return data_all
