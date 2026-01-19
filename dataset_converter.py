from concurrent.futures import ThreadPoolExecutor
import pandas as pd 

N_FILES = 8
PATH = "./TCGA_BRCA_sel/data/"
CSV_PATH = "./TCGA_BRCA_sel/csv/"
FILES = ["data_clinical_patient.txt", 
         "data_clinical_sample.txt", 
         "data_cna.txt", 
         "data_mrna_seq_v2_rsem_zscores_ref_all_samples.txt", 
         "data_mutations.txt", 
         "data_protein_quantification_zscores.txt",
         "data_sv.txt"]

def dataset_to_CSV(filename):
    print(f"Opening {filename} ...")
    try:
        filepath = PATH + filename
        df = pd.read_csv(filepath, sep="\t", encoding='utf-8', low_memory=False, comment="#")
        df = df.dropna(how='all')
        output_file = filename.replace(".txt", ".csv")
        df.to_csv(CSV_PATH + output_file, index=False)
        print(f"Saved file: {output_file}",) 
    except Exception as e:
        print(f"Error processing {filename}: {e}")
    return


if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=N_FILES) as executor:
        executor.map(dataset_to_CSV, FILES)