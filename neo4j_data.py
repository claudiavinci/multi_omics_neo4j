from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd 
from dataset_converter import N_FILES, CSV_PATH
from pathlib import Path

# csv_all["data_cna.csv"][["Hugo_Symbol"]],
# csv_all["data_mrna_seq_v2_rsem_zscores_ref_all_samples.csv"][["Hugo_Symbol"]],
# csv_all["data_sv.csv"][["Site1_Hugo_Symbol"]].rename(columns={"Site1_Hugo_Symbol": "Hugo_Symbol"}),
# csv_all["data_sv.csv"][["Site2_Hugo_Symbol"]].rename(columns={"Site2_Hugo_Symbol": "Hugo_Symbol"})    

def create_gene_entity(csv_all, proteins):
    gene_df = csv_all["data_mutations.csv"][["Hugo_Symbol", "Entrez_Gene_Id", "Chromosome"]].dropna(subset=["Hugo_Symbol"]).drop_duplicates()   
    
    cna_genes = csv_all["data_cna.csv"][["Hugo_Symbol", "Entrez_Gene_Id"]].dropna(subset=["Hugo_Symbol"]).drop_duplicates()
    mrna_genes = csv_all["data_mrna_seq_v2_rsem_zscores_ref_all_samples.csv"][["Hugo_Symbol", "Entrez_Gene_Id"]].dropna(subset=["Hugo_Symbol"]).drop_duplicates()
    sv1 = csv_all["data_sv.csv"][["Site1_Hugo_Symbol", "Site1_Chromosome"]].rename(columns={"Site1_Hugo_Symbol": "Hugo_Symbol", "Site1_Chromosome": "Chromosome"})
    sv2 = csv_all["data_sv.csv"][["Site2_Hugo_Symbol", "Site2_Chromosome"]].rename(columns={"Site2_Hugo_Symbol": "Hugo_Symbol", "Site2_Chromosome": "Chromosome"})
    sv_genes = pd.concat([sv1, sv2]).dropna(subset=["Hugo_Symbol"]).drop_duplicates()

    for df, on_cols in [(cna_genes, ["Hugo_Symbol", "Entrez_Gene_Id"]),
                        (mrna_genes, ["Hugo_Symbol", "Entrez_Gene_Id"]),
                        (sv_genes, ["Hugo_Symbol", "Chromosome"]),
                        (proteins, ["Hugo_Symbol"])]:
        gene_df = gene_df.merge(df, on=on_cols, how="outer")
    
    gene_df = gene_df.rename(columns={
        "Hugo_Symbol": ":ID",
        "Entrez_Gene_Id": "entrez_id",
        "Chromosome": "chromosome"
    })
    gene_df[":LABEL"] = "Gene"
    gene_df = gene_df[[":ID", ":LABEL", "entrez_id", "chromosome"]]
    return gene_df

def create_protein_entity(csv_all):
    protein_df = csv_all["data_protein_quantification_zscores.csv"][["Composite.Element.REF"]].dropna(subset=["Composite.Element.REF"]).drop_duplicates()
    # protein_df["Hugo_Symbol"] = protein_df["Composite.Element.REF"].str.split("|").str[0]
    protein_df["Protein_Name"] = protein_df["Composite.Element.REF"].str.split("|").str[1]
    protein_df[":LABEL"] = "Protein"
    protein_df = protein_df.rename(columns={
        "Composite.Element.REF": ":ID",
        # "Hugo_Symbol": "hugo_symbol", 
        "Protein_Name": "protein_name"
    })
    protein_df = protein_df[ [":ID", ":LABEL", "hugo_symbol", "protein_name"] ]
    return protein_df

def create_patient_entity():
    pass

def create_sample_entity():
    pass

def create_sv_entity():
    pass

def create_relationship():
    pass

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
    create_gene_entity(csv_all)
    # csv_all è un dizionario con chiave il nome del file e valore il dataframe corrispondente

# per i geni: da tutti i file devo prende Hugo_symbol e le proprietà cromosoma e posizione
# per le relazioni dovrò rileggere tutti i file