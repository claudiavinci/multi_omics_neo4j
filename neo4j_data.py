from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd 
from dataset_converter import N_FILES, CSV_PATH
from pathlib import Path 

def create_gene_entity(csv_all):
    print("Creating Gene entity...")
    gene_df = csv_all["data_mutations.csv"][["Hugo_Symbol", "Entrez_Gene_Id", "Chromosome"]].dropna(subset=["Hugo_Symbol"]).copy() 
    proteins_genes = csv_all["data_protein_quantification_zscores.csv"][["Composite.Element.REF"]].dropna(subset=["Composite.Element.REF"]).copy()
    proteins_genes["Hugo_Symbol"] = proteins_genes["Composite.Element.REF"].str.split("|").str[0]
    cna_genes = csv_all["data_cna.csv"][["Hugo_Symbol", "Entrez_Gene_Id"]].dropna(subset=["Hugo_Symbol"]).copy()
    mrna_genes = csv_all["data_mrna_seq_v2_rsem_zscores_ref_all_samples.csv"][["Hugo_Symbol", "Entrez_Gene_Id"]].dropna(subset=["Hugo_Symbol"]).copy()
    sv1 = csv_all["data_sv.csv"][["Site1_Hugo_Symbol", "Site1_Chromosome"]].rename(columns={"Site1_Hugo_Symbol": "Hugo_Symbol", "Site1_Chromosome": "Chromosome"})
    sv2 = csv_all["data_sv.csv"][["Site2_Hugo_Symbol", "Site2_Chromosome"]].rename(columns={"Site2_Hugo_Symbol": "Hugo_Symbol", "Site2_Chromosome": "Chromosome"})
    sv_genes = pd.concat([sv1, sv2]).dropna(subset=["Hugo_Symbol"]).copy()

    for df, on_cols in [(cna_genes, ["Hugo_Symbol", "Entrez_Gene_Id"]),
                        (mrna_genes, ["Hugo_Symbol", "Entrez_Gene_Id"]),
                        (sv_genes, ["Hugo_Symbol", "Chromosome"]),
                        (proteins_genes["Hugo_Symbol"], ["Hugo_Symbol"])]:
        gene_df = gene_df.merge(df, on=on_cols, how="outer").drop_duplicates(subset=["Hugo_Symbol"])
    gene_df.columns = [col.lower() for col in gene_df.columns]
    gene_df = gene_df.rename(columns={
        "hugo_symbol": ":ID",
    })
    gene_df.insert(1, ":LABEL", "Gene")
    print(f"Created Gene entity with {len(gene_df)} records and columns: {gene_df.columns.tolist()}")
    return gene_df

def create_protein_entity(proteins_csv):
    print("Creating Protein entity...")
    protein_df = proteins_csv[["Composite.Element.REF"]].dropna(subset=["Composite.Element.REF"]).drop_duplicates().copy()
    protein_df["Protein_Name"] = protein_df["Composite.Element.REF"].str.split("|").str[1].copy()
    protein_df = protein_df.rename(columns={
        "Composite.Element.REF": ":ID",
        "Protein_Name": "protein_name"
    })
    protein_df.insert(1, ":LABEL", "Protein")
    print(f"Created Protein entity with {len(protein_df)} records and columns: {protein_df.columns.tolist()}")
    return protein_df

def create_patient_entity(patients_csv):
    print("Creating Patient entity...")
    patient_cols = [
        "PATIENT_ID",
        "AGE",
        "SEX",
        "CANCER_TYPE_ACRONYM",
        "SUBTYPE",
        "AJCC_PATHOLOGIC_TUMOR_STAGE",
        "PATH_T_STAGE",
        "PATH_N_STAGE",
        "PATH_M_STAGE",
        "HISTORY_NEOADJUVANT_TRTYN",
        "RADIATION_THERAPY",
        "GENETIC_ANCESTRY_LABEL",
        "OS_STATUS",
        "OS_MONTHS",
        "DAYS_LAST_FOLLOWUP"
    ] 
    patients_df = patients_csv[patient_cols].dropna(subset=["PATIENT_ID"]).copy()

    patients_df.columns = [col.lower() for col in patients_df.columns]
    patients_df = patients_df.rename(columns={
        "patient_id": ":ID"
    })
    patients_df.insert(1, ":LABEL", "Patient")
    print(f"Created Patient entity with {len(patients_df)} records and columns: {patients_df.columns.tolist()}")
    return patients_df

def create_sample_entity():
    pass

def create_sv_entity():
    pass

def create_relationship():
    pass

def read_CSV(file):
    df = pd.read_csv(file, sep=",", encoding='utf-8', low_memory=False)
    print(f"Reading file: {file.name} with {len(df)} records.")
    return file.name, df

if __name__ == "__main__":
    csv_all = {}
    # csv_all è un dizionario con chiave il nome del file e valore il dataframe corrispondente
    csv_files = list(Path(CSV_PATH).glob("*.csv"))
    with ThreadPoolExecutor(max_workers=N_FILES) as executor:
        futures = [executor.submit(read_CSV, file) for file in csv_files]
        for future in as_completed(futures):
            file_name, df = future.result()
            csv_all[file_name] = df

    gene_df =create_gene_entity(csv_all)
    protein_df = create_protein_entity(csv_all["data_protein_quantification_zscores.csv"])
    patient_df = create_patient_entity(csv_all["data_clinical_patient.csv"])

# per le relazioni dovrò rileggere tutti i file