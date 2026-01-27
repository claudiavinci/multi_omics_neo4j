import pandas as pd 
from utilities import format_string

def format_entity(df, label, id_col=None, id_list=None, drop_cols: list = None, to_lower: list = None):
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True)
    if to_lower:
        for col in to_lower:
            df[col] = df[col].str.lower()
    df.columns = [format_string(col, "camel") for col in df.columns]
    camel_new_id = format_string(label, "camel") + "Id:ID"
    pascal_label = format_string(label, "pascal")

    if id_col is None and id_list is not None:
        df.insert(0, camel_new_id, id_list)
    elif id_col is not None:
        df = df.rename(columns={format_string(id_col, "camel"): camel_new_id})
    else:
        raise ValueError("Either id_col or id_list must be provided.")

    df.insert(1, ":LABEL", pascal_label)
    df.replace([".", "-", "NA", "N/A", "", " "], pd.NA, inplace=True)
    print(f"Created {pascal_label} entity with {len(df)} records and {len(df.columns)} columns")
    return df

def create_gene_entity(all_data):
    gene_df = all_data["mutations"][["Hugo_Symbol", "Entrez_Gene_Id", "Chromosome"]].dropna(subset=["Hugo_Symbol"]).copy() 
    proteins_genes = all_data["proteins"][["Composite.Element.REF"]].dropna().copy()
    proteins_genes["Hugo_Symbol"] = proteins_genes["Composite.Element.REF"].str.split("|").str[0]
    cna_genes = all_data["cna"][["Hugo_Symbol", "Entrez_Gene_Id"]].dropna(subset=["Hugo_Symbol"]).copy()
    mrna_genes = all_data["mrna"][["Hugo_Symbol", "Entrez_Gene_Id"]].dropna(subset=["Hugo_Symbol"]).copy()
    sv1 = all_data["sv"][["Site1_Hugo_Symbol"]].rename(columns={"Site1_Hugo_Symbol": "Hugo_Symbol"})
    sv2 = all_data["sv"][["Site2_Hugo_Symbol"]].rename(columns={"Site2_Hugo_Symbol": "Hugo_Symbol"})
    sv_genes = pd.concat([sv1, sv2]).dropna(subset=["Hugo_Symbol"]).copy()

    for df, on_cols in [(cna_genes, ["Hugo_Symbol", "Entrez_Gene_Id"]),
                        (mrna_genes, ["Hugo_Symbol", "Entrez_Gene_Id"]),
                        (sv_genes, ["Hugo_Symbol"]),
                        (proteins_genes["Hugo_Symbol"], ["Hugo_Symbol"])]:
        gene_df = gene_df.merge(df, on=on_cols, how="outer").drop_duplicates()

    gene_df = format_entity(gene_df, "Gene", "Hugo_Symbol")
    return gene_df

def create_protein_entity(proteins):
    df = proteins[["Composite.Element.REF"]].dropna(subset=["Composite.Element.REF"]).drop_duplicates().copy()
    df["Protein_Name"] = df["Composite.Element.REF"].str.split("|").str[1].copy()
    df = format_entity(df, "Protein", "Composite.Element.REF")
    return df

def create_patient_entity(patients):
    patient_cols = [
        "PATIENT_ID",
        "SUBTYPE",
        "CANCER_TYPE_ACRONYM",
        "AGE",
        "SEX",
        "AJCC_PATHOLOGIC_TUMOR_STAGE",
        "AJCC_STAGING_EDITION",
        "PATH_T_STAGE",
        "PATH_N_STAGE",
        "PATH_M_STAGE",
        "HISTORY_NEOADJUVANT_TRTYN",
        "RADIATION_THERAPY",
        "OS_STATUS",
        "OS_MONTHS",
        "DAYS_LAST_FOLLOWUP",
        "GENETIC_ANCESTRY_LABEL",
    ] 
    df = patients[patient_cols].dropna(subset=["PATIENT_ID"]).copy()
    print(df['OS_STATUS'].unique())
    df['OS_STATUS'].replace("0:LIVING", "alive", inplace=True)
    df['OS_STATUS'].replace("1:DECEASED", "deceased", inplace=True)
    to_lower = ['SEX']
    df = format_entity(df, "Patient", "PATIENT_ID", to_lower = to_lower)
    return df

def create_sample_entity(samples):
    sample_cols = [
        "SAMPLE_ID",                 # chiave primaria Sample
        "ONCOTREE_CODE",
        "CANCER_TYPE",
        "CANCER_TYPE_DETAILED",
        "TUMOR_TYPE",
        "GRADE",
        "TISSUE_SOURCE_SITE_CODE",
        "TUMOR_TISSUE_SITE",
        "ANEUPLOIDY_SCORE",
        "SAMPLE_TYPE",
        "MSI_SCORE_MANTIS",
        "TMB_NONSYNONYMOUS",
        "TISSUE_SOURCE_SITE",
    ]
    df = samples[sample_cols].dropna(subset=["SAMPLE_ID"]).copy()
    to_lower = ['CANCER_TYPE', 'CANCER_TYPE_DETAILED', 'TUMOR_TYPE', 'TUMOR_TISSUE_SITE', 'TISSUE_SOURCE_SITE', 'SAMPLE_TYPE']
    df = format_entity(df, "Sample", "SAMPLE_ID", to_lower = to_lower)
    return df

def create_mutation_entity(mutations):
    mutation_cols = [
        "Hugo_Symbol",
        # "Tumor_Sample_Barcode",
        # identificazione genomica
        "Chromosome",
        "Start_Position",
        "End_Position",
        "Reference_Allele",
        "Tumor_Seq_Allele2",
        # tipo di mutazione
        "Variant_Classification",
        "Variant_Type",
        "Mutation_Status",
        # effetto molecolare
        "HGVSc",
        "HGVSp_Short",
        "Protein_position",
        "EXON",
        "Codons",
        "Amino_acids",
        # impatto funzionale
        "IMPACT",       
        "SIFT",         
        "PolyPhen",
        # annotazioni cliniche/oncologiche
        "Hotspot",
        "CLIN_SIG",
        # COSMIC -> indica se la mutazione è presente nel database COSMIC
        "COSMIC",
        # qualità delle reads, permettono di calcolare anche la VAF (variant allele frequency) VAF = t_alt_count / t_depth
        "t_alt_count",
        "t_ref_count",
        "t_depth"
    ]
    df = mutations[mutation_cols].dropna(subset=["Hugo_Symbol", "Chromosome", "Start_Position", "Reference_Allele", "Tumor_Seq_Allele2"]).copy()
    IDs = df["Hugo_Symbol"] + ":" + df["Chromosome"].astype(str) + ":" + df["Start_Position"].astype(str) + ":" + df["Reference_Allele"] + ">" + df["Tumor_Seq_Allele2"]
    df = format_entity(df, "Mutation", id_list=IDs, drop_cols=["Hugo_Symbol"])
    
    # sistemare le colonne PolyPhen e SIFT
    
    return df

def create_sv_entity(sv):
    df = sv.dropna(subset=["Sample_Id", "Site1_Chromosome", "Site1_Position", "Site2_Chromosome", "Site2_Position"]).copy()
    IDs = df["Sample_Id"] + "|" + df["Site1_Chromosome"].astype(str) + ":" + df["Site1_Position"].astype(str) + "|" + df["Site2_Chromosome"].astype(str) + ":" + df["Site2_Position"].astype(str)
    df['SV_Status'] = df['SV_Status'].str.lower()
    df = format_entity(df, "Structural Variant", id_list=IDs, drop_cols=["Sample_Id","Site1_Hugo_Symbol", "Site2_Hugo_Symbol"])
    return df

