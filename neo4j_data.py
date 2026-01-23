# from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd 

def format_entity(df, label, id_col, id_list=None, drop_cols: list = None):
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True)
    df.columns = [col.lower() for col in df.columns]
    if id_col == ":ID":
        df.insert(0, id_col, id_list)
    else:
        df = df.rename(columns={id_col.lower(): ":ID"})
    df.insert(1, ":LABEL", label)
    print(f"Created {label} entity with {len(df)} records and {len(df.columns)} columns")
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
    df = patients[patient_cols].dropna(subset=["PATIENT_ID"]).copy()
    df = format_entity(df, "Patient", "PATIENT_ID")
    return df

def create_sample_entity(samples):
    sample_cols = [
        "SAMPLE_ID",                 # chiave primaria Sample
        "ONCOTREE_CODE",
        "CANCER_TYPE",
        "CANCER_TYPE_DETAILED",
        "TUMOR_TYPE",
        "GRADE",
        "SAMPLE_TYPE",
        "TUMOR_TISSUE_SITE",
        "ANEUPLOIDY_SCORE",
        "TMB_NONSYNONYMOUS",
        "MSI_SCORE_MANTIS",
        "TISSUE_SOURCE_SITE",
        "TISSUE_SOURCE_SITE_CODE"
    ]
    df = samples[sample_cols].dropna(subset=["SAMPLE_ID"]).copy()
    df = format_entity(df, "Sample", "SAMPLE_ID")
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
    df = format_entity(df, "Mutation", ":ID", id_list=IDs, drop_cols=["Hugo_Symbol"])
    return df

def create_sv_entity(sv):
    df = sv.dropna(subset=["Sample_Id", "Site1_Chromosome", "Site1_Position", "Site2_Chromosome", "Site2_Position"]).copy()
    IDs = df["Sample_Id"] + "|" + df["Site1_Chromosome"].astype(str) + ":" + df["Site1_Position"].astype(str) + "|" + df["Site2_Chromosome"].astype(str) + ":" + df["Site2_Position"].astype(str)
    df = format_entity(df, "Structural_Variant", ":ID", id_list=IDs, drop_cols=["Sample_Id","Site1_Hugo_Symbol", "Site2_Hugo_Symbol"])
    return df

def create_simple_relationship(start_entity, end_entity, rel_type: str, properties: dict = None): 
    # prendere gli id, aggiungere la colonna che serve per la relazione e creare il dataframe delle relazion 
    rel = pd.DataFrame({':START_ID': start_entity, ':END_ID': end_entity, ':TYPE': rel_type})
    if properties:
        for key, value in properties.items():
            rel[key.lower()] = value
    return rel

def create_gene_sv_relationship(sv_data, sv_entity):
    gene_sv_df = pd.concat([
        sv_data[['Site1_Hugo_Symbol']].rename(columns={'Site1_Hugo_Symbol': 'gene'}).assign(
            sv_id=sv_entity[':ID'], site="1"),
        sv_data[['Site2_Hugo_Symbol']].rename(columns={'Site2_Hugo_Symbol': 'gene'}).assign(
            sv_id=sv_entity[':ID'], site="2")
    ], ignore_index=True)

    return create_simple_relationship(
        start_entity = gene_sv_df['gene'],
        end_entity = gene_sv_df['sv_id'],
        rel_type = 'HAS_ALTERATION',
        properties = {'site': gene_sv_df['site']}
    )

def wide_to_long_df(wide_df, id_col, var_name, value_name):
    long_df = wide_df.melt(
        id_vars=[id_col],
        value_vars=[col for col in wide_df.columns if col != id_col],
        var_name=var_name,
        value_name=value_name
    )
    long_df = long_df.dropna(subset=[value_name])
    return long_df

def create_wide_to_long_relationship(wide_data, start_col, end_col, rel_type, value_name, drop_cols = None):
    df = wide_data.copy()
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True)
    df_long = wide_to_long_df(df, id_col=start_col, var_name=end_col, value_name=value_name)
    start = df_long[start_col]
    end = df_long[end_col]
    properties = {value_name: df_long[value_name]} 

    return create_simple_relationship(start, end, rel_type, properties)