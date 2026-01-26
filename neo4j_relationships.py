import pandas as pd 
from utilities import format_string, wide_to_long_df

def create_simple_relationship(start_entity, end_entity, rel_type: str, properties: dict = None): 
    # prendere gli id, aggiungere la colonna che serve per la relazione e creare il dataframe delle relazion 
    rel = pd.DataFrame({':START_ID': start_entity, ':END_ID': end_entity, ':TYPE': rel_type})
    if properties:
        for key, value in properties.items():
            rel[format_string(key, "camel")] = value
    return rel

def create_gene_sv_relationship(sv_data, sv_entity):
    gene_sv_df = pd.concat([
        sv_data[['Site1_Hugo_Symbol']].rename(columns={'Site1_Hugo_Symbol': 'gene'}).assign(
            sv_id=sv_entity['structuralVariantId:ID'], site="1"),
        sv_data[['Site2_Hugo_Symbol']].rename(columns={'Site2_Hugo_Symbol': 'gene'}).assign(
            sv_id=sv_entity['structuralVariantId:ID'], site="2")
    ], ignore_index=True)

    return create_simple_relationship(
        start_entity = gene_sv_df['gene'],
        end_entity = gene_sv_df['sv_id'],
        rel_type = 'HAS_ALTERATION',
        properties = {'site': gene_sv_df['site']}
    )

def create_wide_to_long_relationship(wide_data, start_col, end_col, rel_type, value_name, drop_cols = None):
    df = wide_data.copy()
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True)
    df_long = wide_to_long_df(df, id_col=start_col, var_name=end_col, value_name=value_name)
    start = df_long[start_col]
    end = df_long[end_col]
    properties = {value_name: df_long[value_name]} 

    return create_simple_relationship(start, end, rel_type, properties)