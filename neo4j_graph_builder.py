from html import entities
from tracemalloc import start
from neo4j_data import (
    create_gene_entity,
    create_gene_sv_relationship,
    create_protein_entity,
    create_patient_entity,
    create_sample_entity,
    create_mutation_entity,
    create_sv_entity,
    # Add more entity creation imports as needed
    create_relationship,
    create_gene_sv_relationship,

)
import pandas as pd

class Neo4jGraphBuilder:
    def __init__(self, all_data: dict):
        self.all_data = all_data
        self.entities = {}
        self.relationships = {}

    def build_entities(self):
        print("Building entities...")
        self.entities['Gene'] = create_gene_entity(self.all_data)
        self.entities['Protein'] = create_protein_entity(self.all_data['proteins'])
        self.entities['Patient'] = create_patient_entity(self.all_data['patients'])
        self.entities['Sample'] = create_sample_entity(self.all_data['samples'])
        self.entities['Mutation'] = create_mutation_entity(self.all_data['mutations'])
        self.entities['Structural_Variant'] = create_sv_entity(self.all_data['sv'])
        # Add more entity creation calls as needed

        print(f"Entities built: {list(self.entities.keys())}")
        return self.entities

    def build_relationships(self):
        print("Building relationships...")
        # Implement relationship creation logic here
        self.relationships['SAMPLE_FROM_PATIENT'] = create_relationship(
            start_entity = self.entities['Sample'][':ID'],
            end_entity = self.all_data['samples']['PATIENT_ID'],
            rel_type = 'FROM_PATIENT'
        )

        self.relationships['GENE_ENCODES_PROTEIN'] = create_relationship(
            start_entity = self.entities['Protein'][':ID'].str.split("|").str[0],
            end_entity = self.entities['Protein'][':ID'],
            rel_type = 'ENCODES'
        )

        self.relationships['GENE_HAS_ALTERATION_MUTATION'] = create_relationship(
            start_entity = self.all_data['mutations']['Hugo_Symbol'],
            end_entity = self.entities['Mutation'][':ID'],
            rel_type = 'HAS_ALTERATION'
        )
        
        self.relationships['MUTATION_OBSERVED_IN_SAMPLE'] = create_relationship(
            start_entity= self.entities['Mutation'][':ID'],
            end_entity= self.all_data['mutations']['Tumor_Sample_Barcode'],
            rel_type='OBSERVED_IN'
        )

        self.relationships['GENE_HAS_ALTERATION_SV'] = create_gene_sv_relationship(self.all_data['sv'], self.entities['Structural_Variant'])
        
        self.relationships['SV_OBRSERVED_IN_SAMPLE'] = create_relationship(
            start_entity= self.entities['Structural_Variant'][':ID'],
            end_entity= self.all_data['sv']['Sample_Id'],
            rel_type='OBSERVED_IN'
        )

        print(f"Relationships built: {list(self.relationships.keys())}")

        return self.relationships   

    def save_entities(self):
        pass

    def save_relationships(self):
        pass
