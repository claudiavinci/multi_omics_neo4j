from neo4j import GraphDatabase
from pathlib import Path
import docker

class neo4jConnection:
    def __init__(self, uri, user, password, import_path, import_cont_path):
        self.__uri = uri
        self.__user = user
        self.__password = password
        self.driver = None
        self.import_path = import_path
        self.import_cont_path = import_cont_path
        try:
            self.driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__password))
            print("Driver creato con successo.")
        except Exception as e:
            print("Impossibile creare il driver: ", e)
    
    def close_connection(self):
        if self.driver is not None:
            self.driver.close()
            print("Connessione chiusa con successo.")
        else: 
            print("Nessuna connessione corrente.")

    def execute_query(self, query, db = None):
        session = None
        response = None
        try:
            if db is not None:
                session = self.driver.session(database=db)
            else:
                session = self.driver.session()
            response = list(session.run(query))
        except Exception as e: 
            print("Impossibile eseguire la query: ", e)
        finally:
            if session is not None:
                session.close()
        return response

    def import_database(self):
        if self.driver is not None:
            self.close_connection()
        
        entities = list(Path(f'{self.import_path}/entities/').glob("*.csv"))
        relationships = list(Path(f'{self.import_path}/relationships/').glob("*.csv"))
        entities_import = ' '.join([f'--nodes {self.import_cont_path}/entities/{entity.name}' for entity in entities])
        rel_import = ' '.join([f'--relationships {self.import_cont_path}/relationships/{rel.name}' for rel in relationships])
        full_import_string = f'neo4j-admin database import full --delimiter="," --array-delimiter="|" --overwrite-destination {entities_import} {rel_import} --verbose'

        try:
            client = docker.from_env()
            neo4jContainer = client.containers.get('neo4j')
            for cmd, msg in [
                ('neo4j stop', "Stopping neo4j database before importing data..."),
                (full_import_string, "Importing graph (nodes and relationships) into neo4j database..."),
            ]:
                print(msg)
                code, out = neo4jContainer.exec_run(cmd)
                if code:
                    raise RuntimeError(out.decode())

        except Exception as e:
            print(f"Import failed: {e}")
        finally:
            try:
                print("Starting neo4j...")
                neo4jContainer.exec_run('neo4j start')
                print("Neo4j database started.")
            except Exception:
                pass