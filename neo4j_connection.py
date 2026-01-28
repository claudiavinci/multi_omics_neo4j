from neo4j import GraphDatabase
from pathlib import Path

class neo4jConnection:
    def __init__(self, uri, user, password, import_path):
        self.__uri = uri
        self.__user = user
        self.__password = password
        self.driver = None
        self.import_path = import_path
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
        