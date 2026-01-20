from neo4j import GraphDatabase

class neo4jConnection:
    def __init__(self, uri, user, password ):
        self.__uri = uri
        self.__user = user
        self.__password = password
        self.driver = None
        try:
            self.driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__password))
            print("Driver creato con successo.")
        except Exception as e:
            print("Impossibile creare il driver: ", e)
    
    def closeConnection(self):
        if self.driver is not None:
            self.driver.close()
            print("Connessione chiusa con successo.")
        else: 
            print("Nessuna connessione corrente.")

    def executeQuery(self, query, db = None):
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
        