import docker
from neo4j_connection import neo4jConnection

if __name__ == '__main__':
    
    client = docker.from_env()
    neo4jContainer = client.containers.get('neo4j')
    neo4j = neo4jConnection(uri="bolt:localhost:7474", user="neo4j", password="admin1234")
