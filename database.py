from neo4j import GraphDatabase
import itertools, string

class Database:
    def __init__(self, password, user, uri):
        self.password = password
        self.user = user
        self.uri = uri
        self.drive = GraphDatabase.driver(uri, auth=(user, password))
        assert self.drive is not None, "[ERRO] Não foi possível estabelecer conexão!"
    
    def createDatabase(self, definitionCode:str = None):
        assert definitionCode is not None, "Necessário definir o código para definir a criação das bases"
        with self.drive.session() as session:
            session.execute_write(lambda tx: tx.run("""
                MATCH(n)
                DETACH DELETE n
            """))
            session.execute_write(lambda tx: tx.run(definitionCode))
    
    def readDatabase(self, query):
        with self.drive.session() as session:
            try:
                return session.run(query).data()
            except Exception as e:
                print("[ERRO] Erro ao tentar rodar a query!")
                raise e