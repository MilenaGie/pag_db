from neo4j import GraphDatabase


def main():
    user, password = "", ""
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=(user,
                                                                 password))
    session = driver.session()
