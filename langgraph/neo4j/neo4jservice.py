from neo4j import GraphDatabase
import os
import logging
logger = logging.getLogger(__name__)

class Neo4jService:
    def __init__(self):
        
        uri = os.getenv('NEO4J_URI')
        if uri is None:
            raise ValueError("Environment variable 'NEO4J_URI' is not set.")
        user = os.getenv('NEO4J_USERNAME')
        if user is None:
            raise ValueError("Environment variable 'NEO4J_USERNAME' is not set.")
        password = os.getenv('NEO4J_PASSWORD')
        if password is None:
            raise ValueError("Environment variable 'NEO4J_PASSWORD' is not set.")

        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
        except Exception as e:
            print("Failed to create a driver for the GraphDatabase: ", e)
            raise
    def close(self):
        self.driver.close()

    def create_node(self, node_id, properties):
        with self.driver.session() as session:
            session.write_transaction(self._create_node_tx, node_id, properties)

    @staticmethod
    def _create_node_tx(tx, node_id, properties):
        query = (
            "MERGE (n:Node {id: $node_id, properties: $properties}) "
        )
        logger.warning(
                f"""MATCH (n:Node id: {node_id}, properties: {properties})"""
        )
        return tx.run(query, node_id=node_id, properties=properties)

    def create_edge(self, start_node, end_node):
        with self.driver.session() as session:
            session.write_transaction(self._create_edge_tx, start_node, end_node)

    @staticmethod
    def _create_edge_tx(tx, start_node, end_node):
        query = (
            "MATCH (a:Node), (b:Node) "
            "WHERE a.id = $start_node AND b.id = $end_node "
            "MERGE (a)-[:CONNECTED_TO]->(b)"
        )
        return tx.run(query, start_node=start_node, end_node=end_node)


    def create_conditional_relationship(self, start_node, condition_name, properties):
        with self.driver.session() as session:
            session.write_transaction(self._create_conditional_relationship_tx, start_node, condition_name, properties)

    @staticmethod
    def _create_conditional_relationship_tx(tx, start_node, condition_name, properties):
        query = (
            "MATCH (start:Node {id: $start_node}) "
            "MERGE (condition:Condition {name: $condition_name, properties: $properties}) "
            "MERGE (start)-[:HAS_CONDITION]->(condition)"
        )
        return tx.run(query, start_node=start_node, condition_name=condition_name, properties=properties)