import networkx as nx
from neo4j import GraphDatabase

def get_driver():
    URI = "bolt://localhost:7687"
    password = "12345678" # CHANGE
    AUTH = ("neo4j", password)
    return GraphDatabase.driver(URI, auth=AUTH)

def load_graph_by_edge(driver, edge_type, directed=False):
    G = nx.DiGraph() if directed else nx.Graph()

    query = f"""
        MATCH (n)-[r:{edge_type}]->(m)
        RETURN n.id AS source_id, properties(n) AS source_props,
               m.id AS target_id, properties(m) AS target_props,
               properties(r) AS edge_props
    """

    with driver.session() as session:
        result = session.run(query)
        
        for record in result:
            s_id = record["source_id"]
            t_id = record["target_id"]
            
            G.add_node(s_id, **record["source_props"])
            G.add_node(t_id, **record["target_props"])
            
            G.add_edge(s_id, t_id, **record["edge_props"])

    return G

def get_user_property_keys(driver, user_id):
    query = """
        MATCH (u:User {id: $user_id})
        RETURN keys(u) AS property_keys
    """
    
    with driver.session() as session:
        result = session.run(query, user_id=user_id)
        record = result.single()
        
        if record is None:
            return []
        
        return record["property_keys"]
    
def get_user_properties(driver, user_id):
    query = """
        MATCH (u:User {id: $user_id})
        UNWIND keys(u) AS key
        RETURN key, u[key] AS value
        ORDER BY key
    """
    
    with driver.session() as session:
        result = session.run(query, user_id=user_id)
        return {record["key"]: record["value"] for record in result}
    
def get_property_values(driver, property_key):
    query = f"""
        MATCH (u:User)
        WHERE u.{property_key} IS NOT NULL
        RETURN u.{property_key} AS value
    """

    with driver.session() as session:
        return [record["value"] for record in session.run(query)]
    
def get_property_values_for_users(driver, property_key, user_ids):
    query = f"""
        MATCH (u:User)
        WHERE u.id IN $user_ids
          AND u.{property_key} IS NOT NULL
        RETURN u.{property_key} AS value
    """

    with driver.session() as session:
        return [record["value"] for record in session.run(
            query, user_ids=list(user_ids)
        )]
