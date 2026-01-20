import networkx as nx
from networkx.algorithms import community
from neo4j import GraphDatabase

def get_driver():
    URI = "bolt://localhost:7687"
    password = "12345678" # CHANGE
    AUTH = ("neo4j", password)
    return GraphDatabase.driver(URI, auth=AUTH)

def get_graph_from_neo4j(driver):
    G = nx.DiGraph()
    with driver.session() as session:
        nodes_result = session.run("""
            MATCH (n)
            RETURN id(n) AS id, labels(n) AS labels, properties(n) AS properties
        """)
        for record in nodes_result:    
            G.add_node(record["id"], labels=record["labels"], **record["properties"])

        rels_result = session.run("""
            MATCH (n)-[r]->(m)
            RETURN id(n) AS source, id(m) AS target, type(r) AS type
        """)
        for record in rels_result:
            G.add_edge(record["source"], record["target"], type=record["type"])
    return G

def load_msg_graph(driver):
    return get_graph_from_neo4j(driver)

def load_user_graphs(driver):
    G_shares = nx.Graph()
    G_viral = nx.Graph()
    G_misinfo = nx.Graph()
    G_rapid_shares = nx.Graph()

    networkx_graph = load_msg_graph(driver)

    user_nodes = {n for n, d in networkx_graph.nodes(data=True) if 'User' in d.get('labels', [])}

    for u_node in user_nodes:
        G_shares.add_node(u_node, **networkx_graph.nodes[u_node])
        G_viral.add_node(u_node, **networkx_graph.nodes[u_node])
        G_misinfo.add_node(u_node, **networkx_graph.nodes[u_node])
        G_rapid_shares.add_node(u_node, **networkx_graph.nodes[u_node])

    for u, v, data in networkx_graph.edges(data=True):
        if u in user_nodes and v in user_nodes:
            edge_type = data.get('type')
            if edge_type == 'SHARES':
                G_shares.add_edge(u, v, **data)
            elif edge_type == 'VIRAL_SHARES':
                G_viral.add_edge(u, v, **data)
            elif edge_type == 'SHARES_MISINFORMATION':
                G_misinfo.add_edge(u, v, **data)
            elif edge_type == "RAPID_SHARE":
                G_rapid_shares.add_edge(u, v, **data)

    G_shares.remove_nodes_from(list(nx.isolates(G_shares)))
    G_viral.remove_nodes_from(list(nx.isolates(G_viral)))
    G_misinfo.remove_nodes_from(list(nx.isolates(G_misinfo)))
    G_rapid_shares.remove_nodes_from(list(nx.isolates(G_rapid_shares)))

    return (G_shares, G_viral, G_misinfo, G_rapid_shares)
