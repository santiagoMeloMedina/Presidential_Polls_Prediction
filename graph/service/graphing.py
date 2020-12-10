
import constant.graph as GRAPH
from model.node import Node
from configuration.connection import neo4j
from pyspark.sql import Row
from configuration.connection import spark

def runCreateQuery(elements):
    """ This function creates and graph into neo4j from generating query string """
    with neo4j.session() as session:
        session.run(GRAPH.CREATE.format(','.join(elements)))
    return

def makeCreateNodesQuery(nodes):
    """ This function delivers the nodes to be queried for creation"""
    queryNodes = []
    counter = 0
    for node in nodes:
        queryNodes.append(GRAPH.NODE.format(var=node.getVar(), state=node.getState(), date=str(node.getDate())))
        print("{}/{}".format(counter, len(nodes)), end="\r")
        counter += 1
    return queryNodes

def makeCreateEdgesQuery(nodes):
    """ This function delivers the edges to be queried for creation"""
    queryEdges = []
    counter = 0
    for i in range(len(nodes)):
        for j in range(len(nodes)):
            if i != j:
                u, v = nodes[i], nodes[j]
                if u.getDate() < v.getDate():
                    queryEdges.append(GRAPH.EDGE.format(u=u.getVar(), v=v.getVar(), winner=v.getWinner()))
            print("{}/{}".format(counter, len(nodes)*len(nodes)), end="\r")
            counter += 1
    return queryEdges

def makeNodes(df):
    """ This function creates the nodes from rows on dataset"""
    nodes = []
    rows = df.select(["state", "modeldate", "winstate_inc", "winstate_chal"])
    counter = 0
    for row in rows.collect():
        row = row.asDict()
        row['var'] = "poll{}".format(counter)
        nodes.append(Node(**row))
        counter += 1
    return nodes[:300]

def buildGraph(df):
    """ This function combines queries to create nodes and edges into neo4j database"""
    nodes = makeNodes(df)
    nodes_q = makeCreateNodesQuery(nodes)
    print("---------------------------")
    edges_q = makeCreateEdgesQuery(nodes)
    runCreateQuery(nodes_q+edges_q)

def callMetrics():
    """ This function takes the metrics from neo4j generated graph"""
    result = [[], [], []]
    with neo4j.session() as session:
        session.run(GRAPH.GDS_CREATE)
        session.run(GRAPH.LOUVAIN_SET)
        louvain = session.run(GRAPH.LOUVAIN_CHECK)
        pagerank = session.run(GRAPH.PAGERANK)
        session.run(GRAPH.DEGREE_SET)
        degree = session.run(GRAPH.DEGREE_CHECK)
    for rank in pagerank:
        result[0].append(rank[0])
    for lou in louvain:
        result[1].append(lou[1])
    for deg in degree:
        result[2].append(deg[0])
    return result

def addMetricsToFrame(df):
    metrics = callMetrics()
    rows = df.limit(len(metrics[0]))
    page_rank = spark.sparkContext.parallelize(metrics[0])
    new_row = Row("pagerank")
    page_rank_df = page_rank.map(new_row).toDF()
    rows = rows.join(page_rank_df)
    return rows
        

