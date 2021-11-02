from pyspark.sql import SparkSession
from graphframes import *

from functools import reduce
from pyspark.sql.functions import *

# Example Graphframes
# from graphframes.examples import Graphs
# same_g = Graphs(spark).friends()
# print(same_g)

def test(spark: SparkSession):
    # **** POC

    # create dataframe

    v = spark.createDataFrame([
        ("A", "ARON", 350),
        ("B", "BILL", 360),
        ("C", "CLAIR", 195),
        ("D", "DANIEL", 90),
        ("E", "ERIC", 90),
        ("F", "FRANK", 215),
        ("G", "GRAHAM", 30),
        ("H", "HENRY", 25),
        ("I", "INNA", 25),
        ("J", "JEN", 20)
    ], ["id", "name", "call_count"])

    e = spark.createDataFrame([
        ("A", "B", 60),
        ("B", "A", 50),
        ("A", "C", 50),
        ("C", "A", 100),
        ("A", "D", 90),
        ("C", "I", 25),
        ("C", "J", 20),
        ("B", "F", 50),
        ("F", "B", 110),
        ("F", "G", 30),
        ("F", "H", 25),
        ("B", "E", 90)
    ], ["src", "dst", "relationship"])

    g = GraphFrame(v, e)

    # **** vertices and edges

    verticesDF = g.vertices
    edgesDF = g.edges

    verticesDF.show()
    edgesDF.show()

    # **** inDegrees, outDegrees and degrees

    inDegreeDF = g.inDegrees
    outDegreeDF = g.outDegrees
    degreeDF = g.degrees

    inDegreeDF.sort(['inDegree'], ascending=[0]).show()  # Sort and show
    outDegreeDF.sort(['outDegree'], ascending=[0]).show()
    degreeDF.show()

    # **** Graph's Structure

    PageRankResults = g.pageRank(resetProbability=0.15, tol=0.01)

    # Lets sort it to see who are the most influential users.
    PageRankResults.vertices.sort(['pagerank'], ascending=[0]).show()
    # Lets see how PageRank has normalized the weights based on number of edges.
    PageRankResults.edges.show()

    # **** Clustering And Community Detection

    # Label Propagation Algorithm (LPA)
    result = g.labelPropagation(maxIter=5)
    result.sort(['label'], ascending=[0]).show()

    # Connected Components
    result = g.connectedComponents()
    result.show()

    # Strongly Connected Components
    result = g.stronglyConnectedComponents(maxIter=18)
    result.show()


def databricks_demo(spark: SparkSession):
    # reference: https://docs.databricks.com/spark/latest/graph-analysis/graphframes/user-guide-python.html

    vertices = spark.createDataFrame([
        ("a", "Alice", 34),
        ("b", "Bob", 36),
        ("c", "Charlie", 30),
        ("d", "David", 29),
        ("e", "Esther", 32),
        ("f", "Fanny", 36),
        ("g", "Gabby", 60)], ["id", "name", "age"])

    edges = spark.createDataFrame([
        ("a", "b", "friend"),
        ("b", "c", "follow"),
        ("c", "b", "follow"),
        ("f", "c", "follow"),
        ("e", "f", "follow"),
        ("e", "d", "friend"),
        ("d", "a", "friend"),
        ("a", "e", "friend")
    ], ["src", "dst", "relationship"])

    g = GraphFrame(vertices, edges)
    # print(g)

    # *** Basic graph and DataFrame queries

    g.vertices.show()
    g.edges.show()
    g.inDegrees.show()
    g.outDegrees.show()
    g.degrees.show()

    youngest = g.vertices.groupBy().min("age")
    youngest.show()

    numFollows = g.edges.filter("relationship = 'follow'").count()
    # numFollows = g.edges.filter(col("relationship") == 'follow').count()
    print("The number of follow edges is", numFollows)

    # *** Motif finding

    # Search for pairs of vertices with edges in both directions between them.
    motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
    motifs.show()
    # +----------------+--------------+----------------+--------------+
    # | a | e | b | e2 |
    # +----------------+--------------+----------------+--------------+
    # | [c, Charlie, 30] | [c, b, follow] | [b, Bob, 36] | [b, c, follow] |
    # | [b, Bob, 36] | [b, c, follow] | [c, Charlie, 30] | [c, b, follow] |
    # +----------------+--------------+----------------+--------------+

    filtered = motifs.filter("b.age > 30 or a.age > 30")
    filtered.show()

    # *** Stateful queries

    # Find chains of 4 vertices.
    chain4 = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")

    # Query on sequence, with state (cnt)
    #  (a) Define method for updating state given the next element of the motif.
    def cumFriends(cnt, edge):
        relationship = col(edge)["relationship"]
        return when(relationship == "friend", cnt + 1).otherwise(cnt)

    #  (b) Use sequence operation to apply method to sequence of elements in motif.
    #   In this case, the elements are the 3 edges.
    edges = ["ab", "bc", "cd"]
    numFriends = reduce(cumFriends, edges, lit(0))

    chainWith2Friends2 = chain4.withColumn("num_friends", numFriends).where(numFriends >= 2)
    chainWith2Friends2.show()

    # +---------------+--------------+---------------+--------------+---------------+--------------+----------------+-----------+
    # | a | ab | b | bc | c | cd | d | num_friends |
    # +---------------+--------------+---------------+--------------+---------------+--------------+----------------+-----------+
    # | [d, David, 29] | [d, a, friend] | [a, Alice, 34] | [a, e, friend] | [e, Esther, 32] | [e, f, follow] | [f, Fanny,
    #                                                                                                           36] | 2 |
    # | [e, Esther, 32] | [e, d, friend] | [d, David, 29] | [d, a, friend] | [a, Alice, 34] | [a, e, friend] | [e, Esther,
    #                                                                                                           32] | 3 |
    # | [d, David, 29] | [d, a, friend] | [a, Alice, 34] | [a, e, friend] | [e, Esther, 32] | [e, d, friend] | [d, David,
    #                                                                                                           29] | 3 |
    # | [d, David, 29] | [d, a, friend] | [a, Alice, 34] | [a, b, friend] | [b, Bob, 36] | [b, c, follow] | [c, Charlie,
    #                                                                                                        30] | 2 |
    # | [e, Esther, 32] | [e, d, friend] | [d, David, 29] | [d, a, friend] | [a, Alice, 34] | [a, b, friend] | [b, Bob,
    #                                                                                                           36] | 3 |
    # | [a, Alice, 34] | [a, e, friend] | [e, Esther, 32] | [e, d, friend] | [d, David, 29] | [d, a, friend] | [a, Alice,
    #                                                                                                           34] | 3 |
    # +---------------+--------------+---------------+--------------+---------------+--------------+----------------+-----------+


    # *** Subgraphs

    g2 = g.filterEdges("relationship = 'friend'").filterVertices("age > 30").dropIsolatedVertices()
    g2.vertices.show()
    g2.edges.show()

    # *** Standard graph algorithms
    # ------------------------------


    # 1. Breadth-first search (BFS)
    paths = g.bfs("name = 'Esther'", "age < 32")
    paths.show()

    filteredPaths = g.bfs(
        fromExpr="name = 'Esther'",
        toExpr="age < 32",
        edgeFilter="relationship != 'friend'",
        maxPathLength=3)
    filteredPaths.show()

    # 2. Connected components
    result = g.connectedComponents()
    result.show()

    # 3. Strongly connected components # expensive computationally
    result = g.stronglyConnectedComponents(maxIter=10)
    result.select("id", "component").show()

    # 4. Label Propagation
    result = g.labelPropagation(maxIter=5)
    result.show()

    # 5. PageRank
    results = g.pageRank(resetProbability=0.15, tol=0.01)  # expensive computationally
    results.vertices.show()
    results.edges.show()

    # Run PageRank for a fixed number of iterations.
    results = g.pageRank(resetProbability=0.15, maxIter=10)
    results.vertices.show()
    results.edges.show()

    # Run PageRank personalized for vertex "a"
    results = g.pageRank(resetProbability=0.15, maxIter=10, sourceId="a")
    results.vertices.show()
    results.edges.show()

    # 6. Shortest paths
    results = g.shortestPaths(landmarks=["a", "d"])  # vertex a & d are landmarks for computation
    results.show()

    # 7. Triangle count
    results = g.triangleCount()
    results.show()

