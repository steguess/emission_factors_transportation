if __name__ == '__main__':

    from pyspark.sql import SparkSession
    from pyspark.context import SparkContext
    print ("Scope 3 Emission Analysis")
    print ("--")
    print ("Analysis initialization:")
    print ("  1. Building the Spark Session...")

    # 1. Building the Spark Session
    sc = SparkSession.builder\
        .master('local')\
        .appName('Scope3')\
        .getOrCreate()
    spark = SparkSession(sc)

    # 2. Dataframes creation
    print ("  2. Creating the vertices (Sites) and edges (trips) DataFrames from HDFS...")
    stations = (spark.read
            .option("header","true")
            .option("inferSchema","true")
            .csv("hdfs://localhost:9000/datalake/raw/scope3",sep=";")
            .limit(100)
            .distinct())
    print ("  2a. Creating the vertices")
    origin_keys=stations.select("Origin_Key").distinct()
    target_keys=stations.select("Target_Key").distinct()
    nodes=origin_keys.union(target_keys).distinct()
    print ("  2b. Creating the edges")

    # 3. Time to build our graph with the GraphFrames library
    from pyspark.sql.functions import count,avg,desc,asc,col
    from graphframes import GraphFrame

    print ("  3. Building the graph with GraphFrames...")
    # GraphFrames requires the vertices DataFrame to have a column named id.
    vertices = nodes.withColumnRenamed("Origin_Key","id")

    # GraphFrames requires the edges DataFrame to have columns named src and dst
    trips = (stations.withColumnRenamed("Origin_Key", "src")
                  .withColumnRenamed("Target_Key", "dst"))
    #               .withColumnRenamed("Origin_Key", "src_name")
    #               .withColumnRenamed("Target_Key", "dst_name")              
    edges = (trips.groupBy("src", "dst")
                   .agg(count("*").alias("trip_count"),
                        avg("Distance").alias("Distance")))

    # Build the graph
    graph = GraphFrame(vertices, edges)

    # graph processing usually requires multiple iterations, 
    # therefore caching it will bring better performance
    graph.cache()   


    # 4. Let's use the PageRank algorithm to identify the top 5 most popular bikestations.
    print ("  4. Running the PageRank algorithm to get the top 5 most popular bike stations (be patient)...")
    from pyspark.sql.functions import desc
    ranks = graph.pageRank(resetProbability=0.15, maxIter=10)
    ranks.vertices.orderBy(desc("pagerank")).select("id", "pagerank").show(5)

    # 5. Finalize the execution (release resources if needed)
    print ("The job is done! Have a great day my human friend :)")