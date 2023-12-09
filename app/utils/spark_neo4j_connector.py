from pyspark.sql import SparkSession

def create_spark_session():
    jar_path = "./jars/neo4j-connector-apache-spark_2.12-5.2.0_for_spark_3.jar"
    spark = SparkSession.builder \
        .appName("Neo4j and Spark Example") \
        .config("spark.jars", jar_path) \
        .getOrCreate()
    return spark


def read_data_from_neo4j(spark, neo4j_options, db=None):
    # Read data from Neo4j into a Spark DataFrame using the provided options
    if db:
        neo4j_options["database"] = db
    return spark.read.format("org.neo4j.spark.DataSource") \
        .options(**neo4j_options) \
        .load()

def get_cached_data(db):
    """
    Retrieve a cached DataFrame for the specified database.
    :param db: The database name.
    :return: Cached Spark DataFrame.
    """
    return dataframe_cache.get(db)

def execute_query(spark, df, query, db):
    """
    Execute a SQL query on a Spark DataFrame.
    :param spark: Spark session.
    :param df: Spark DataFrame to query.
    :param query: SQL query string.
    :return: Result of the query execution.
    """
    # Ensure the DataFrame is registered as a temp view
    df.createOrReplaceTempView(db)

    # Execute the query
    result = spark.sql(query)

    return result

def cache_db(db=None):           
    # Initialize Spark session
    spark = create_spark_session()
    
    # Define Neo4j options (modify these options based on your Neo4j setup)
    neo4j_options = {
        "url": "bolt://localhost:7687",
        "authentication.type": "basic",
        "authentication.basic.username": "neo4j",
        "authentication.basic.password": "12345678",
        "relationship": "REVIEWED",
        "relationship.nodes.map": "true",
        "relationship.source.labels": "Customer",
        "relationship.target.labels": "Product"
    }

    # Read data from Neo4j
    df = read_data_from_neo4j(spark, neo4j_options, db)

    # Perform some simple operations for testing
    df.cache()
    df.createOrReplaceTempView(db);
    return spark, df
    