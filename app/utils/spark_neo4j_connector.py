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

