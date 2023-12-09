# app/routes.py
from flask import render_template, jsonify, request
from app.utils.spark_neo4j_connector import create_spark_session, read_data_from_neo4j

productsDb = "products"
categoryDb = "categories"

last_cached_db = {}

def init_routes(app):

    @app.route('/')
    def home():
        return render_template("home.html")
    
    @app.route('/test')
    def test_spark_neo4j():
        try:
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
            df = read_data_from_neo4j(spark, neo4j_options, productsDb)

            # Perform some simple operations for testing
            df_count = df.count()
            
            # Stop the Spark session
            spark.stop()

            return jsonify({"message": "Success", "data_count": df_count})
        except Exception as e:
            spark.stop()
            return jsonify({"error": str(e)}), 500


    @app.route('/data-querying', methods=['GET', 'POST'])
    def data_querying():
        global last_cached_db  # Reference the global variable

        db_selected = False
        query_inputted = False
        db = None

        if request.method == 'POST':
            # Get the text input from the form
            query = request.form.get('query')
            query_inputted = bool(query)

            # Check which button was clicked
            if 'button' in request.form:
                db = request.form['button']
                db_selected = True

                # Check if the database has changed
                if db != last_cached_db:
                    
                    # If yes, recache the database and update the global variable
                    spark, df = cache_db(db)  # Assuming cache_db is a defined function
                    last_cached_db = db
                else:
                    # If not, use the existing cached data
                    # Assuming get_cached_data is a defined function to retrieve cached data
                    spark, df = get_cached_data(spark, db)

                # Execute the query (Assuming execute_query is a defined function)
                # You need to handle the execution of the query and the results
                results = execute_query(spark, df, query, db)  # Implement this function
                results.show()
        return render_template('data-query.html')






    @app.route('/trends')
    def CreateTrends():
        
        return render_template('trends.html')









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
    
    