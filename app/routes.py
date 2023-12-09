# app/routes.py
from flask import render_template, jsonify, request
from app.utils.spark_neo4j_connector import create_spark_session, read_data_from_neo4j, cache_db, get_cached_data, execute_query
from app.utils.neo4j_connection import Neo4jConnection
from app.utils.CustomFunctions import *
from app.utils.parsing_functions import *
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
        url = "bolt://localhost:7687"
        username = "neo4j"
        password = "12345678"
        neo4j_conn = Neo4jConnection(url, username, password)

        # Retrieve data from the 'categories' database
        listOftuples = retrieve_non_empty_asins(neo4j_conn, 'categories')

        # Calculate data from the 'products' database
        calculated_data = calculate_average_salesrank(neo4j_conn, listOftuples, 'products')
        write_to_csv(calculated_data, "salesRanks.csv")
        neo4j_conn.close()
        best_selling_categories, best_selling_authors = parseBestSellingCategories("salesRanks.csv")
        return render_template(
            'trends.html',
            best_selling_categories=best_selling_categories,
            best_selling_authors=best_selling_authors)










    