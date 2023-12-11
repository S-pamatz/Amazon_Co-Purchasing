# app/routes.py
from flask import render_template, jsonify, request
from app.utils.spark_neo4j_connector import *
from app.utils.neo4j_connection import Neo4jConnection
from app.utils.CustomFunctions import *
from app.utils.parsing_functions import *
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
productsDb = "products"
categoryDb = "categories"














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
                "relationship.target.labels": "Product",
                "labels" : "Product",
                "labels": "Customer"
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
        last_cached_db = {}
        spark= create_spark_session()
        def fetch_data(spark, last_cached_db):
         
            db_selected = False
            query_inputted = False
            db = None
            results= None
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
                        spark, df = cache2_db(db)  # Assuming cache_db is a defined function
                        last_cached_db = db
                    else:
                        # If not, use the existing cached data
                        # Assuming get_cached_data is a defined function to retrieve cached data
                        spark, df = get_cached_data(spark, db)

                    # Execute the query (Assuming execute_query is a defined function)
                    
                    results = execute_query(spark, df, query, db)  # Implement this function
                    return results
        results = fetch_data(spark, last_cached_db)
        return render_template('data-query.html', results=results)

    




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






    @app.route('/TF-IDF', methods=['GET', 'POST'])
    def tfidf_vectorization_route():
        if request.method == 'POST':
            # Database connection details
            uri = "bolt://localhost:7687"
            username = "neo4j"
            password = "12345678"
            database_name = "products"

            driver = create_db_driver(uri, username, password, database_name)

            query = """
            MATCH (p:Product)
            WHERE p.group = 'Book'
            RETURN p.title AS title
            """

            result_titles = execute_read_query_tfidf(driver, query)
            titles_df = create_dataframe_tfidf(result_titles)
            tfidf_df = tfidf_vectorization(titles_df)
            
            chart_filename = generate_bar_chart(tfidf_df)
            # Render a template with the necessary data
            #chart_filename = 'bar_chart.png'
            return render_template('TF-IDF.html', chart_filename=chart_filename)

        return render_template('TF-IDF.html')



    @app.route('/Kmeans', methods=['GET', 'POST'])
    def kmeans_clustering_route():
        if request.method == 'POST':
            # Cache the database and get Spark session and DataFrame
            spark, df = cache_db_kmeans("products")

            # Ensure your query is aligned with the DataFrame schema
            # Example query (adjust according to your DataFrame schema)
            query = """
                SELECT salesrank
                FROM products
                WHERE group = 'Book'
            """
            result_sales_rank = execute_query(spark, df, query, "products")


            # Data processing
            df_processed = create_dataframe(spark, result_sales_rank)
            df_features = assemble_features(df_processed, ["salesrank"], "features")

            # Train KMeans model
            model = train_kmeans_model(df_features, 3, 1)

            # Evaluate model
            silhouette, centers = evaluate_model(model, df_features)

            # Stop Spark session
            spark.stop()

            # Pass the KMeans results to the template
            results = {
                'silhouette': silhouette,
                'centers': centers
            }

            return render_template('Kmeans.html', kmeans_results={'silhouette': silhouette, 'centers': centers})


        # If it's a GET request, render the initial form
        return render_template('Kmeans.html')
    






    @app.route('/market-analysis')
    def market_analysis():
        return render_template('market-analysis.html')
 
    @app.route('/perform-analysis', methods=['POST'])
    def perform_analysis():
       
        sales_rank_threshold = request.form.get('salesRankThreshold')
        if sales_rank_threshold:
            
            sales_rank_threshold = int(sales_rank_threshold)
        else:
            sales_rank_threshold = 10000 
 
        top_selling_data = find_top_selling_products_by_group()
        top_selling_within_threshold_data = find_top_selling_products_within_sales_rank_threshold(
            sales_rank_threshold)
 
        combined_data = {
            'top_selling': top_selling_data,
            'top_selling_within_threshold': top_selling_within_threshold_data
        }
 
        return render_template('market-analysis.html', data=combined_data)