from .neo4j_connection import *
import csv
import json
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import logging
import matplotlib.pyplot as plt
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator

from py2neo import Graph



def retrieve_non_empty_asins(neo4j_conn, database):
    # Set the database for the connection
    neo4j_conn.set_database(database)

    query = """
    MATCH (c:Category)
    WHERE size(c.asins) > 0
    RETURN c.name AS name, c.asins AS asins
    """
    results = neo4j_conn.query(query, db= database)
    name_asins_tuples = [(record['name'], record['asins']) for record in results]
    return name_asins_tuples


def calculate_average_salesrank(neo4j_conn, categories, database):
    category_salesrank_data = []

    # Set the database for the connection
    neo4j_conn.set_database(database)

    for category in categories:
        name = category[0]
        asins_list = category[1]
        salesranks = []

        for asin in asins_list:
            product_query = """
            MATCH (p:Product {ASIN: $asin})
            RETURN p.salesrank AS salesrank
            """
            salesrank = neo4j_conn.query(product_query, parameters={'asin': asin}, db=database)
            if salesrank and salesrank[0]['salesrank'] != -1:
                salesranks.append(salesrank[0]['salesrank'])

        if salesranks:
            average_salesrank = sum(salesranks) / len(salesranks)
            category_salesrank_data.append((name, average_salesrank))
        else:
            category_salesrank_data.append((name, None))

    category_salesrank_data.sort(key=lambda x: (x[1] is None, x[1]))
    return category_salesrank_data

def write_to_csv(category_salesrank_data, output_file):
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Category Name', 'Average Sales Rank'])

        for name, salesrank in category_salesrank_data:
            if salesrank is not None:
                writer.writerow([name, salesrank])
            else:
                writer.writerow([name, 'No Sales Rank Data'])

#-----------------------------------------------------------------------------------------------Ryans:



def create_db_driver(uri, username, password, database_name):
    return GraphDatabase.driver(uri, auth=(username, password), database=database_name)

# Function to execute a read query
def execute_read_query_tfidf(driver, query, parameters=None):
    with driver.session() as session:
        try:
            result = session.run(query, parameters)
            return [record["title"] for record in result]
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return []

# Function to create DataFrame from titles
def create_dataframe_tfidf(titles):
    return pd.DataFrame(titles, columns=['title'])

# Function for TF-IDF vectorization
def tfidf_vectorization(titles_df):
    tfidf_vectorizer = TfidfVectorizer(token_pattern=r'(?u)\b\w\w+\b', stop_words='english')
    tfidf_matrix = tfidf_vectorizer.fit_transform(titles_df['title'])
    tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=tfidf_vectorizer.get_feature_names_out())
    tfidf_df['title'] = titles_df['title']
    return tfidf_df

# Function to save DataFrame to CSV
def save_to_csv(tfidf_df, filename):
    tfidf_df.to_csv(filename, index=False)
    print(f"TF-IDF DataFrame saved to '{filename}'")


def initialize_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def get_neo4j_driver(uri, username, password, database_name):
    return GraphDatabase.driver(uri, auth=(username, password), database=database_name)

def execute_read_query(driver, query, parameters=None):
    with driver.session() as session:
        try:
            result = session.run(query, parameters)
            return [record['salesrank'] for record in result]
        except Exception as e:
            print(f"An error occurred: {e}")
            return []

def create_dataframe(spark, df):
    data = [row['salesrank'] for row in df.collect()]
    return spark.createDataFrame([(value,) for value in data], ["salesrank"])


def assemble_features(df, input_cols, output_col):
    assembler = VectorAssembler(inputCols=input_cols, outputCol=output_col)
    return assembler.transform(df)

def train_kmeans_model(df, k, seed):
    kmeans = KMeans().setK(k).setSeed(seed)
    model = kmeans.fit(df)
    return model

def evaluate_model(model, df):
    evaluator = ClusteringEvaluator()
    predictions = model.transform(df)
    silhouette = evaluator.evaluate(predictions)
    centers = model.clusterCenters()
    return silhouette, centers

def generate_bar_chart(tfidf_df, filename='bar_chart.png', figsize=(8, 6), dpi=100):
                word_scores = tfidf_df.drop(columns=['title']).sum().sort_values(ascending=False)
                top_words = word_scores.head(10)

                # Set the figure size and DPI
                plt.figure(figsize=figsize, dpi=dpi)

                plt.bar(top_words.index, top_words.values, color='skyblue')
                plt.xlabel('Words')
                plt.ylabel('TF-IDF Score')
                plt.title('Top 10 Most Important Words')
                plt.xticks(rotation=45)

                full_path = os.path.join('app/static', filename)
                print("Saving chart to:", full_path)  # This will show you the exact path being used
                plt.savefig(full_path)
                plt.close()

                return filename



#-----------------------------------------------------------------------------------------------Ayush


# Database connection
uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
graph = Graph(uri, auth=(username, password), name="products")

def execute_read_query(query, parameters=None):
    try:
        # Using py2neo's run method to execute a query
        return graph.run(query, parameters).data()
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


def find_top_selling_products_by_group():
    query = """
    MATCH (p:Product)
    WHERE p.group IS NOT NULL
    WITH p.group AS product_group, p
    ORDER BY p.salesrank
    RETURN product_group, collect(p.title)[..5] AS top_selling_titles
    """
    results = execute_read_query(query)
    return results


def find_top_selling_products_within_sales_rank_threshold(max_sales_rank):
    query = """
    MATCH (p:Product)
    WHERE p.group IS NOT NULL AND p.salesrank <= $max_sales_rank
    WITH p.group AS product_group, p
    ORDER BY p.salesrank
    RETURN product_group, collect(p.title)[..5] AS top_selling_titles
    """
    results = execute_read_query(
        query, {'max_sales_rank': max_sales_rank})
    return results


# Define your maximum sales rank threshold
max_sales_rank_threshold = 10000  # Adjust this value based on your data

# Execute the functions
top_selling_products = find_top_selling_products_by_group()
top_selling_products_threshold = find_top_selling_products_within_sales_rank_threshold(
    max_sales_rank_threshold)

# Output the results to JSON files
with open('top_selling_products_by_group.json', 'w') as outfile:
    json.dump({'top_selling_products_by_group': top_selling_products},
              outfile, indent=4)

with open('top_selling_products_within_threshold_by_group.json', 'w') as outfile:
    json.dump({'top_selling_products_within_threshold_by_group':
              top_selling_products_threshold}, outfile, indent=4)

print("Top selling products by group analysis has been saved to top_selling_products_by_group.json")
print("Top selling products within sales rank threshold by group analysis has been saved to top_selling_products_within_threshold_by_group.json")
