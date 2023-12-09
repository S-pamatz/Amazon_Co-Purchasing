from .neo4j_connection import *
import csv

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

