from flask import Flask, jsonify
from neo4j import GraphDatabase

app = Flask(__name__)

# Neo4j connection
uri = "bolt://localhost:7687"
username = "neo4j"
password = "Silvester59"
driver = GraphDatabase.driver(uri, auth=(username, password))

def get_db_session():
    return driver.session()

@app.route('/')
def index():
    with get_db_session() as session:
        result = session.run("MATCH (n) RETURN n LIMIT 5")
        nodes = [record["n"] for record in result]

        # Convert Neo4j Nodes to dictionaries
        serialized_nodes = [dict(node) for node in nodes]
        return jsonify(serialized_nodes)

if __name__ == '__main__':
    app.run(debug=True, port = 5001)
