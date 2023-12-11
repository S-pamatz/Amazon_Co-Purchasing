Prerequisites:

Python 3.xxx
Flask
Neo4j Database
Git

Installation:


Step 1: Set Up a Virtual Environment (Optional)

`python3 -m venv venv`

`source venv/bin/activate`  # On Windows use `venv\Scripts\activate`

Step 2: Install Dependencies

pip install -r requirements.txt

`FLASK_APP=app.py`

use `flask run` to run app

Step 3: Database Setup

Start Neo4j Database 

Create 2 new dbs using dumps, make sure the default neo4j db is empty

Update uri, username, and password in app.py.

Step 4: Run the Application

flask run` to run app


