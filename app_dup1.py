
from flask import Flask, request, jsonify, render_template
from sql_to_mongo_transpiler.codegen.mongodb_generator import MongoDBGenerator
from sql_to_mongo_transpiler.parser.sql_parser import get_parser
from sql_to_mongo_transpiler.semantic.semantic_analyzer import SemanticAnalyzer
from pymongo import MongoClient
import psycopg2
import json

app = Flask(__name__)

# DB connections
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["testdb"]

def run_sql(query):
    conn = psycopg2.connect(
        dbname="testdb",
        user="postgres",
        password="password",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    conn.close()
    return result

def run_mongo(collection, query, projection=None):
    return list(mongo_db[collection].find(query, projection))

@app.route("/")
def index():
    return render_template("index_dup1.html")

# 🔥 Run SQL → Mongo pipeline
@app.route("/run", methods=["POST"])
def run_query():
    data = request.json

    sql = data["sql"]
    schema = data["schema"]

    try:
        parser = get_parser()
        analyzer = SemanticAnalyzer(schema)
        generator = MongoDBGenerator()

        ast = parser.parse(sql)
        analyzer.validate_query(ast)

        mongo_data = generator.generate(ast)

        collection = mongo_data["collection"]

        if "filter" in mongo_data:
            mongo_result = run_mongo(
                collection,
                mongo_data["filter"],
                mongo_data["projection"]
            )
        else:
            mongo_result = list(
                mongo_db[collection].aggregate(mongo_data["pipeline"])
            )

        # remove _id
        for doc in mongo_result:
            doc.pop("_id", None)

        return jsonify({
            "mongo": mongo_data["string"],
            "result": mongo_result
        })

    except Exception as e:
        return jsonify({"error": str(e)})

# 🔥 Direct Mongo commands (insert/delete)
@app.route("/mongo", methods=["POST"])
def run_mongo_raw():
    data = request.json
    query = data["query"]

    try:
        if "deleteMany" in query:
            mongo_db.users.delete_many({})
        elif "insert_many" in query:
            docs = eval(query.split("insert_many(")[1][:-1])
            mongo_db.users.insert_many(docs)

        return jsonify({"status": "ok"})

    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    app.run(debug=True)
