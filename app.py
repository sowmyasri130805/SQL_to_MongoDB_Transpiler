from flask import Flask, request, jsonify, render_template
from sql_to_mongo_transpiler.codegen.mongodb_generator import MongoDBGenerator
from sql_to_mongo_transpiler.parser.sql_parser import get_parser
from sql_to_mongo_transpiler.semantic.semantic_analyzer import SemanticAnalyzer
from pymongo import MongoClient
import psycopg2
import json

app = Flask(__name__)

# ---------------- DB CONNECTIONS ---------------- #

mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["transpiler_db"]


def run_sql(query):
    conn = psycopg2.connect(
        dbname="transpiler_db",
        user="postgres",
        password="password",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    # Return column names alongside rows
    columns = [desc[0] for desc in cur.description] if cur.description else []
    conn.close()
    return rows, columns


def run_mongo(collection, query, projection=None):
    return list(mongo_db[collection].find(query, projection))


# ---------------- ROUTES ---------------- #

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/schema")
def get_schema():
    """
    Serve schema from schema.json.
    Falls back to introspecting PostgreSQL if schema.json not found.
    """
    try:
        with open("schema.json") as f:
            return jsonify(json.load(f))
    except FileNotFoundError:
        # Auto-introspect from PostgreSQL
        try:
            conn = psycopg2.connect(
                dbname="transpiler_db",
                user="postgres",
                password="password",
                host="localhost",
                port="5432"
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT table_name, column_name, data_type,
                       CASE WHEN column_name IN (
                           SELECT kcu.column_name
                           FROM information_schema.table_constraints tc
                           JOIN information_schema.key_column_usage kcu
                             ON tc.constraint_name = kcu.constraint_name
                           WHERE tc.constraint_type = 'PRIMARY KEY'
                             AND tc.table_name = c.table_name
                       ) THEN true ELSE false END AS is_pk
                FROM information_schema.columns c
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position;
            """)
            rows = cur.fetchall()
            conn.close()

            tables = {}
            for table_name, col_name, data_type, is_pk in rows:
                if table_name not in tables:
                    tables[table_name] = []
                tables[table_name].append({
                    "name": col_name,
                    "type": data_type,
                    "primary_key": is_pk
                })

            schema = {
                "tables": [
                    {"name": t, "columns": cols}
                    for t, cols in tables.items()
                ]
            }
            return jsonify(schema)

        except Exception as e:
            return jsonify({"error": f"Could not load schema: {str(e)}"}), 500


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

        # SQL execution — now returns (rows, columns)
        sql_rows, sql_columns = run_sql(sql)

        # Mongo execution
        if "filter" in mongo_data:
            mongo_result = run_mongo(
                collection,
                mongo_data["filter"],
                mongo_data.get("projection")
            )
        else:
            mongo_result = list(
                mongo_db[collection].aggregate(mongo_data["pipeline"])
            )

        # Remove _id from mongo results
        for doc in mongo_result:
            doc.pop("_id", None)

        # Convert SQL rows to JSON-serializable format
        sql_result_serializable = [list(row) for row in sql_rows]

        return jsonify({
            "mongo": mongo_data["string"],
            "columns": sql_columns,           # ← new: column names for table headers
            "sql_result": sql_result_serializable,
            "mongo_result": mongo_result
        })

    except Exception as e:
        return jsonify({"error": str(e)})


if __name__ == "__main__":
    app.run(debug=True)
