import sys
import os
import json
from sql_to_mongo_transpiler.lexer.sql_lexer import get_lexer, LexerError
from sql_to_mongo_transpiler.parser.sql_parser import get_parser
from sql_to_mongo_transpiler.semantic.semantic_analyzer import SemanticAnalyzer, SemanticError
from sql_to_mongo_transpiler.codegen.optimizer import MongoOptimizer
from sql_to_mongo_transpiler.schema_loader import load_schema, SchemaError
import psycopg2
from pymongo import MongoClient
import re
import ast

# ---------------- DATABASE CONNECTION ---------------- #

def run_sql(query):
    conn = psycopg2.connect(
        dbname="transpiler_db",
        user="postgres",
        password="password",  # change if needed
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    conn.close()
    return result


client = MongoClient("mongodb://localhost:27017/")
db = client["transpiler_db"]

def run_mongo(collection, query, projection=None):
    return list(db[collection].find(query, projection))


# ---------------- MONGO STRING PARSER ---------------- #

def parse_mongo_string(mongo_str):
    try:
        # collection
        collection = re.search(r"db\.(\w+)\.find", mongo_str).group(1)

        # inside find(...)
        args = re.search(r"find\((.*)\)", mongo_str).group(1)

        # split filter + projection
        parts = args.split("},")
        filter_part = parts[0] + "}"

        projection_part = None
        if len(parts) > 1:
            projection_part = parts[1].strip()
            if not projection_part.endswith("}"):
                projection_part += "}"

        # convert JS → Python dict
        filter_dict = ast.literal_eval(filter_part.replace("$", "\"$\""))

        projection_dict = None
        if projection_part:
            projection_dict = ast.literal_eval(projection_part.replace("$", "\"$\""))

        return collection, filter_dict, projection_dict

    except Exception as e:
        print("Mongo Parse Error:", e)
        return None, {}, None


# ---------------- COMPARATOR ---------------- #

def normalize_sql(result):
    return sorted([tuple(row) for row in result])

def normalize_mongo(result):
    cleaned = []
    for doc in result:
        doc.pop("_id", None)
        cleaned.append(tuple(doc.values()))
    return sorted(cleaned)

def compare(sql_result, mongo_result):
    return normalize_sql(sql_result) == normalize_mongo(mongo_result)

#----------------------------------------------------------#
DEFAULT_SCHEMA_PATH = "schema.json"

def run_lexer(sql):
    print(f"\n[Lexer Output] Processing: {sql}")
    lexer = get_lexer()
    try:
        tokens = lexer.tokenize(sql)
        for token in tokens:
            print(f"Token(type='{token.type}', value='{token.value}')")
    except LexerError as e:
        print(f"Lexical Error: {e.message} at line {e.line}, column {e.column}")
    except Exception as e:
        print(f"Error: {e}")

def run_parser(sql):
    print(f"\n[Parser Output] Processing: {sql}")
    parser = get_parser()
    try:
        ast = parser.parse(sql)
        print(ast)
    except SyntaxError as e:
        print(f"Syntax Error: {e}")
    except Exception as e:
        print(f"Error: {e}")

def get_user_schema():
    print("\nSchema Options:")
    print("1. Use default schema (schema.json)")
    print("2. Load custom schema file")
    
    choice = input("Enter choice: ").strip()

    if choice == '1':
        schema_path = DEFAULT_SCHEMA_PATH
        # Check if default schema exists
        if not os.path.exists(schema_path):
             print(f"Schema Error: Default schema file '{schema_path}' not found.")
             return None
    elif choice == '2':
        schema_path = input("Enter path to schema JSON file: ").strip()
    else:
        print("Invalid choice. Returning to main menu.")
        return None

    try:
        schema = load_schema(schema_path)
        print("Schema loaded successfully.")
        return schema
    except SchemaError as e:
        print(f"Schema Error: {e}")
        return None
    except Exception as e:
        print(f"Schema Error: {e}")
        return None

from sql_to_mongo_transpiler.codegen.mongodb_generator import MongoDBGenerator

def run_full_pipeline(sql, schema):
    print(f"\n[Full Pipeline] Processing: {sql}")
    parser = get_parser()
    analyzer = SemanticAnalyzer(schema)
    generator = MongoDBGenerator()
    
    try:
        # 1. Parse
        ast = parser.parse(sql)
        # 2. Semantic Analysis
        analyzer.validate_query(ast)
        
        print("Query is semantically valid.")
        
        # 3. Code Generation
        mongo_query = generator.generate(ast)
        print("MongoDB Query:")
        print(mongo_query["string"])
        
    except SyntaxError as e:
        print(f"Syntax Error: {e}")
    except SemanticError as e:
        print(f"Semantic Error: {e}")
    except Exception as e:
        print(f"Error: {e}")

def run_with_execution(sql, schema):
    print(f"\n[Execution Mode] Processing: {sql}")

    parser = get_parser()
    analyzer = SemanticAnalyzer(schema)
    generator = MongoDBGenerator()

    try:
        # 1. Parse
        ast = parser.parse(sql)

        # 2. Semantic check
        analyzer.validate_query(ast)

        # 3. Generate Mongo string
        mongo_data = generator.generate(ast)

        print("\nGenerated Mongo Query:")
        print(mongo_data["string"])

        # 4. Parse Mongo string → executable form
        #collection, filter_dict, projection_dict = parse_mongo_string(mongo_query)
        collection = mongo_data["collection"]
        print("\nParsed Mongo:")
        print("Collection:", collection)
        # Handle normal find
        if "filter" in mongo_data:
            filter_dict = mongo_data["filter"]
            projection_dict = mongo_data["projection"]
            print("Filter:", filter_dict)
            print("Projection:", projection_dict)
            #mongo_result = run_mongo(collection, filter_dict, projection_dict)
            cursor = db[collection].find(filter_dict, projection_dict)
            # APPLY SORT
            if "sort" in mongo_data:
                cursor = cursor.sort(list(mongo_data["sort"].items()))
            # APPLY LIMIT
            if "limit" in mongo_data:
                cursor = cursor.limit(mongo_data["limit"])
            mongo_result = list(cursor)
            # Handle aggregate
        elif "pipeline" in mongo_data:
            pipeline = mongo_data["pipeline"]
            print("Pipeline:", pipeline)
            mongo_result = list(db[collection].aggregate(mongo_data["pipeline"]))
        else:
            raise ValueError("Unknown MongoDB operation type")
        # 5. Execute SQL
        sql_result = run_sql(sql)
        print("\nSQL Result:", sql_result)

        # 6. MongoDB result
        #mongo_result = run_mongo(collection, filter_dict, projection_dict)
        print("\nMongo Result:", mongo_result)
        print("\n================ DEBUG INFO ================")
        print("SQL rows count:", len(sql_result))
        print("Mongo docs count:", len(mongo_result))

        # 7. Compare
        #match = compare(sql_result, mongo_result)
        #print("\n Matched!" if match else "\n It's a Mismatch!")

    except Exception as e:
        print(f"Error: {e}")

#def print_menu():
#   print("\n" + "="*40)
 #   print(" SQL to MongoDB Transpiler - Phase Tester")
  #  print("="*40)
   # print("1. Lexical Analysis (Tokens)")
    #print("2. Syntax Analysis (AST)")
    #print("3. Full Pipeline (Lexer + Parser + Semantic)")
    #print("4. Exit")
#    print("="*40)

def print_menu():
    print("\n" + "="*40)
    print(" SQL to MongoDB Transpiler")
    print("="*40)
    print("1. Lexical Analysis (Tokens)")
    print("2. Syntax Analysis (AST)")
    print("3. Full Pipeline")
    print("4. Execute + Validate ")
    print("5. Execute + Optimization")
    print("6.Exit")
    print("="*40)



def run_with_execution_and_optimization(sql, schema):
    print(f"\n[Execution + Optimization Mode] Processing: {sql}")

    parser = get_parser()
    analyzer = SemanticAnalyzer(schema)
    generator = MongoDBGenerator()
    optimizer = MongoOptimizer()

    try:
        # ---------------- 1. Parse ----------------
        ast = parser.parse(sql)

        # ---------------- 2. Semantic Check ----------------
        analyzer.validate_query(ast)

        # ---------------- 3. Generate Mongo ----------------
        mongo_data = generator.generate(ast)
        #mongo_query = mongo_data["string"]

        print("\nGenerated Mongo Query:")
        print(mongo_data["string"])

        # ---------------- 4.  Optimization Stage ----------------
        try:
            #optimized_query = optimizer.optimize(mongo_query)
            before_query = mongo_data["string"]
            mongo_data = optimizer.optimize(mongo_data)
            after_query = mongo_data["string"]
            print("\n--- Optimization Stage ---")
            print("Before:", before_query)
            print("After :", after_query)
            print("\n--------------------------")

        except Exception as e:
            print("⚠️ Optimization failed:", e)
            optimized_query = mongo_query

        # ---------------- 5. Execute Mongo ----------------
        collection = mongo_data["collection"]

        print("\nParsed Mongo:")
        print("Collection:", collection)

        if "filter" in mongo_data:
            filter_dict = mongo_data["filter"]
            projection_dict = mongo_data["projection"]

            print("Filter:", filter_dict)
            print("Projection:", projection_dict)

           # mongo_result = run_mongo(collection, filter_dict, projection_dict)
            cursor = db[collection].find(filter_dict, projection_dict)
            # APPLY SORT
            if "sort" in mongo_data:
                cursor = cursor.sort(list(mongo_data["sort"].items()))
            # APPLY LIMIT
            if "limit" in mongo_data:
                cursor = cursor.limit(mongo_data["limit"])

            mongo_result = list(cursor)
        elif "pipeline" in mongo_data:
            pipeline = mongo_data["pipeline"]

            print("Pipeline:", pipeline)

            mongo_result = list(db[collection].aggregate(pipeline))

        else:
            raise ValueError("Unknown MongoDB operation type")

        # ---------------- 6. Execute SQL ----------------
        sql_result = run_sql(sql)

        print("\nSQL Result:", sql_result)

        # ---------------- 7. Mongo Result ----------------
        print("\nMongo Result:", mongo_result)

        print("\n================ DEBUG INFO ================")
        print("SQL rows count:", len(sql_result))
        print("Mongo docs count:", len(mongo_result))

    except SyntaxError as e:
        print(f"Syntax Error: {e}")
    except SemanticError as e:
        print(f"Semantic Error: {e}")
    except Exception as e:
        print(f"Error: {e}")

def main():
    while True:
        print_menu()
        choice = input("Enter choice: ").strip()

        if choice == '1':
            sql = input("\nEnter SQL Query: ").strip()
            if sql:
                run_lexer(sql)
            else:
                print("Error: Empty input")
        elif choice == '2':
            sql = input("\nEnter SQL Query: ").strip()
            if sql:
                run_parser(sql)
            else:
                print("Error: Empty input")
        elif choice == '3':
            # Ask for schema first
            schema = get_user_schema()
            if schema:
                sql = input("\nEnter SQL Query: ").strip()
                if sql:
                    run_full_pipeline(sql, schema)
                else:
                    print("Error: Empty input")
        elif choice == '4':
            schema = get_user_schema()
            if schema:
                sql = input("\nEnter SQL Query: ").strip()
                if sql:
                    run_with_execution(sql, schema)
        elif choice == '5':
            schema = get_user_schema()
            if schema:
                sql = input("\nEnter SQL Query: ").strip()
                if sql:
                    run_with_execution_and_optimization(sql, schema)
        elif choice == '6':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
