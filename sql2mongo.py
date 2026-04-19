import argparse
import json
import logging
import os
import sys
import re

def preprocess_sql(input_str: str) -> list[str]:
    """
    Strips SQL comments and splits by semicolons into discrete queries.
    """
    queries = []
    # Remove single-line comments
    cleaned = re.sub(r'--.*$', '', input_str, flags=re.MULTILINE)
    
    # Split by semicolon
    for q in cleaned.split(';'):
        q_stripped = q.strip()
        if q_stripped:
            queries.append(q_stripped + ";")
            
    return queries

# Assume there is a function like this, we implement it using existing modules
def transpile(schema: dict, query: str) -> list:
    """
    Core transpiler wrapper.
    Converts SQL to MongoDB Query JSON array.
    """
    try:
        from sql2mongo.parser.sql_parser import get_parser
        from sql2mongo.semantic.semantic_analyzer import SemanticAnalyzer, SemanticError
        from sql2mongo.codegen.mongodb_generator import MongoDBGenerator
        from sql2mongo.codegen.optimizer import MongoOptimizer
        from sql2mongo.lexer.sql_lexer import LexerError
    except ImportError as e:
        raise RuntimeError(f"Transpiler modules or dependencies not found. Details: {e}")

    parser = get_parser()
    analyzer = SemanticAnalyzer(schema)
    generator = MongoDBGenerator()
    optimizer = MongoOptimizer()

    queries = preprocess_sql(query)
    results = []

    for q in queries:
        try:
            ast = parser.parse(q)
            analyzer.validate_query(ast)
            mongo_data = generator.generate(ast)
            optimized_data = optimizer.optimize(mongo_data)
            results.append(optimized_data.get("string", ""))
        except Exception as e:
            results.append(f"Error: {str(e)}")

    return results


def setup_logger(verbose: bool):
    """Configures the logging level based on verbosity."""
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="[%(levelname)s] %(message)s"
    )

def load_schema_file(filepath):
    if not os.path.exists(filepath):
        print(f"Error: Schema file '{filepath}' not found.", file=sys.stderr)
        return None, None
    try:
        with open(filepath, 'r') as f:
            return json.load(f), filepath
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON schema format in '{filepath}'. Details: {e}", file=sys.stderr)
        return None, None
    except Exception as e:
        print(f"Error reading schema file: {e}", file=sys.stderr)
        return None, None

def interactive_shell(args):
    """
    Runs the transpiler in an interactive shell mode.
    """
    schema, current_schema_path = load_schema_file(args.schema)
    if schema is None:
        sys.exit(1)

    print("=====================================================")
    print(" SQL to MongoDB Transpiler - Interactive Shell")
    print(" Type 'exit' or 'quit' to close the shell.")
    print("=====================================================")
    while True:
        try:
            sql = input("sql2mongo> ").strip()
            if sql.lower() in ("exit", "quit"):
                break
            if not sql:
                continue

            if sql == ":show schema":
                print(f"Current schema: {current_schema_path}")
                continue

            if sql.startswith(":set schema "):
                parts = sql.split(" ", 2)
                if len(parts) == 3:
                    new_path = parts[2].strip()
                    new_schema, resolved_path = load_schema_file(new_path)
                    if new_schema is not None:
                        schema = new_schema
                        current_schema_path = resolved_path
                        print(f"Schema updated to: {current_schema_path}")
                else:
                    print("Usage: :set schema <path_to_schema.json>")
                continue

            logging.debug(f"Input received: {sql}")
            
            results = transpile(schema, sql)
            
            for res in results:
                print(res)

        except (KeyboardInterrupt, EOFError):
            print("\nExiting shell...")
            break
        except Exception as e:
            # Handle exception gracefully without breaking shell
            print(f"Transpiler Error: {e}")

def main():
    # Detect the split between 'shell' sub-command and default 'transpile' behavior
    if len(sys.argv) > 1 and sys.argv[1] == "shell":
        parser = argparse.ArgumentParser(
            prog="sql2mongo shell",
            description="Run in interactive shell mode",
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        parser.add_argument("--schema", required=True, help="Path to JSON schema file")
        parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
        parser.add_argument("--verbose", action="store_true", help="Print debug/log steps")
        
        args = parser.parse_args(sys.argv[2:])
        args.command = "shell"
    else:
        parser = argparse.ArgumentParser(
            prog="sql2mongo",
            description="SQL to MongoDB Transpiler CLI",
            epilog="Examples:\n"
                   "  sql2mongo --schema schema.json --query \"SELECT * FROM users\"\n"
                   "  sql2mongo --schema schema.json --query query.sql --pretty --output result.json\n"
                   "  sql2mongo shell --schema schema.json --verbose",
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        parser.add_argument("--schema", required=True, help="Path to JSON schema file")
        parser.add_argument("--query", required=True, help="Raw SQL string OR path to a .sql file")
        parser.add_argument("--output", help="Path to save MongoDB query JSON (Optional)")
        parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
        parser.add_argument("--verbose", action="store_true", help="Print debug/log steps")
        
        args = parser.parse_args()
        args.command = "transpile"

    # 1. Provide Context Logging
    setup_logger(args.verbose)

    # 2. Handle commands
    if args.command == "shell":
        logging.debug("Entering interactive shell mode")
        interactive_shell(args)
        return

    if args.command == "transpile":
        # Load Schema
        logging.debug(f"Loading schema from: {args.schema}")
        schema, _ = load_schema_file(args.schema)
        if schema is None:
            sys.exit(1)

        # Process query
        query_str = args.query

        if os.path.isfile(query_str):
            logging.debug(f"Query argument detected as file: {query_str}")
            try:
                with open(query_str, 'r') as f:
                    query_str = f.read()
                logging.debug(f"Read {len(query_str)} bytes from query file")
            except Exception as e:
                print(f"Error: Failed to read query file '{query_str}'. Details: {e}", file=sys.stderr)
                sys.exit(1)
        else:
            logging.debug("Query argument detected as raw SQL string")

        # Execute transpile
        try:
            logging.debug(f"Transpiling query: {query_str}")
            result = transpile(schema, query_str)
        except Exception as e:
             print(f"Error: Transpiler failed. Details: {e}", file=sys.stderr)
             sys.exit(1)

        # Print and/or Save output
        if not isinstance(result, list):
            print("Error: Transpiler failed structurally.", file=sys.stderr)
            sys.exit(1)
            
        for res in result:
            print(res)

        if hasattr(args, "output") and args.output:
            logging.debug(f"Saving output to: {args.output}")
            try:
                with open(args.output, 'w') as f:
                    json.dump(result, f, indent=4)
            except Exception as e:
                 print(f"Error: Failed to write to output file '{args.output}'. Details: {e}", file=sys.stderr)
                 sys.exit(1)

if __name__ == "__main__":
    main()
