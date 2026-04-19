import pytest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from sql2mongo.parser.sql_parser import get_parser
from sql2mongo.semantic.semantic_analyzer import SemanticAnalyzer
from sql2mongo.codegen.mongodb_generator import MongoDBGenerator
from sql2mongo.codegen.optimizer import MongoOptimizer

SCHEMA = {
    "users": {
        "id": "int",
        "name": "string",
        "age": "int",
        "city": "string",
        "balance": "float"
    },
    "orders": {
        "order_id": "int",
        "user_id": "int",
        "amount": "float",
        "status": "string"
    }
}

@pytest.fixture
def transpiler():
    parser = get_parser()
    analyzer = SemanticAnalyzer(SCHEMA)
    generator = MongoDBGenerator()
    optimizer = MongoOptimizer()
    
    def transpile(sql_query):
        ast = parser.parse(sql_query)
        analyzer.validate_query(ast)
        mongo_data = generator.generate(ast)
        # Apply optimization if possible (simulate full cli pipeline)
        optimized_data = optimizer.optimize(mongo_data)
        return optimized_data
        
    return transpile

# ----------------- BASIC SELECT -----------------
def test_select_all(transpiler):
    res = transpiler("SELECT * FROM users;")
    assert res["string"].replace(" ", "") == "db.users.find({})"
    assert res["filter"] == {}

def test_select_case_insensitivity(transpiler):
    res = transpiler("sElEcT * fRoM users WhErE age = 20;")
    assert res["filter"] == {"age": 20}

def test_select_columns(transpiler):
    res = transpiler("SELECT name, age FROM users;")
    assert "name" in res["string"] and "age" in res["string"]
    assert res["projection"] == {"name": 1, "age": 1}

def test_select_qualified_columns(transpiler):
    res = transpiler("SELECT users.name, users.city FROM users;")
    # Projection strips table name for single-table queries
    assert {"name": 1, "city": 1}.items() <= res["projection"].items()

# ----------------- WHERE CONDITIONS -----------------
def test_where_equality(transpiler):
    res = transpiler("SELECT * FROM users WHERE city = 'Delhi';")
    assert res["filter"] == {"city": "Delhi"}

def test_where_inequality(transpiler):
    res = transpiler("SELECT * FROM users WHERE city != 'Mumbai';")
    assert res["filter"] == {"city": {"$ne": "Mumbai"}}

def test_where_greater_less(transpiler):
    res_gt = transpiler("SELECT * FROM users WHERE age > 25;")
    assert res_gt["filter"] == {"age": {"$gt": 25}}
    
    res_lte = transpiler("SELECT * FROM users WHERE age <= 30;")
    assert res_lte["filter"] == {"age": {"$lte": 30}}

def test_where_between(transpiler):
    res = transpiler("SELECT * FROM users WHERE age BETWEEN 20 AND 30;")
    assert res["filter"] == {"age": {"$gte": 20, "$lte": 30}}

def test_where_in_list(transpiler):
    res = transpiler("SELECT * FROM users WHERE city IN ('Delhi', 'Pune');")
    assert res["filter"] == {"city": {"$in": ["Delhi", "Pune"]}}

# ----------------- LOGICAL OPERATORS -----------------
def test_logical_and(transpiler):
    res = transpiler("SELECT * FROM users WHERE age >= 18 AND city = 'Delhi';")
    # Optimizer collapses $and if fields are unique into merged doc
    assert res["filter"] == {"age": {"$gte": 18}, "city": "Delhi"}

def test_logical_or(transpiler):
    res = transpiler("SELECT * FROM users WHERE age < 18 OR age > 60;")
    # Optimizer keeps disjoint ranges logic wrapper
    assert "$or" in res["filter"] or "age" in res["filter"]

# ----------------- ORDER BY & LIMIT -----------------
def test_order_by_single(transpiler):
    res = transpiler("SELECT * FROM users ORDER BY age DESC;")
    assert res["sort"] == {"age": -1}

def test_order_by_multiple(transpiler):
    res = transpiler("SELECT * FROM users ORDER BY age DESC, city ASC;")
    assert res["sort"] == {"age": -1, "city": 1}

def test_limit_only(transpiler):
    res = transpiler("SELECT * FROM users LIMIT 10;")
    assert res["limit"] == 10

def test_order_by_limit_combined(transpiler):
    res = transpiler("SELECT * FROM users WHERE age > 20 ORDER BY balance DESC LIMIT 5;")
    assert res["limit"] == 5
    assert res["sort"] == {"balance": -1}
    assert res["filter"] == {"age": {"$gt": 20}}

# ----------------- AGGREGATION & GROUP BY -----------------
def test_group_by_single(transpiler):
    res = transpiler("SELECT city, COUNT(id) FROM users GROUP BY city;")
    pipeline = res["pipeline"]
    group_stage = next(stage["$group"] for stage in pipeline if "$group" in stage)
    assert group_stage["_id"] == "$city"

def test_group_by_multiple(transpiler):
    res = transpiler("SELECT city, age, COUNT(id) FROM users GROUP BY city, age;")
    pipeline = res["pipeline"]
    group_stage = next(stage["$group"] for stage in pipeline if "$group" in stage)
    assert group_stage["_id"] == {"city": "$city", "age": "$age"}

def test_count_star(transpiler):
    res = transpiler("SELECT city, COUNT(*) FROM users GROUP BY city;")
    pipeline = res["pipeline"]
    group_stage = next(stage["$group"] for stage in pipeline if "$group" in stage)
    assert group_stage["count"] == {"$sum": 1}

def test_count_column(transpiler):
    res = transpiler("SELECT city, COUNT(age) FROM users GROUP BY city;")
    pipeline = res["pipeline"]
    group_stage = next(stage["$group"] for stage in pipeline if "$group" in stage)
    assert "count_age" in group_stage
    assert "$sum" in group_stage["count_age"]
    assert "$cond" in group_stage["count_age"]["$sum"]

def test_multiple_aggregates(transpiler):
    res = transpiler("SELECT city, SUM(balance), AVG(age), MIN(id), MAX(balance) FROM users GROUP BY city;")
    pipeline = res["pipeline"]
    group_stage = next(stage["$group"] for stage in pipeline if "$group" in stage)
    assert "sum_balance" in group_stage and group_stage["sum_balance"] == {"$sum": "$balance"}
    assert "avg_age" in group_stage and group_stage["avg_age"] == {"$avg": "$age"}
    assert "min_id" in group_stage and group_stage["min_id"] == {"$min": "$id"}
    assert "max_balance" in group_stage and group_stage["max_balance"] == {"$max": "$balance"}

def test_group_by_with_where(transpiler):
    res = transpiler("SELECT city, SUM(balance) FROM users WHERE age > 20 GROUP BY city;")
    pipeline = res["pipeline"]
    # match stage should precede group
    match_stage = next(stage["$match"] for stage in pipeline if "$match" in stage)
    assert match_stage == {"age": {"$gt": 20}}

def test_having_clause(transpiler):
    res = transpiler("SELECT city, SUM(balance) FROM users GROUP BY city HAVING SUM(balance) > 1000;")
    pipeline = res["pipeline"]
    # match stage applied AFTER grouping
    match_stages = [stage["$match"] for stage in pipeline if "$match" in stage]
    assert len(match_stages) == 1
    assert match_stages[-1] == {"sum_balance": {"$gt": 1000.0}}

def test_order_by_group_by(transpiler):
    # Tests aggregation sorting
    res = transpiler("SELECT city, SUM(balance) FROM users GROUP BY city ORDER BY sum_balance DESC LIMIT 10;")
    pipeline = res["pipeline"]
    sort_stage = next(stage["$sort"] for stage in pipeline if "$sort" in stage)
    assert sort_stage == {"sum_balance": -1}
    limit_stage = next(stage["$limit"] for stage in pipeline if "$limit" in stage)
    assert limit_stage == 10

# ----------------- JOINS -----------------
def test_explicit_join(transpiler):
    res = transpiler("SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id;")
    pipeline = res["pipeline"]
    lookup = next(stage["$lookup"] for stage in pipeline if "$lookup" in stage)
    assert lookup["from"] == "orders"
    assert lookup["localField"] == "id"
    assert lookup["foreignField"] == "user_id"
    
def test_implicit_join(transpiler):
    res = transpiler("SELECT users.name, orders.amount FROM users, orders WHERE users.id = orders.user_id;")
    pipeline = res["pipeline"]
    lookup = next(stage["$lookup"] for stage in pipeline if "$lookup" in stage)
    assert lookup["from"] == "orders"

def test_join_with_extra_filters(transpiler):
    res = transpiler("SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 20;")
    pipeline = res["pipeline"]
    match = next((stage["$match"] for stage in pipeline if "$match" in stage), None)
    assert match is not None
    assert "age" in str(match)

def test_join_projection(transpiler):
    res = transpiler("SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id;")
    pipeline = res["pipeline"]
    project = next(stage["$project"] for stage in pipeline if "$project" in stage)
    assert "name" in project and project["name"] == 1
    assert "orders.amount" in project and project["orders.amount"] == 1

# ----------------- SUBQUERIES -----------------
def test_in_subquery(transpiler):
    res = transpiler("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);")
    pipeline = res["pipeline"]
    lookup = next(stage["$lookup"] for stage in pipeline if "$lookup" in stage)
    assert lookup["from"] == "orders"
    # assert non empty matching array
    match = next(stage["$match"] for stage in pipeline if "$match" in stage)
    assert "orders" in match and "$ne" in match["orders"]

# ----------------- OPTIMIZER SPECIFIC TESTS -----------------
def test_optimizer_or_conversion(transpiler):
    # Optimizer should collapse multiple OR equalities into $in
    res = transpiler("SELECT * FROM users WHERE age = 20 OR age = 21 OR age = 22;")
    assert res["filter"] == {"age": {"$in": [20, 21, 22]}}

def test_optimizer_range_merge(transpiler):
    # Optimizer merges GT and LT conditions from multiple OR paths if distinct arrays
    # wait: The optimizer script merges ">" values using min() across OR bounds.
    res = transpiler("SELECT * FROM users WHERE age > 20 OR age > 30;")
    assert res["filter"] == {"age": {"$gt": 20}}
