from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["transpiler_db"]

# Drop existing collections (for clean setup)
db.users.drop()
db.orders.drop()

# Users collection
users_data = [
    {"id": 1, "name": "Alice", "age": 25, "city": "Delhi"},
    {"id": 2, "name": "Bob", "age": 30, "city": "Mumbai"},
    {"id": 3, "name": "Charlie", "age": 28, "city": "Bangalore"}
]

# Orders collection
orders_data = [
    {"order_id": 101, "user_id": 1, "amount": 500},
    {"order_id": 102, "user_id": 2, "amount": 300},
    {"order_id": 103, "user_id": 1, "amount": 700},
    {"order_id": 104, "user_id": 3, "amount": 200}
]

db.users.insert_many(users_data)
db.orders.insert_many(orders_data)

print("MongoDB collections created and seeded ✅")
