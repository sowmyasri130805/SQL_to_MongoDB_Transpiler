import psycopg2
from pymongo import MongoClient

# PostgreSQL
pg = psycopg2.connect(
    dbname="testdb",
    user="postgres",
    password="password",  # change if needed
    host="localhost"
)

cur = pg.cursor()
cur.execute("SELECT * FROM users;")
print("PostgreSQL:", cur.fetchall())

# MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["testdb"]
print("MongoDB:", list(db.users.find({}, {"_id": 0})))
