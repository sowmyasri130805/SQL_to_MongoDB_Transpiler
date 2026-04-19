-- Drop tables if they exist (for repeatability)
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;

-- Users table
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT,
    age INT,
    city TEXT
);

-- Orders table
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    user_id INT,
    amount INT,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Insert sample data
INSERT INTO users (id, name, age, city) VALUES
(1, 'Alice', 25, 'Delhi'),
(2, 'Bob', 30, 'Mumbai'),
(3, 'Charlie', 28, 'Bangalore');

INSERT INTO orders (order_id, user_id, amount) VALUES
(101, 1, 500),
(102, 2, 300),
(103, 1, 700),
(104, 3, 200);
