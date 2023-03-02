-- create a commerce schema
CREATE SCHEMA commerce;

-- Use commerce schema
SET
    search_path TO commerce;

-- create a table named products
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price REAL NOT NULL
);

-- create a users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    PASSWORD VARCHAR(255) NOT NULL
);

ALTER TABLE
    products REPLICA IDENTITY FULL;

ALTER TABLE
    users REPLICA IDENTITY FULL;