# 🚀 Custom Database Management System (DBMS)

A **high-performance DBMS** built from scratch using **C++ (backend)** and **React (frontend)** with **B+ Tree indexing**, a custom storage engine, and a SQL-like query parser.

## 🌟 Key Features

### 📂 Custom Storage Engine
- Databases stored as **folders**, tables as `.dat` (data) and `.idx` (index) files
- Metadata & schema stored in `.bin` files

### ⚡ Optimized Performance
- **B+ Tree indexing** for **O(log n)** search, insert, update, and delete operations
- Efficient **record management** using primary/foreign keys

### 🔍 SQL-like Query Support
- **CRUD Operations**:
  - `CREATE`, `DROP` (databases & tables)
  - `INSERT`, `UPDATE`, `DELETE`, `SELECT` (with `WHERE` clauses)
- **Joins** for multi-table queries
- **Logical Operators**: `AND`, `OR`, `NOT`, `LIKE`

### 💻 Modern Frontend (React)
- Interactive **query editor** with syntax highlighting
- Tabular **result display** for easy data visualization

## 🛠️ Tech Stack
- **Backend**: C++ (with CMake, Boost libraries)
- **Frontend**: React.js
- **Indexing**: B+ Tree

## 🚀 Installation & Setup

### Prerequisites
- **C++17** (or later)
- **CMake** (for building)
- **Boost Libraries** (`filesystem`, `program_options`)
- **Node.js & npm** (for frontend)

### Backend Setup
1. Clone the repo:
   ```bash
   git clone git@github.com:AmirHashmi017/Database-Project.git
   cd Database Project/Database Project
2. Build with CMake:
   ```bash
   mkdir build
   cd build
   cmake ..
   cmake --build .
   cd debug
   ./Database.exe

### Gateway Service Setup
1. Navigate to the gateway-service directory:
   ```bash
   cd Database Project/gateway-service
2. Install dependencies & run:
   ```bash
   npm install
   node server.js

### Frontend Setup
1. Navigate to the frontend directory:
   ```bash
   cd Database Project/db-frontend
2. Install dependencies & run:
   ```bash
   npm install
   npm run start
3. Access the app at http://localhost:3000

### 📖 Usage Examples

#### Create a Database
   ```bash
   CREATE DATABASE skylines;
   USE skylines;
   ```
   
#### Create a table
   ```bash
   CREATE TABLE users (id INT, name STRING(50), age INT, PRIMARY KEY (id));
   CREATE TABLE orders (order_id INT, user_id INT, product_id INT, PRIMARY KEY (order_id), FOREIGN KEY (user_id) REFERENCES users(id), FOREIGN KEY (product_id) REFERENCES 
   products(product_id));
   ```

#### CRUD Operartions
   ```bash
   INSERT INTO users VALUES (3, 'Bob Johnson', 40);
   UPDATE users SET name = 'John Smith' WHERE id = 1;
   DELETE FROM users WHERE age < 30;
   SELECT * FROM users WHERE age > 20 AND name LIKE 'John';
   SELECT users.name,orders.order_id FROM users JOIN orders ON users.id = orders.user_id;
   ```

#### Drop Database and Table
   ```bash
   DROP TABLE users;
   DROP DATABASE skylines;
   ```

#### For more help about queries see file 
  ```bash
  cd Database Project/Testing Queries.txt
  ```
   

   

