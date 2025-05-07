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
   git clone <repo-url>
   cd <repo-name>/backend
