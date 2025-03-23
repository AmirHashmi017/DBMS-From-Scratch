
#include "database_manager.h"
#include <iostream>

int main() {
    DatabaseManager db;

    // Create a table for users
    db.createTable("users",
        {
            {"id", "int", 0},
            {"name", "string", 50},
            {"email", "string", 100},
            {"active", "bool", 0}
        },
        "id" // primary key
    );

    // Create a table for orders with a foreign key
    db.createTable("orders",
        {
            {"order_id", "int", 0},
            {"user_id", "int", 0},
            {"total", "float", 0},
            {"date", "string", 10}
        },
        "order_id", // primary key
        { {"user_id", {"users", "id"}} } // foreign keys
    );

    // Insert records
    db.insertRecord("users", {
        {"id", 1},
        {"name", std::string("Alice")},
        {"email", std::string("alice@example.com")},
        {"active", true}
        });

    db.insertRecord("users", {
        {"id", 2},
        {"name", std::string("Bob")},
        {"email", std::string("bob@example.com")},
        {"active", false}
        });

    db.insertRecord("orders", {
        {"order_id", 101},
        {"user_id", 1},
        {"total", 129.99f},
        {"date", std::string("2025-03-21")}
        });

    // Search for records
    std::cout << "\nSearching for user with id = 1:" << std::endl;
    auto results = db.searchRecords("users", "id", 1);
    for (const auto& record : results) {
        std::cout << "  ID: " << std::get<int>(record.at("id"))
            << ", Name: " << std::get<std::string>(record.at("name"))
            << ", Email: " << std::get<std::string>(record.at("email"))
            << ", Active: " << std::get<bool>(record.at("active")) << std::endl;
    }

    // List all tables
    std::cout << "\nAvailable tables:" << std::endl;
    for (const auto& table : db.listTables()) {
        std::cout << "  " << table << std::endl;
    }

    return 0;
}

//Command for running
//g++ -std=c++17 -Iinclude src/main.cpp src/storage/catalog.cpp src/storage/bptree.cpp src/storage/record.cpp -o main