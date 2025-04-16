#ifndef DATABASE_MANAGER_H
#define DATABASE_MANAGER_H

#include "catalog.h"
#include "bptree.h"
#include <string>
#include <vector>
#include <map>
#include <variant>
#include <fstream>
#include <iostream>
#include <filesystem>
#ifdef _WIN32
#include <windows.h> // For Windows systems
#else
#include <unistd.h> // For Unix systems
#include <limits.h> // For PATH_MAX
#endif

// Define a generic record type that can hold different data types
using FieldValue = std::variant<int, float, std::string, bool>;
using Record = std::map<std::string, FieldValue>;

class DatabaseManager {
public:
    DatabaseManager(const std::string& catalog_path = "catalog.bin");
    ~DatabaseManager();

    // Create a new table
    bool createTable(
        const std::string& table_name,
        const std::vector<std::tuple<std::string, std::string, int>>& columns,
        const std::string& primary_key,
        const std::map<std::string, std::pair<std::string, std::string>>& foreign_keys = {}
    );

    // Insert a record into a table
    bool insertRecord(const std::string& table_name, const Record& record);

    // Search for records in a table
    std::vector<Record> searchRecords(const std::string& table_name, const std::string& key_column, const FieldValue& key_value);

    // List all tables
    std::vector<std::string> listTables() const;

    // Get schema for a specific table
    TableSchema getTableSchema(const std::string& table_name) const;
    std::vector<Record> getAllRecords(const std::string& table_name);
    bool updateRecord(const std::string& table_name, const std::string& key_column,
        const FieldValue& key_value, const Record& new_values);

    bool deleteRecord(const std::string& table_name, const std::string& key_column,
        const FieldValue& key_value);

    std::vector<Record> searchRecordsAdvanced(
        const std::string& table_name,
        const std::vector<std::tuple<std::string, FieldValue, std::string>>& conditions);

private:
    Catalog catalog;
    std::string catalog_path;
    std::map<std::string, BPlusTree*> indexes; // Map of table name to index

    // Helper methods
    Column::Type stringToColumnType(const std::string& type_str);
    void saveRecord(std::ofstream& file, const Record& record, const TableSchema& schema, int& offset);
    Record loadRecord(std::ifstream& file, const TableSchema& schema);
    int getFieldSize(const Column& column) const;
    void serializeField(std::ofstream& file, const FieldValue& value, const Column& column);
    FieldValue deserializeField(std::ifstream& file, const Column& column);

    // Create index for a table
    void createIndex(const TableSchema& schema);

    // Load existing indexes
    void loadIndexes();
    
};

#endif