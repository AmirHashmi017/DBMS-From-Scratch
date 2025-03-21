#ifndef DATABASE_MANAGER_H
#define DATABASE_MANAGER_H

#include "storage/catalog.h"
#include "storage/bptree.h"
#include <string>
#include <vector>
#include <map>
#include <variant>
#include <fstream>
#include <iostream>

// Define a generic record type that can hold different data types
using FieldValue = std::variant<int, float, std::string, bool>;
using Record = std::map<std::string, FieldValue>;

class DatabaseManager {
public:
    DatabaseManager(const std::string& catalog_path = "data/catalog.bin");
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