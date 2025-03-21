#include "storage/database_manager.h"
#include <filesystem>
#include <algorithm>
#include <stdexcept>

DatabaseManager::DatabaseManager(const std::string& catalog_path) : catalog_path(catalog_path) {
    // Load the catalog
    catalog.load(catalog_path);
    
    // Load existing indexes
    loadIndexes();
}

DatabaseManager::~DatabaseManager() {
    // Save the catalog
    catalog.save(catalog_path);
    
    // Clean up indexes
    for (auto& pair : indexes) {
        delete pair.second;
    }
}

bool DatabaseManager::createTable(
    const std::string& table_name,
    const std::vector<std::tuple<std::string, std::string, int>>& columns,
    const std::string& primary_key,
    const std::map<std::string, std::pair<std::string, std::string>>& foreign_keys
) {
    // Check if table already exists
    for (const auto& table : catalog.tables) {
        if (table.name == table_name) {
            std::cerr << "Table '" << table_name << "' already exists." << std::endl;
            return false;
        }
    }
    
    // Create new table schema
    TableSchema new_table;
    new_table.name = table_name;
    new_table.data_file_path = "data/" + table_name + ".dat";
    new_table.index_file_path = "data/" + table_name + ".idx";
    
    // Add columns
    for (const auto& [name, type_str, length] : columns) {
        Column column;
        column.name = name;
        column.type = stringToColumnType(type_str);
        column.length = length;
        column.is_primary_key = (name == primary_key);
        column.is_foreign_key = foreign_keys.find(name) != foreign_keys.end();
        
        if (column.is_foreign_key) {
            auto it = foreign_keys.find(name);
            column.references_table = it->second.first;
            column.references_column = it->second.second;
        }
        
        new_table.columns.push_back(column);
    }
    
    // Validate primary key
    bool primary_key_found = false;
    for (const auto& column : new_table.columns) {
        if (column.is_primary_key) {
            primary_key_found = true;
            break;
        }
    }
    
    if (!primary_key_found) {
        std::cerr << "Primary key '" << primary_key << "' not found in columns." << std::endl;
        return false;
    }
    
    // Add the table to the catalog
    catalog.tables.push_back(new_table);
    catalog.save(catalog_path);
    
    // Create index for the table
    createIndex(new_table);
    
    std::cout << "Table '" << table_name << "' created successfully." << std::endl;
    return true;
}

bool DatabaseManager::insertRecord(const std::string& table_name, const Record& record) {
    // Find the table
    TableSchema schema;
    bool found = false;
    
    for (const auto& table : catalog.tables) {
        if (table.name == table_name) {
            schema = table;
            found = true;
            break;
        }
    }
    
    if (!found) {
        std::cerr << "Table '" << table_name << "' not found." << std::endl;
        return false;
    }
    
    // Validate record against schema
    for (const auto& column : schema.columns) {
        if (record.find(column.name) == record.end()) {
            std::cerr << "Missing value for column '" << column.name << "'." << std::endl;
            return false;
        }
        
        // Type checking could be added here
    }
    
    // Open the data file in append mode
    std::ofstream data_file(schema.data_file_path, std::ios::binary | std::ios::app);
    if (!data_file) {
        std::cerr << "Failed to open data file for table '" << table_name << "'." << std::endl;
        return false;
    }
    
    // Get the current offset
    int offset = data_file.tellp();
    
    // Save the record
    saveRecord(data_file, record, schema, offset);
    data_file.close();
    
    // Find the primary key and its value
    std::string primary_key_column;
    FieldValue primary_key_value;
    
    for (const auto& column : schema.columns) {
        if (column.is_primary_key) {
            primary_key_column = column.name;
            primary_key_value = record.at(column.name);
            break;
        }
    }
    
    // Insert into index
    if (indexes.find(table_name) != indexes.end()) {
        // Convert primary key value to integer (assuming primary keys are int for now)
        int key_int = std::get<int>(primary_key_value);
        indexes[table_name]->insert(key_int, offset);
    }
    
    std::cout << "Record inserted into table '" << table_name << "' at offset " << offset << std::endl;
    return true;
}

std::vector<Record> DatabaseManager::searchRecords(const std::string& table_name, const std::string& key_column, const FieldValue& key_value) {
    std::vector<Record> results;
    
    // Find the table
    TableSchema schema;
    bool found = false;
    
    for (const auto& table : catalog.tables) {
        if (table.name == table_name) {
            schema = table;
            found = true;
            break;
        }
    }
    
    if (!found) {
        std::cerr << "Table '" << table_name << "' not found." << std::endl;
        return results;
    }
    
    // If searching by primary key and index exists, use it
    if (indexes.find(table_name) != indexes.end()) {
        bool is_primary_key = false;
        for (const auto& column : schema.columns) {
            if (column.name == key_column && column.is_primary_key) {
                is_primary_key = true;
                break;
            }
        }
        
        if (is_primary_key) {
            // Convert primary key value to integer (assuming primary keys are int for now)
            int key_int = std::get<int>(key_value);
            std::vector<int> offsets = indexes[table_name]->search(key_int);
            
            // Load records from the offsets
            std::ifstream data_file(schema.data_file_path, std::ios::binary);
            if (!data_file) {
                std::cerr << "Failed to open data file for table '" << table_name << "'." << std::endl;
                return results;
            }
            
            for (int offset : offsets) {
                data_file.seekg(offset);
                Record record = loadRecord(data_file, schema);
                results.push_back(record);
            }
            
            data_file.close();
            return results;
        }
    }
    
    // Otherwise, do a sequential scan
    std::ifstream data_file(schema.data_file_path, std::ios::binary);
    if (!data_file) {
        std::cerr << "Failed to open data file for table '" << table_name << "'." << std::endl;
        return results;
    }
    
    // Read all records and filter
    while (data_file) {
        int start_pos = data_file.tellg();
        if (start_pos == -1) break; // EOF
        
        Record record = loadRecord(data_file, schema);
        if (!data_file) break; // EOF or error
        
        // Check if the record matches the search criteria
        if (record.find(key_column) != record.end() && record[key_column] == key_value) {
            results.push_back(record);
        }
    }
    
    data_file.close();
    return results;
}

std::vector<std::string> DatabaseManager::listTables() const {
    std::vector<std::string> table_names;
    for (const auto& table : catalog.tables) {
        table_names.push_back(table.name);
    }
    return table_names;
}

TableSchema DatabaseManager::getTableSchema(const std::string& table_name) const {
    for (const auto& table : catalog.tables) {
        if (table.name == table_name) {
            return table;
        }
    }
    
    throw std::runtime_error("Table '" + table_name + "' not found.");
}

Column::Type DatabaseManager::stringToColumnType(const std::string& type_str) {
    if (type_str == "int") return Column::INT;
    if (type_str == "float") return Column::FLOAT;
    if (type_str == "string") return Column::STRING;
    if (type_str == "char") return Column::CHAR;
    if (type_str == "bool") return Column::BOOL;
    
    throw std::runtime_error("Unknown column type: " + type_str);
}

void DatabaseManager::saveRecord(std::ofstream& file, const Record& record, const TableSchema& schema, int& offset) {
    offset = file.tellp();
    
    // Write each field according to the schema
    for (const auto& column : schema.columns) {
        if (record.find(column.name) != record.end()) {
            serializeField(file, record.at(column.name), column);
        } else {
            // This shouldn't happen if validation is done correctly
            throw std::runtime_error("Missing field: " + column.name);
        }
    }
}

Record DatabaseManager::loadRecord(std::ifstream& file, const TableSchema& schema) {
    Record record;
    
    // Read each field according to the schema
    for (const auto& column : schema.columns) {
        try {
            FieldValue value = deserializeField(file, column);
            record[column.name] = value;
        } catch (const std::exception& e) {
            // If we can't read a complete record, return an empty one
            if (file.eof()) {
                return Record();
            }
            throw;
        }
    }
    
    return record;
}

int DatabaseManager::getFieldSize(const Column& column) const {
    switch (column.type) {
        case Column::INT: return sizeof(int);
        case Column::FLOAT: return sizeof(float);
        case Column::BOOL: return sizeof(bool);
        case Column::STRING:
        case Column::CHAR:
            // Strings have a length field plus the actual string data
            return sizeof(int) + column.length;
        default:
            throw std::runtime_error("Unknown column type");
    }
}

void DatabaseManager::serializeField(std::ofstream& file, const FieldValue& value, const Column& column) {
    switch (column.type) {
        case Column::INT: {
            int val = std::get<int>(value);
            file.write(reinterpret_cast<const char*>(&val), sizeof(val));
            break;
        }
        case Column::FLOAT: {
            float val = std::get<float>(value);
            file.write(reinterpret_cast<const char*>(&val), sizeof(val));
            break;
        }
        case Column::BOOL: {
            bool val = std::get<bool>(value);
            file.write(reinterpret_cast<const char*>(&val), sizeof(val));
            break;
        }
        case Column::STRING:
        case Column::CHAR: {
            std::string val = std::get<std::string>(value);
            // Truncate or pad the string to the specified length
            val.resize(column.length, '\0');
            int len = val.length();
            file.write(reinterpret_cast<const char*>(&len), sizeof(len));
            file.write(val.c_str(), len);
            break;
        }
    }
}

FieldValue DatabaseManager::deserializeField(std::ifstream& file, const Column& column) {
    switch (column.type) {
        case Column::INT: {
            int val;
            file.read(reinterpret_cast<char*>(&val), sizeof(val));
            return val;
        }
        case Column::FLOAT: {
            float val;
            file.read(reinterpret_cast<char*>(&val), sizeof(val));
            return val;
        }
        case Column::BOOL: {
            bool val;
            file.read(reinterpret_cast<char*>(&val), sizeof(val));
            return val;
        }
        case Column::STRING:
        case Column::CHAR: {
            int len;
            file.read(reinterpret_cast<char*>(&len), sizeof(len));
            std::string val(len, '\0');
            file.read(&val[0], len);
            return val;
        }
        default:
            throw std::runtime_error("Unknown column type");
    }
}

void DatabaseManager::createIndex(const TableSchema& schema) {
    // Find the primary key column to index
    std::string primary_key_column;
    for (const auto& column : schema.columns) {
        if (column.is_primary_key) {
            primary_key_column = column.name;
            break;
        }
    }
    
    if (primary_key_column.empty()) {
        std::cerr << "Table '" << schema.name << "' has no primary key, no index created." << std::endl;
        return;
    }
    
    // Create or open the index file
    if (indexes.find(schema.name) == indexes.end()) {
        indexes[schema.name] = new BPlusTree(schema.index_file_path);
    }
    
    std::cout << "Created index for table '" << schema.name << "' on primary key '" << primary_key_column << "'." << std::endl;
}

void DatabaseManager::loadIndexes() {
    for (const auto& table : catalog.tables) {
        if (std::filesystem::exists(table.index_file_path)) {
            indexes[table.name] = new BPlusTree(table.index_file_path);
            std::cout << "Loaded index for table '" << table.name << "'." << std::endl;
        }
    }
}