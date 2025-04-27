#include <filesystem>
#include <iostream>
#include <system_error>
#include <set>
#include "database_manager.h"


// Get the executable path helper function

namespace fs = std::filesystem;
std::string getExecutablePath() {
#ifdef _WIN32
    char buffer[MAX_PATH];
    GetModuleFileNameA(NULL, buffer, MAX_PATH);
    std::string::size_type pos = std::string(buffer).find_last_of("\\/");
    return std::string(buffer).substr(0, pos);
#else
    char buffer[PATH_MAX];
    ssize_t count = readlink("/proc/self/exe", buffer, PATH_MAX);
    return std::string(buffer, (count > 0) ? count : 0).substr(0,
        std::string(buffer).find_last_of("/"));
#endif
}

DatabaseManager::DatabaseManager(const std::string& catalog_path_rel) {
    try {
        // Initialize current database as empty
        this->current_database.clear();

        // Set up the database directory
        std::filesystem::path dataDir = "db_data";
        if (!std::filesystem::exists(dataDir)) {
            std::filesystem::create_directories(dataDir);
        }

        // Set absolute path for catalog
        this->catalog_path = (dataDir / std::filesystem::path(catalog_path_rel).filename()).string();
        std::cout << "Using catalog path: " << this->catalog_path << std::endl;

        // Load the catalog if it exists
        if (std::filesystem::exists(this->catalog_path)) {
            catalog.load(this->catalog_path);
        }

        // Load existing indexes
        loadIndexes();
    }
    catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Filesystem error in constructor: " << e.what() << std::endl;
        std::cerr << "Path: " << e.path1() << std::endl;
        throw;
    }
    catch (const std::exception& e) {
        std::cerr << "Error in constructor: " << e.what() << std::endl;
        throw;
    }
}


DatabaseManager::~DatabaseManager() {
    // Save catalog
    catalog.save(catalog_path);

    // Clean up indexes
    for (auto& [name, index] : indexes) {
        delete index;
    }
    indexes.clear();
}

void ensureWritePermissions(const fs::path& path) {
    try {
        fs::permissions(path, fs::perms::owner_write | fs::perms::group_write | fs::perms::others_write,
            fs::perm_options::add);
    }
    catch (const fs::filesystem_error& e) {
        std::cerr << "Failed to set write permissions for path: " << path << "\nError: " << e.what() << std::endl;
        throw;
    }
}

void createDirectoriesWithPermissions(const fs::path& path) {
    try {
        if (!fs::exists(path)) {
            fs::create_directories(path);
        }
        ensureWritePermissions(path);
    }
    catch (const fs::filesystem_error& e) {
        std::cerr << "Error creating directories or setting permissions: " << e.what() << std::endl;
        throw;
    }
}


bool DatabaseManager::createTable(
    const std::string& table_name,
    const std::vector<std::tuple<std::string, std::string, int>>& columns,
    const std::string& primary_key,
    const std::map<std::string, std::pair<std::string, std::string>>& foreign_keys) {

    // Check if database is selected
    if (current_database.empty()) {
        std::cerr << "Error: No database selected. Use 'USE DATABASE' first." << std::endl;
        return false;
    }

    // Validate table name
    if (table_name.empty()) {
        std::cerr << "Error: Table name cannot be empty" << std::endl;
        return false;
    }

    // Check for invalid characters in table name
    if (table_name.find_first_of("\\/:*?\"<>|") != std::string::npos) {
        std::cerr << "Error: Table name contains invalid characters" << std::endl;
        return false;
    }

    // Check if table already exists
    for (const auto& table : catalog.tables) {
        if (table.name == table_name) {
            std::cerr << "Error: Table '" << table_name << "' already exists" << std::endl;
            return false;
        }
    }

    // Validate columns
    if (columns.empty()) {
        std::cerr << "Error: Table must have at least one column" << std::endl;
        return false;
    }

    // Check for duplicate column names
    std::set<std::string> column_names;
    for (const auto& [col_name, col_type, col_length] : columns) {
        if (col_name.empty()) {
            std::cerr << "Error: Column name cannot be empty" << std::endl;
            return false;
        }
        if (!column_names.insert(col_name).second) {
            std::cerr << "Error: Duplicate column name '" << col_name << "'" << std::endl;
            return false;
        }
    }

    // Validate primary key
    if (primary_key.empty()) {
        std::cerr << "Error: Primary key cannot be empty" << std::endl;
        return false;
    }
    if (column_names.find(primary_key) == column_names.end()) {
        std::cerr << "Error: Primary key column '" << primary_key << "' does not exist" << std::endl;
        return false;
    }

    // Validate foreign keys
    for (const auto& [fk_col, ref] : foreign_keys) {
        if (column_names.find(fk_col) == column_names.end()) {
            std::cerr << "Error: Foreign key column '" << fk_col << "' does not exist" << std::endl;
            return false;
        }
        const auto& [ref_table, ref_column] = ref;
        if (ref_table.empty() || ref_column.empty()) {
            std::cerr << "Error: Invalid reference for foreign key '" << fk_col << "'" << std::endl;
            return false;
        }
    }

    // Create new table schema
    TableSchema table;
    table.name = table_name;

    // Add columns with validation
    for (const auto& [col_name, col_type, col_length] : columns) {
        Column column;
        column.name = col_name;
        column.type = stringToColumnType(col_type);
        
        // Validate column type
        if (column.type == Column::UNKNOWN) {
            std::cerr << "Error: Invalid column type '" << col_type << "' for column '" << col_name << "'" << std::endl;
            return false;
        }

        // Validate length for string/char types
        if ((column.type == Column::STRING || column.type == Column::CHAR) && col_length <= 0) {
            std::cerr << "Error: Invalid length for column '" << col_name << "'" << std::endl;
            return false;
        }
        column.length = col_length;
        
        column.is_primary_key = (col_name == primary_key);
        column.is_foreign_key = foreign_keys.find(col_name) != foreign_keys.end();

        if (column.is_foreign_key) {
            const auto& [ref_table, ref_column] = foreign_keys.at(col_name);
            column.references_table = ref_table;
            column.references_column = ref_column;
        }

        table.columns.push_back(column);
    }

    // Create data and index file paths
    std::filesystem::path baseDir = std::filesystem::path(catalog_path).parent_path();
    table.data_file_path = (baseDir / (table_name + ".dat")).string();
    table.index_file_path = (baseDir / (table_name + ".idx")).string();

    // Add table to catalog
    catalog.tables.push_back(table);

    // Save catalog
    catalog.save(catalog_path);

    // Create index for primary key
    createIndex(table);

    return true;
}

void DatabaseManager::createIndex(const TableSchema& schema) {
    // Find primary key column
    Column primary_key_column;
    bool found = false;

    for (const auto& column : schema.columns) {
        if (column.is_primary_key) {
            primary_key_column = column;
            found = true;
            break;
        }
    }

    if (!found) {
        std::cerr << "No primary key found for table '" << schema.name << "'" << std::endl;
        return;
    }

    // Make sure the directory exists
    std::filesystem::path indexPath(schema.index_file_path);
    std::filesystem::create_directories(indexPath.parent_path());

    // Create B+ tree index
    auto* index = new BPlusTree(schema.index_file_path);
    indexes[schema.name] = index;
}

void DatabaseManager::loadIndexes() {
    for (const auto& table : catalog.tables) {
        std::cout << "Loading index for table " << table.name << " from " << table.index_file_path << std::endl;

        // Check if the index file exists
        if (std::filesystem::exists(table.index_file_path)) {
            auto* index = new BPlusTree(table.index_file_path);
            indexes[table.name] = index;
        }
        else {
            std::cout << "Index file doesn't exist yet. Will be created on first insert." << std::endl;
            createIndex(table);
        }
    }
}

bool DatabaseManager::insertRecord(const std::string& table_name, const Record& record) {
    // Validate table name
    if (table_name.empty()) {
        std::cerr << "Error: Table name cannot be empty" << std::endl;
        return false;
    }

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
        std::cerr << "Error: Table '" << table_name << "' not found" << std::endl;
        return false;
    }

    // Validate record against schema
    for (const auto& column : schema.columns) {
        if (column.is_primary_key || column.is_foreign_key) {
            if (record.find(column.name) == record.end()) {
                std::cerr << "Error: Required column '" << column.name << "' is missing from record" << std::endl;
                return false;
            }
        }
    }

    // Validate data types
    for (const auto& [col_name, value] : record) {
        bool column_found = false;
        for (const auto& column : schema.columns) {
            if (column.name == col_name) {
                column_found = true;
                // Check if value type matches column type
                if ((column.type == Column::INT && !std::holds_alternative<int>(value)) ||
                    (column.type == Column::FLOAT && !std::holds_alternative<float>(value)) ||
                    (column.type == Column::STRING && !std::holds_alternative<std::string>(value)) ||
                    (column.type == Column::CHAR && !std::holds_alternative<std::string>(value)) ||
                    (column.type == Column::BOOL && !std::holds_alternative<bool>(value))) {
                    std::cerr << "Error: Invalid data type for column '" << col_name << "'" << std::endl;
                    return false;
                }
                
                // Check string length for STRING and CHAR types
                if ((column.type == Column::STRING || column.type == Column::CHAR) && 
                    std::holds_alternative<std::string>(value)) {
                    const std::string& str_value = std::get<std::string>(value);
                    if (str_value.length() > static_cast<size_t>(column.length)) {
                        std::cerr << "Error: String length exceeds maximum length for column '" << col_name << "'" << std::endl;
                        return false;
                    }
                }
                break;
            }
        }
        if (!column_found) {
            std::cerr << "Error: Column '" << col_name << "' does not exist in table '" << table_name << "'" << std::endl;
            return false;
        }
    }

    // Get the primary key value
    int primary_key_value = 0;
    std::string primary_key_column;
    for (const auto& column : schema.columns) {
        if (column.is_primary_key) {
            primary_key_column = column.name;
            if (record.find(primary_key_column) == record.end()) {
                std::cerr << "Record is missing primary key '" << primary_key_column << "'" << std::endl;
                return false;
            }
            if (column.type == Column::INT) {
                primary_key_value = std::get<int>(record.at(primary_key_column));
            }
            else {
                std::cerr << "Primary key must be an integer" << std::endl;
                return false;
            }
            break;
        }
    }

    // Check if primary key already exists
    if (indexes.find(table_name) != indexes.end()) {
        auto existing_offsets = indexes[table_name]->search(primary_key_value);
        if (!existing_offsets.empty()) {
            std::cerr << "Error: Primary key value " << primary_key_value << " already exists in table '" << table_name << "'" << std::endl;
            return false;
        }
    }

    // Check foreign key constraints
    for (const auto& column : schema.columns) {
        if (column.is_foreign_key) {
            // Get the foreign key value
            if (record.find(column.name) == record.end()) {
                std::cerr << "Record is missing foreign key '" << column.name << "'" << std::endl;
                return false;
            }

            // Find the referenced table
            TableSchema ref_schema;
            bool ref_found = false;
            for (const auto& table : catalog.tables) {
                if (table.name == column.references_table) {
                    ref_schema = table;
                    ref_found = true;
                    break;
                }
            }
            if (!ref_found) {
                std::cerr << "Referenced table '" << column.references_table << "' not found for foreign key '" << column.name << "'" << std::endl;
                return false;
            }

            // Get the foreign key value
            int foreign_key_value = 0;
            if (column.type == Column::INT) {
                foreign_key_value = std::get<int>(record.at(column.name));
            }
            else {
                std::cerr << "Foreign key must be an integer" << std::endl;
                return false;
            }

            // Check if the value exists in the referenced table
            if (indexes.find(column.references_table) != indexes.end()) {
                auto ref_offsets = indexes[column.references_table]->search(foreign_key_value);
                if (ref_offsets.empty()) {
                    std::cerr << "Foreign key value " << foreign_key_value << " not found in referenced table '" << column.references_table << "'" << std::endl;
                    return false;
                }
            }
            else {
                std::cerr << "No index found for referenced table '" << column.references_table << "'" << std::endl;
                return false;
            }
        }
    }

    // Ensure the index exists
    if (indexes.find(table_name) == indexes.end()) {
        createIndex(schema);
    }

    // Make sure directories exist
    std::filesystem::path dataFilePath(schema.data_file_path);
    std::filesystem::create_directories(dataFilePath.parent_path());

    // Open data file in appropriate mode
    std::ofstream data_file(schema.data_file_path, std::ios::binary | std::ios::app);
    if (!data_file) {
        std::cerr << "Failed to open data file: " << schema.data_file_path << std::endl;
        return false;
    }

    // Get current position in file (will be the record offset)
    int offset = data_file.tellp();

    // Save the record using the fixed serialization function
    saveRecord(data_file, record, schema, offset);
    data_file.close();

    // Index the record
    if (indexes[table_name]) {
        indexes[table_name]->insert(primary_key_value, offset);
    }
    else {
        std::cerr << "Index creation failed for table " << table_name << std::endl;
        return false;
    }

    return true;
}

// Best combined implementation of serializeField
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
    case Column::STRING: {
        std::string val;
        if (std::holds_alternative<std::string>(value)) {
            val = std::get<std::string>(value);
        }
        // No truncation - length check is done in insertRecord
        int len = val.size();
        file.write(reinterpret_cast<const char*>(&len), sizeof(len));
        file.write(val.c_str(), len);
        break;
    }
    case Column::CHAR: {
        std::string val;
        if (std::holds_alternative<std::string>(value)) {
            val = std::get<std::string>(value);
        }
        // No truncation - length check is done in insertRecord
        file.write(val.c_str(), column.length);
        break;
    }
    case Column::BOOL: {
        bool val = std::get<bool>(value);
        file.write(reinterpret_cast<const char*>(&val), sizeof(val));
        break;
    }
    }
}

// Best combined implementation of deserializeField
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
    case Column::STRING: {
        int len;
        file.read(reinterpret_cast<char*>(&len), sizeof(len));
        std::string val(len, '\0');
        file.read(&val[0], len);
        // Trim null characters
        size_t nullPos = val.find('\0');
        if (nullPos != std::string::npos) {
            val = val.substr(0, nullPos);
        }
        return val;
    }
    case Column::CHAR: {
        std::string val(column.length, '\0');
        file.read(&val[0], column.length);
        // Trim null characters
        size_t nullPos = val.find('\0');
        if (nullPos != std::string::npos) {
            val = val.substr(0, nullPos);
        }
        return val;
    }
    case Column::BOOL: {
        bool val;
        file.read(reinterpret_cast<char*>(&val), sizeof(val));
        return val;
    }
    default:
        return 0; // Default to int 0
    }
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
        std::cerr << "Table '" << table_name << "' not found" << std::endl;
        return results;
    }

    // Check if the key column is the primary key
    bool is_primary_key = false;
    for (const auto& column : schema.columns) {
        if (column.name == key_column && column.is_primary_key) {
            is_primary_key = true;
            break;
        }
    }

    // Check if data file exists
    if (!std::filesystem::exists(schema.data_file_path)) {
        std::cerr << "Data file not found: " << schema.data_file_path << std::endl;
        return results;
    }

    // Open data file for reading
    std::ifstream data_file(schema.data_file_path, std::ios::binary);
    if (!data_file) {
        std::cerr << "Failed to open data file: " << schema.data_file_path << std::endl;
        return results;
    }

    // If searching by primary key and index exists, use it
    if (is_primary_key && indexes.find(table_name) != indexes.end()) {
        int key_int = std::get<int>(key_value);
        auto offsets = indexes[table_name]->search(key_int);

        for (int offset : offsets) {
            data_file.seekg(offset);
            results.push_back(loadRecord(data_file, schema));
        }
    }
    else {
        // Sequential scan
        data_file.seekg(0, std::ios::end);
        size_t file_size = data_file.tellg();
        data_file.seekg(0);

        while (data_file.tellg() < file_size) {
            size_t record_start = data_file.tellg();
            Record record = loadRecord(data_file, schema);

            // Check if this record matches the search criteria
            if (record.find(key_column) != record.end() && record[key_column] == key_value) {
                results.push_back(record);
            }
        }
    }

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
    return TableSchema(); // Empty schema if not found
}

Column::Type DatabaseManager::stringToColumnType(const std::string& type_str) {
    if (type_str == "int") return Column::INT;
    if (type_str == "float") return Column::FLOAT;
    if (type_str == "string") return Column::STRING;
    if (type_str == "char") return Column::CHAR;
    if (type_str == "bool") return Column::BOOL;

    // Default to string
    std::cerr << "Unknown type '" << type_str << "', defaulting to STRING" << std::endl;
    return Column::STRING;
}

void DatabaseManager::saveRecord(std::ofstream& file, const Record& record, const TableSchema& schema, int& offset) {
    // Store current position as record start
    offset = file.tellp();

    // Write each field according to schema
    for (const auto& column : schema.columns) {
        if (record.find(column.name) != record.end()) {
            serializeField(file, record.at(column.name), column);
        }
        else {
            // Write default value if field is missing
            FieldValue default_value;
            switch (column.type) {
            case Column::INT: default_value = 0; break;
            case Column::FLOAT: default_value = 0.0f; break;
            case Column::STRING:
            case Column::CHAR: default_value = std::string(""); break;
            case Column::BOOL: default_value = false; break;
            }
            serializeField(file, default_value, column);
        }
    }
}

Record DatabaseManager::loadRecord(std::ifstream& file, const TableSchema& schema) {
    Record record;

    for (const auto& column : schema.columns) {
        record[column.name] = deserializeField(file, column);
    }

    return record;
}

int DatabaseManager::getFieldSize(const Column& column) const {
    switch (column.type) {
    case Column::INT: return sizeof(int);
    case Column::FLOAT: return sizeof(float);
    case Column::STRING: return sizeof(int) + column.length; // Length + max chars
    case Column::CHAR: return column.length;
    case Column::BOOL: return sizeof(bool);
    default: return 0;
    }
}


std::vector<Record> DatabaseManager::getAllRecords(const std::string& table_name) {
    std::vector<Record> results;

    // Find the table schema
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
        std::cerr << "Table '" << table_name << "' not found" << std::endl;
        return results;
    }

    // Check if data file exists
    if (!std::filesystem::exists(schema.data_file_path)) {
        std::cerr << "Data file not found: " << schema.data_file_path << std::endl;
        return results;
    }

    // Open data file for reading
    std::ifstream data_file(schema.data_file_path, std::ios::binary);
    if (!data_file) {
        std::cerr << "Failed to open data file: " << schema.data_file_path << std::endl;
        return results;
    }

    // Read all records from the data file
    data_file.seekg(0, std::ios::end);
    size_t file_size = data_file.tellg();
    data_file.seekg(0);

    while (data_file.tellg() < file_size && data_file.good()) {
        results.push_back(loadRecord(data_file, schema));
    }

    return results;
}
// Add these implementations at the end of DatabaseManager.cpp

bool evaluateSingleCondition(const Record& record, const std::string& column, const std::string& op, const FieldValue& value) {
    if (record.find(column) == record.end()) {
        return false;
    }

    const FieldValue& record_value = record.at(column);

    // Handle different comparison operators
    if (op == "=") {
        return record_value == value;
    }
    else if (op == "!=") {
        return record_value != value;
    }
    else if (op == ">") {
        if (std::holds_alternative<int>(record_value) && std::holds_alternative<int>(value)) {
            return std::get<int>(record_value) > std::get<int>(value);
        }
        else if (std::holds_alternative<float>(record_value) && std::holds_alternative<float>(value)) {
            return std::get<float>(record_value) > std::get<float>(value);
        }
        // Could add string comparison for lexicographic ordering if needed
    }
    else if (op == "<") {
        if (std::holds_alternative<int>(record_value) && std::holds_alternative<int>(value)) {
            return std::get<int>(record_value) < std::get<int>(value);
        }
        else if (std::holds_alternative<float>(record_value) && std::holds_alternative<float>(value)) {
            return std::get<float>(record_value) < std::get<float>(value);
        }
    }
    else if (op == ">=") {
        if (std::holds_alternative<int>(record_value) && std::holds_alternative<int>(value)) {
            return std::get<int>(record_value) >= std::get<int>(value);
        }
        else if (std::holds_alternative<float>(record_value) && std::holds_alternative<float>(value)) {
            return std::get<float>(record_value) >= std::get<float>(value);
        }
    }
    else if (op == "<=") {
        if (std::holds_alternative<int>(record_value) && std::holds_alternative<int>(value)) {
            return std::get<int>(record_value) <= std::get<int>(value);
        }
        else if (std::holds_alternative<float>(record_value) && std::holds_alternative<float>(value)) {
            return std::get<float>(record_value) <= std::get<float>(value);
        }
    }
    else if (op == "LIKE" && std::holds_alternative<std::string>(record_value) && std::holds_alternative<std::string>(value)) {
        // Simple LIKE implementation - could be extended for patterns
        return std::get<std::string>(record_value).find(std::get<std::string>(value)) != std::string::npos;
    }

    return false;
}

bool DatabaseManager::evaluateCondition(
    const Record& record,
    const std::vector<std::tuple<std::string, std::string, FieldValue>>& conditions,
    const std::vector<std::string>& operators) {

    if (conditions.empty()) {
        return true; // No conditions means all records match
    }

    // Evaluate the first condition
    bool result = evaluateSingleCondition(
        record,
        std::get<0>(conditions[0]),
        std::get<1>(conditions[0]),
        std::get<2>(conditions[0])
    );

    // Apply additional conditions with operators
    for (size_t i = 1; i < conditions.size(); i++) {
        bool next_result = evaluateSingleCondition(
            record,
            std::get<0>(conditions[i]),
            std::get<1>(conditions[i]),
            std::get<2>(conditions[i])
        );

        // Apply the operator between the previous result and this condition
        std::string op = operators[i - 1];
        if (op == "AND") {
            result = result && next_result;
        }
        else if (op == "OR") {
            result = result || next_result;
        }
        else if (op == "NOT") {
            result = result && !next_result;
        }
    }

    return result;
}

std::vector<Record> DatabaseManager::searchRecordsWithFilter(
    const std::string& table_name,
    const std::vector<std::tuple<std::string, std::string, FieldValue>>& conditions,
    const std::vector<std::string>& operators) {

    std::vector<Record> results;

    // Find the table schema
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
        std::cerr << "Table '" << table_name << "' not found" << std::endl;
        return results;
    }

    // Check if data file exists
    if (!std::filesystem::exists(schema.data_file_path)) {
        std::cerr << "Data file not found: " << schema.data_file_path << std::endl;
        return results;
    }

    // Open data file for reading
    std::ifstream data_file(schema.data_file_path, std::ios::binary);
    if (!data_file) {
        std::cerr << "Failed to open data file: " << schema.data_file_path << std::endl;
        return results;
    }

    // Read all records and apply filter
    data_file.seekg(0, std::ios::end);
    size_t file_size = data_file.tellg();
    data_file.seekg(0);

    while (data_file.tellg() < file_size && data_file.good()) {
        Record record = loadRecord(data_file, schema);

        // Apply filter conditions
        if (evaluateCondition(record, conditions, operators)) {
            results.push_back(record);
        }
    }

    return results;
}

bool DatabaseManager::updateRecordsWithFilter(
    const std::string& table_name,
    const std::map<std::string, FieldValue>& update_values,
    const std::vector<std::tuple<std::string, std::string, FieldValue>>& conditions,
    const std::vector<std::string>& operators) {

    // Find the table schema
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
        std::cerr << "Table '" << table_name << "' not found" << std::endl;
        return false;
    }

    // Check if data file exists
    if (!std::filesystem::exists(schema.data_file_path)) {
        std::cerr << "Data file not found: " << schema.data_file_path << std::endl;
        return false;
    }

    // First, read all records and identify which ones need to be updated
    std::ifstream read_file(schema.data_file_path, std::ios::binary);
    if (!read_file) {
        std::cerr << "Failed to open data file: " << schema.data_file_path << std::endl;
        return false;
    }

    // Create a temporary file for the updated data
    std::string temp_file_path = schema.data_file_path + ".tmp";
    std::ofstream write_file(temp_file_path, std::ios::binary);
    if (!write_file) {
        std::cerr << "Failed to create temporary file" << std::endl;
        return false;
    }

    read_file.seekg(0, std::ios::end);
    size_t file_size = read_file.tellg();
    read_file.seekg(0);

    // Track record offsets for updating the index
    std::map<int, int> updated_offsets; // original_offset -> new_offset
    int records_updated = 0;
    int record_index = 0;

    while (read_file.tellg() < file_size && read_file.good()) {
        int original_offset = read_file.tellg();
        Record record = loadRecord(read_file, schema);

        // Create a new record with updated values if the condition matches
        Record updated_record = record;
        bool should_update = evaluateCondition(record, conditions, operators);

        if (should_update) {
            // Apply updates
            for (const auto& [key, value] : update_values) {
                updated_record[key] = value;
            }
            records_updated++;
        }

        // Write the record (original or updated) to the temporary file
        int new_offset = write_file.tellp();
        saveRecord(write_file, updated_record, schema, new_offset);

        // Store the offset mapping if this record has a primary key
        // and especially if it was updated (for index update)
        if (should_update) {
            // Find primary key
            for (const auto& column : schema.columns) {
                if (column.is_primary_key && column.type == Column::INT) {
                    int pk_value = std::get<int>(updated_record[column.name]);
                    updated_offsets[pk_value] = new_offset;
                    break;
                }
            }
        }

        record_index++;
    }

    read_file.close();
    write_file.close();

    // Replace the original file with the temporary file
    std::filesystem::rename(temp_file_path, schema.data_file_path);

    // Update the index with the new record offsets
    if (!updated_offsets.empty() && indexes.find(table_name) != indexes.end()) {
        BPlusTree* index = indexes[table_name];
        for (const auto& [key, new_offset] : updated_offsets) {
            index->insert(key, new_offset);
        }
    }

    std::cout << "Updated " << records_updated << " records" << std::endl;
    return true;
}

int DatabaseManager::deleteRecordsWithFilter(
    const std::string& table_name,
    const std::vector<std::tuple<std::string, std::string, FieldValue>>& conditions,
    const std::vector<std::string>& operators) {

    // Find the table schema
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
        std::cerr << "Table '" << table_name << "' not found" << std::endl;
        return 0;
    }

    // Check if data file exists
    if (!std::filesystem::exists(schema.data_file_path)) {
        std::cerr << "Data file not found: " << schema.data_file_path << std::endl;
        return 0;
    }

    // First, read all records and identify which ones need to be kept
    std::ifstream read_file(schema.data_file_path, std::ios::binary);
    if (!read_file) {
        std::cerr << "Failed to open data file: " << schema.data_file_path << std::endl;
        return 0;
    }

    // Create a temporary file that will contain only the kept records
    std::string temp_file_path = schema.data_file_path + ".tmp";
    std::ofstream write_file(temp_file_path, std::ios::binary);
    if (!write_file) {
        std::cerr << "Failed to create temporary file" << std::endl;
        return 0;
    }

    read_file.seekg(0, std::ios::end);
    size_t file_size = read_file.tellg();
    read_file.seekg(0);

    // Track deleted record primary keys for updating the index
    std::vector<int> deleted_keys;
    std::map<int, int> kept_records; // primary_key -> new_offset
    int records_deleted = 0;

    while (read_file.tellg() < file_size && read_file.good()) {
        int original_offset = read_file.tellg();
        Record record = loadRecord(read_file, schema);

        // Determine if this record should be deleted
        bool should_delete = evaluateCondition(record, conditions, operators);

        // Find primary key value
        int primary_key_value = -1;
        for (const auto& column : schema.columns) {
            if (column.is_primary_key && column.type == Column::INT) {
                primary_key_value = std::get<int>(record[column.name]);
                break;
            }
        }

        if (should_delete) {
            // Add to the list of deleted keys for index update
            if (primary_key_value != -1) {
                deleted_keys.push_back(primary_key_value);
            }
            records_deleted++;
        }
        else {
            // Keep this record
            int new_offset = write_file.tellp();
            saveRecord(write_file, record, schema, new_offset);

            // Update the offset mapping for the index
            if (primary_key_value != -1) {
                kept_records[primary_key_value] = new_offset;
            }
        }
    }

    read_file.close();
    write_file.close();

    // Replace the original file with the temporary file
    std::filesystem::rename(temp_file_path, schema.data_file_path);

    // Rebuild the index with the kept records
    if (indexes.find(table_name) != indexes.end() && !kept_records.empty()) {
        // It might be simpler to rebuild the index completely
        delete indexes[table_name];

        // Create a new index
        auto* new_index = new BPlusTree(schema.index_file_path);

        // Insert all kept records
        for (const auto& [key, offset] : kept_records) {
            new_index->insert(key, offset);
        }

        indexes[table_name] = new_index;
    }

    std::cout << "Deleted " << records_deleted << " records" << std::endl;
    return records_deleted;
}

bool DatabaseManager::createDatabase(const std::string& db_name) {
    try {
        std::filesystem::path dbDir = getDatabasePath(db_name);
        std::cout << "Creating database at path: " << dbDir << std::endl;

        if (std::filesystem::exists(dbDir)) {
            std::cerr << "Database '" << db_name << "' already exists at: " << dbDir << std::endl;
            return false;
        }

        // Create the database directory
        if (!std::filesystem::create_directories(dbDir)) {
            std::cerr << "Failed to create database directory at: " << dbDir << std::endl;
            return false;
        }

        // Create catalog file
        std::filesystem::path catalogPath = dbDir / "catalog.bin";
        std::ofstream catalog_file(catalogPath, std::ios::binary);
        if (!catalog_file.is_open()) {
            std::cerr << "Failed to create catalog file at: " << catalogPath << std::endl;
            return false;
        }
        catalog_file.close();

        std::cout << "Successfully created database '" << db_name << "'" << std::endl;
        return true;
    }
    catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Filesystem error in createDatabase: " << e.what() << std::endl;
        return false;
    }
    catch (const std::exception& e) {
        std::cerr << "Error creating database: " << e.what() << std::endl;
        return false;
    }
}

std::filesystem::path DatabaseManager::getDatabasePath(const std::string& db_name) {
    try {
        // Use the same db_data directory as in constructor
        std::filesystem::path dataDir = "db_data";
        
        // Create the data directory if it doesn't exist
        if (!std::filesystem::exists(dataDir)) {
            std::filesystem::create_directories(dataDir);
        }
        
        // Return the database path without creating it
        return dataDir / db_name;
    }
    catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Filesystem error in getDatabasePath: " << e.what() << std::endl;
        std::cerr << "Path: " << e.path1() << std::endl;
        throw;
    }
    catch (const std::exception& e) {
        std::cerr << "Error in getDatabasePath: " << e.what() << std::endl;
        throw;
    }
}

bool DatabaseManager::dropDatabase(const std::string& db_name) {
    std::filesystem::path dbDir = getDatabasePath(db_name);

    if (!std::filesystem::exists(dbDir)) {
        std::cerr << "Database '" << db_name << "' does not exist." << std::endl;
        return false;
    }

    // If this is the current database, clear it
    if (current_database == db_name) {
        current_database.clear();
        catalog.tables.clear();  // Clear the catalog
        indexes.clear();         // Clear all indexes
        catalog_path.clear();    // Clear catalog path
    }

    try {
        // Remove all files and subdirectories
        std::filesystem::remove_all(dbDir);
        return true;
    }
    catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Error dropping database: " << e.what() << std::endl;
        return false;
    }
}

bool DatabaseManager::useDatabase(const std::string& db_name) {
    std::filesystem::path dbDir = getDatabasePath(db_name);

    if (!std::filesystem::exists(dbDir)) {
        std::cerr << "Database '" << db_name << "' does not exist." << std::endl;
        return false;
    }

    current_database = db_name;
    catalog_path = (dbDir / "catalog.bin").string();

    // Reload the catalog for the new database
    catalog.load(catalog_path);

    // Reload indexes for the new database
    loadIndexes();

    return true;
}

bool DatabaseManager::dropTable(const std::string& table_name) {
    if (current_database.empty()) {
        std::cerr << "No database selected. Use 'USE DATABASE' first." << std::endl;
        return false;
    }

    // Find the table schema
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
        std::cerr << "Table '" << table_name << "' does not exist." << std::endl;
        return false;
    }

    // Delete the table's data file
    if (std::filesystem::exists(schema.data_file_path)) {
        std::filesystem::remove(schema.data_file_path);
    }

    // Delete the table's index file
    if (std::filesystem::exists(schema.index_file_path)) {
        std::filesystem::remove(schema.index_file_path);
    }

    // Remove and delete the index from memory
    auto it = indexes.find(table_name);
    if (it != indexes.end()) {
        delete it->second;
        indexes.erase(it);
    }

    // Remove table from catalog
    if (!catalog.removeTable(table_name)) {
        std::cerr << "Failed to remove table from catalog." << std::endl;
        return false;
    }

    // Save the updated catalog
    catalog.save(catalog_path);

    return true;
}

std::vector<std::string> DatabaseManager::listDatabases() const {
    std::vector<std::string> databases;
    std::filesystem::path dataDir = "db_data";

    if (!std::filesystem::exists(dataDir)) {
        return databases;
    }

    for (const auto& entry : std::filesystem::directory_iterator(dataDir)) {
        if (entry.is_directory()) {
            databases.push_back(entry.path().filename().string());
        }
    }

    return databases;
}

std::string DatabaseManager::getCurrentDatabase() const {
    return current_database;
}