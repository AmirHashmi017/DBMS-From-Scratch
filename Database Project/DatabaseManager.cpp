#include "database_manager.h"

// Get the executable path helper function
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
    // Get executable directory and create data directory
    std::string exePath = getExecutablePath();
    std::filesystem::path dataDir = std::filesystem::path(exePath) / "data";
    std::filesystem::create_directories(dataDir);

    // Set absolute path for catalog
    this->catalog_path = (dataDir / std::filesystem::path(catalog_path_rel).filename()).string();

    std::cout << "Using catalog path: " << this->catalog_path << std::endl;

    // Load the catalog if it exists
    catalog.load(this->catalog_path);

    // Load existing indexes
    loadIndexes();
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

bool DatabaseManager::createTable(
    const std::string& table_name,
    const std::vector<std::tuple<std::string, std::string, int>>& columns,
    const std::string& primary_key,
    const std::map<std::string, std::pair<std::string, std::string>>& foreign_keys) {

    // Check if table already exists
    for (const auto& table : catalog.tables) {
        if (table.name == table_name) {
            std::cerr << "Table '" << table_name << "' already exists" << std::endl;
            return false;
        }
    }

    // Validate that primary key column exists in the columns list
    if (!primary_key.empty()) {
        bool found = false;
        for (const auto& [col_name, col_type, col_length] : columns) {
            if (col_name == primary_key) {
                found = true;
                break;
            }
        }
        if (!found) {
            std::cerr << "Primary key column '" << primary_key << "' not found in column list" << std::endl;
            return false;
        }
    }

    // Validate foreign key references
    for (const auto& [col_name, ref_pair] : foreign_keys) {
        const auto& [ref_table, ref_column] = ref_pair;

        // Check if the column exists in our columns list
        bool col_found = false;
        for (const auto& [name, type, length] : columns) {
            if (name == col_name) {
                col_found = true;
                break;
            }
        }
        if (!col_found) {
            std::cerr << "Foreign key column '" << col_name << "' not found in column list" << std::endl;
            return false;
        }

        // Check if referenced table exists
        bool ref_table_found = false;
        bool ref_column_found = false;

        for (const auto& table : catalog.tables) {
            if (table.name == ref_table) {
                ref_table_found = true;

                // Check if referenced column exists in referenced table
                for (const auto& col : table.columns) {
                    if (col.name == ref_column) {
                        ref_column_found = true;
                        break;
                    }
                }
                break;
            }
        }

        if (!ref_table_found) {
            std::cerr << "Referenced table '" << ref_table << "' does not exist" << std::endl;
            return false;
        }

        if (!ref_column_found) {
            std::cerr << "Referenced column '" << ref_column << "' not found in table '" << ref_table << "'" << std::endl;
            return false;
        }
    }

    // Create new table schema
    TableSchema table;
    table.name = table_name;

    // Add columns
    for (const auto& [col_name, col_type, col_length] : columns) {
        Column column;
        column.name = col_name;
        column.type = stringToColumnType(col_type);
        column.length = col_length;
        column.is_primary_key = (col_name == primary_key);
        column.is_foreign_key = foreign_keys.find(col_name) != foreign_keys.end();

        // Set references if it's a foreign key
        if (column.is_foreign_key) {
            const auto& [ref_table, ref_column] = foreign_keys.at(col_name);
            column.references_table = ref_table;
            column.references_column = ref_column;
        }

        table.columns.push_back(column);
    }

    // Create data and index file paths using the same base directory as catalog
    std::filesystem::path baseDir = std::filesystem::path(catalog_path).parent_path();
    table.data_file_path = (baseDir / (table_name + ".dat")).string();
    table.index_file_path = (baseDir / (table_name + ".idx")).string();

    std::cout << "Creating table files at: " << baseDir << std::endl;
    std::cout << "  Data file: " << table.data_file_path << std::endl;
    std::cout << "  Index file: " << table.index_file_path << std::endl;

    // Add table to catalog
    catalog.tables.push_back(table);

    // Save catalog
    catalog.save(catalog_path);

    // Create index for primary key
    if (!primary_key.empty()) {
        createIndex(table);
    }

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
        return false;
    }

    // Get the primary key value and column
    int primary_key_value = 0;
    std::string primary_key_column;
    bool has_primary_key = false;

    for (const auto& column : schema.columns) {
        if (column.is_primary_key) {
            primary_key_column = column.name;
            has_primary_key = true;

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

    // Verify there are no duplicate primary keys
    if (has_primary_key) {
        auto existing_records = searchRecords(table_name, primary_key_column, primary_key_value);
        if (!existing_records.empty()) {
            std::cerr << "Error: Record with primary key " << primary_key_value
                << " already exists. Primary keys must be unique." << std::endl;
            return false;
        }
    }

    // Validate field lengths for string and char fields
    for (const auto& column : schema.columns) {
        if (record.find(column.name) != record.end()) {
            if (column.type == Column::STRING || column.type == Column::CHAR) {
                if (std::holds_alternative<std::string>(record.at(column.name))) {
                    const std::string& value = std::get<std::string>(record.at(column.name));
                    if (value.length() > column.length) {
                        std::cerr << "Error: Value for column '" << column.name
                            << "' exceeds maximum length of " << column.length << std::endl;
                        return false;
                    }
                }
            }
        }
    }

    // Validate foreign key constraints
    for (const auto& column : schema.columns) {
        if (column.is_foreign_key && record.find(column.name) != record.end()) {
            const FieldValue& foreign_key_value = record.at(column.name);
            auto referenced_records = searchRecords(
                column.references_table,
                column.references_column,
                foreign_key_value
            );

            if (referenced_records.empty()) {
                std::cerr << "Error: Foreign key constraint violation. No matching record found in "
                    << column.references_table << " for " << column.name << " = ";
                std::visit([](auto&& arg) { std::cerr << arg; }, foreign_key_value);
                std::cerr << std::endl;
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

    // Open data file for appending
    std::ofstream data_file(schema.data_file_path, std::ios::binary | std::ios::app);
    if (!data_file) {
        std::cerr << "Failed to open data file: " << schema.data_file_path << std::endl;
        return false;
    }

    // Get current position in file (will be the record offset)
    int offset = data_file.tellp();

    // Save the record
    saveRecord(data_file, record, schema, offset);

    // Index the record
    if (has_primary_key) {
        indexes[table_name]->insert(primary_key_value, offset);
    }

    return true;
}

std::vector<Record> DatabaseManager::searchRecords(const std::string& table_name, const std::string& key_column, const FieldValue& key_value) {
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

    // Open data file
    std::ifstream data_file(schema.data_file_path, std::ios::binary);
    if (!data_file) {
        std::cerr << "Failed to open data file: " << schema.data_file_path << std::endl;
        return results;
    }

    // If no search condition provided, return all records
    if (key_column.empty()) {
        data_file.seekg(0, std::ios::end);
        size_t file_size = data_file.tellg();
        data_file.seekg(0);

        while (data_file.tellg() < file_size && data_file.good()) {
            results.push_back(loadRecord(data_file, schema));
        }
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

    // If searching by primary key and index exists, use it
    if (is_primary_key && indexes.find(table_name) != indexes.end() && std::holds_alternative<int>(key_value)) {
        int key_int = std::get<int>(key_value);
        auto offsets = indexes[table_name]->search(key_int);

        for (int offset : offsets) {
            if (offset >= 0) { // Ensure valid offset
                data_file.clear(); // Clear any error flags
                data_file.seekg(offset);
                if (data_file.good()) {
                    Record record = loadRecord(data_file, schema);
                    // Double-check the record actually has the key we're looking for
                    if (record.find(key_column) != record.end() && record[key_column] == key_value) {
                        results.push_back(record);
                    }
                    else {
                        std::cerr << "Warning: Index returned a mismatched record for key " << key_int << std::endl;
                    }
                }
            }
        }
    }
    else {
        // Sequential scan for non-primary key or if index doesn't exist
        data_file.seekg(0, std::ios::end);
        size_t file_size = data_file.tellg();
        data_file.seekg(0);

        while (data_file.tellg() < file_size && data_file.good()) {
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
        std::string val = std::get<std::string>(value);
        // Truncate or pad string to fit column length
        val.resize(column.length, '\0');
        int len = val.size();
        file.write(reinterpret_cast<const char*>(&len), sizeof(len));
        file.write(val.c_str(), len);
        break;
    }
    case Column::CHAR: {
        std::string val = std::get<std::string>(value);
        // Truncate or pad string to fit column length
        val.resize(column.length, '\0');
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
        return val;
    }
    case Column::CHAR: {
        std::string val(column.length, '\0');
        file.read(&val[0], column.length);
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