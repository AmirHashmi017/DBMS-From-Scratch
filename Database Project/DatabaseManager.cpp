#include <filesystem>
#include <iostream>
#include <system_error>
#include <set>
#include "database_manager.h"
#include <thread>
#include <chrono>
#include "query_parser.h"

using namespace std;
namespace fs = filesystem;

// Get the executable path helper function
string getExecutablePath() {
#ifdef _WIN32
    char buffer[MAX_PATH];
    GetModuleFileNameA(NULL, buffer, MAX_PATH);
    string::size_type pos = string(buffer).find_last_of("\\/");
    return string(buffer).substr(0, pos);
#else
    char buffer[PATH_MAX];
    ssize_t count = readlink("/proc/self/exe", buffer, PATH_MAX);
    return string(buffer, (count > 0) ? count : 0).substr(0,
        string(buffer).find_last_of("/"));
#endif
}

DatabaseManager::DatabaseManager(const string& catalog_path_rel) {
    try {
        // Initialize current database as empty
        this->current_database.clear();

        // Set up the database directory
        filesystem::path dataDir = "db_data";
        if (!filesystem::exists(dataDir)) {
            filesystem::create_directories(dataDir);
        }

        // Set absolute path for catalog
        this->catalog_path = (dataDir / filesystem::path(catalog_path_rel).filename()).string();
        cout << "Using catalog path: " << this->catalog_path << endl;

        // Load the catalog if it exists
        if (filesystem::exists(this->catalog_path)) {
            catalog.load(this->catalog_path);
        }

        // Load existing indexes
        loadIndexes();
    }
    catch (const filesystem::filesystem_error& e) {
        cerr << "Filesystem error in constructor: " << e.what() << endl;
        cerr << "Path: " << e.path1() << endl;
        throw;
    }
    catch (const exception& e) {
        cerr << "Error in constructor: " << e.what() << endl;
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
        cerr << "Failed to set write permissions for path: " << path << "\nError: " << e.what() << endl;
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
        cerr << "Error creating directories or setting permissions: " << e.what() << endl;
        throw;
    }
}

bool DatabaseManager::createTable(
    const string& table_name,
    const vector<tuple<string, string, int>>& columns,
    const string& primary_key,
    const map<string, pair<string, string>>& foreign_keys) {

    // Check if database is selected
    if (current_database.empty()) {
        cerr << "Error: No database selected. Use 'USE DATABASE' first." << endl;
        return false;
    }

    // Validate table name
    if (table_name.empty()) {
        cerr << "Error: Table name cannot be empty" << endl;
        return false;
    }

    // Check for invalid characters in table name
    if (table_name.find_first_of("\\/:*?\"<>|") != string::npos) {
        cerr << "Error: Table name contains invalid characters" << endl;
        return false;
    }

    // Check if table already exists
    for (const auto& table : catalog.tables) {
        if (table.name == table_name) {
            cerr << "Error: Table '" << table_name << "' already exists" << endl;
            return false;
        }
    }

    // Validate columns
    if (columns.empty()) {
        cerr << "Error: Table must have at least one column" << endl;
        return false;
    }

    // Check for duplicate column names
    set<string> column_names;
    for (const auto& [col_name, col_type, col_length] : columns) {
        if (col_name.empty()) {
            cerr << "Error: Column name cannot be empty" << endl;
            return false;
        }
        if (!column_names.insert(col_name).second) {
            cerr << "Error: Duplicate column name '" << col_name << "'" << endl;
            return false;
        }
    }

    // Validate primary key
    if (primary_key.empty()) {
        cerr << "Error: Primary key cannot be empty" << endl;
        return false;
    }
    if (column_names.find(primary_key) == column_names.end()) {
        cerr << "Error: Primary key column '" << primary_key << "' does not exist" << endl;
        return false;
    }

    // Validate foreign keys
    for (const auto& [fk_col, ref] : foreign_keys) {
        if (column_names.find(fk_col) == column_names.end()) {
            cerr << "Error: Foreign key column '" << fk_col << "' does not exist" << endl;
            return false;
        }
        const auto& [ref_table, ref_column] = ref;
        if (ref_table.empty() || ref_column.empty()) {
            cerr << "Error: Invalid reference for foreign key '" << fk_col << "'" << endl;
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
            cerr << "Error: Invalid column type '" << col_type << "' for column '" << col_name << "'" << endl;
            return false;
        }

        // Validate length for string/char types
        if ((column.type == Column::STRING || column.type == Column::CHAR) && col_length <= 0) {
            cerr << "Error: Invalid length for column '" << col_name << "'" << endl;
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
    filesystem::path baseDir = filesystem::path(catalog_path).parent_path();
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
        cerr << "No primary key found for table '" << schema.name << "'" << endl;
        return;
    }

    // Make sure the directory exists
    filesystem::path indexPath(schema.index_file_path);
    filesystem::create_directories(indexPath.parent_path());

    // Create B+ tree index
    auto* index = new BPlusTree(schema.index_file_path);
    indexes[schema.name] = index;
}

void DatabaseManager::loadIndexes() {
    for (const auto& table : catalog.tables) {
        cout << "Loading index for table " << table.name << " from " << table.index_file_path << endl;
        if (filesystem::exists(table.index_file_path)) {
            auto* index = new BPlusTree(table.index_file_path);
            indexes[table.name] = index;
        } else {
            cout << "Index file does not exist: " << table.index_file_path << endl;
        }
    }
}

bool DatabaseManager::insertRecord(const string& table_name, const Record& record) {
    // Validate table name
    if (table_name.empty()) {
        cerr << "Error: Table name cannot be empty" << endl;
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
        cerr << "Error: Table '" << table_name << "' not found" << endl;
        return false;
    }

    // Validate record against schema
    for (const auto& column : schema.columns) {
        if (column.is_primary_key || column.is_foreign_key) {
            if (record.find(column.name) == record.end()) {
                cerr << "Error: Required column '" << column.name << "' is missing from record" << endl;
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
                if ((column.type == Column::INT && !holds_alternative<int>(value)) ||
                    (column.type == Column::FLOAT && !holds_alternative<float>(value)) ||
                    (column.type == Column::STRING && !holds_alternative<string>(value)) ||
                    (column.type == Column::CHAR && !holds_alternative<string>(value)) ||
                    (column.type == Column::BOOL && !holds_alternative<bool>(value))) {
                    cerr << "Error: Invalid data type for column '" << col_name << "'" << endl;
                    return false;
                }
                
                // Check string length for STRING and CHAR types
                if ((column.type == Column::STRING || column.type == Column::CHAR) && 
                    holds_alternative<string>(value)) {
                    const string& str_value = get<string>(value);
                    if (str_value.length() > static_cast<size_t>(column.length)) {
                        cerr << "Error: String length exceeds maximum length for column '" << col_name << "'" << endl;
                        return false;
                    }
                }
                break;
            }
        }
        if (!column_found) {
            cerr << "Error: Column '" << col_name << "' does not exist in table '" << table_name << "'" << endl;
            return false;
        }
    }

    // Get the primary key value
    int primary_key_value = 0;
    string primary_key_column;
    for (const auto& column : schema.columns) {
        if (column.is_primary_key) {
            primary_key_column = column.name;
            if (record.find(primary_key_column) == record.end()) {
                cerr << "Record is missing primary key '" << primary_key_column << "'" << endl;
                return false;
            }
            if (column.type == Column::INT) {
                primary_key_value = get<int>(record.at(primary_key_column));
            }
            else {
                cerr << "Primary key must be an integer" << endl;
                return false;
            }
            break;
        }
    }

    // Check if primary key already exists
    if (indexes.find(table_name) != indexes.end()) {
        auto existing_offsets = indexes[table_name]->search(primary_key_value);
        if (!existing_offsets.empty()) {
            cerr << "Error: Primary key value " << primary_key_value << " already exists in table '" << table_name << "'" << endl;
            return false;
        }
    }

    // Check foreign key constraints
    for (const auto& column : schema.columns) {
        if (column.is_foreign_key) {
            // Get the foreign key value
            if (record.find(column.name) == record.end()) {
                cerr << "Record is missing foreign key '" << column.name << "'" << endl;
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
                cerr << "Referenced table '" << column.references_table << "' not found for foreign key '" << column.name << "'" << endl;
                return false;
            }

            // Get the foreign key value
            int foreign_key_value = 0;
            if (column.type == Column::INT) {
                foreign_key_value = get<int>(record.at(column.name));
            }
            else {
                cerr << "Foreign key must be an integer" << endl;
                return false;
            }

            // Check if the value exists in the referenced table
            if (indexes.find(column.references_table) != indexes.end()) {
                auto ref_offsets = indexes[column.references_table]->search(foreign_key_value);
                if (ref_offsets.empty()) {
                    cerr << "Foreign key value " << foreign_key_value << " not found in referenced table '" << column.references_table << "'" << endl;
                    return false;
                }
            }
            else {
                cerr << "No index found for referenced table '" << column.references_table << "'" << endl;
                return false;
            }
        }
    }

    // Ensure the index exists
    if (indexes.find(table_name) == indexes.end()) {
        createIndex(schema);
    }

    // Make sure directories exist
    filesystem::path dataFilePath(schema.data_file_path);
    filesystem::create_directories(dataFilePath.parent_path());

    // Open data file in appropriate mode
    ofstream data_file(schema.data_file_path, ios::binary | ios::app);
    if (!data_file) {
        cerr << "Failed to open data file: " << schema.data_file_path << endl;
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
        cerr << "Index creation failed for table " << table_name << endl;
        return false;
    }

    return true;
}

// Best combined implementation of serializeField
void DatabaseManager::serializeField(ofstream& file, const FieldValue& value, const Column& column) {
    switch (column.type) {
    case Column::INT: {
        int val = get<int>(value);
        file.write(reinterpret_cast<const char*>(&val), sizeof(val));
        break;
    }
    case Column::FLOAT: {
        float val = get<float>(value);
        file.write(reinterpret_cast<const char*>(&val), sizeof(val));
        break;
    }
    case Column::STRING: {
        string val;
        if (holds_alternative<string>(value)) {
            val = get<string>(value);
        }
        // No truncation - length check is done in insertRecord
        int len = val.size();
        file.write(reinterpret_cast<const char*>(&len), sizeof(len));
        file.write(val.c_str(), len);
        break;
    }
    case Column::CHAR: {
        string val;
        if (holds_alternative<string>(value)) {
            val = get<string>(value);
        }
        // No truncation - length check is done in insertRecord
        file.write(val.c_str(), column.length);
        break;
    }
    case Column::BOOL: {
        bool val = get<bool>(value);
        file.write(reinterpret_cast<const char*>(&val), sizeof(val));
        break;
    }
    }
}

// Best combined implementation of deserializeField
FieldValue DatabaseManager::deserializeField(ifstream& file, const Column& column) {
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
        string val(len, '\0');
        file.read(&val[0], len);
        // Trim null characters
        size_t nullPos = val.find('\0');
        if (nullPos != string::npos) {
            val = val.substr(0, nullPos);
        }
        return val;
    }
    case Column::CHAR: {
        string val(column.length, '\0');
        file.read(&val[0], column.length);
        // Trim null characters
        size_t nullPos = val.find('\0');
        if (nullPos != string::npos) {
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

vector<Record> DatabaseManager::searchRecords(const string& table_name, const string& key_column, const FieldValue& key_value) {
    vector<Record> results;

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
        cerr << "Table '" << table_name << "' not found" << endl;
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
    if (!filesystem::exists(schema.data_file_path)) {
        cerr << "Data file not found: " << schema.data_file_path << endl;
        return results;
    }

    // Open data file for reading
    ifstream data_file(schema.data_file_path, ios::binary);
    if (!data_file) {
        cerr << "Failed to open data file: " << schema.data_file_path << endl;
        return results;
    }

    // If searching by primary key and index exists, use it
    if (is_primary_key && indexes.find(table_name) != indexes.end()) {
        int key_int = get<int>(key_value);
        auto offsets = indexes[table_name]->search(key_int);

        for (int offset : offsets) {
            data_file.seekg(offset);
            results.push_back(loadRecord(data_file, schema));
        }
    }
    else {
        // Sequential scan
        data_file.seekg(0, ios::end);
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

vector<string> DatabaseManager::listTables() const {
    vector<string> table_names;
    for (const auto& table : catalog.tables) {
        table_names.push_back(table.name);
    }
    return table_names;
}

TableSchema DatabaseManager::getTableSchema(const string& table_name) const {
    for (const auto& table : catalog.tables) {
        if (table.name == table_name) {
            return table;
        }
    }
    return TableSchema(); // Empty schema if not found
}

Column::Type DatabaseManager::stringToColumnType(const string& type_str) {
    if (type_str == "INT") return Column::INT;
    if (type_str == "FLOAT") return Column::FLOAT;
    if (type_str == "STRING") return Column::STRING;
    if (type_str == "CHAR") return Column::CHAR;
    if (type_str == "BOOL") return Column::BOOL;

    // Default to string
    cerr << "Unknown type '" << type_str << "', defaulting to STRING" << endl;
    return Column::STRING;
}

void DatabaseManager::saveRecord(ofstream& file, const Record& record, const TableSchema& schema, int& offset) {
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
            case Column::CHAR: default_value = string(""); break;
            case Column::BOOL: default_value = false; break;
            }
            serializeField(file, default_value, column);
        }
    }
}

Record DatabaseManager::loadRecord(ifstream& file, const TableSchema& schema) {
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

vector<Record> DatabaseManager::getAllRecords(const string& table_name) {
    vector<Record> results;

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
        cerr << "Table '" << table_name << "' not found" << endl;
        return results;
    }

    // Check if data file exists
    if (!filesystem::exists(schema.data_file_path)) {
        cerr << "Data file not found: " << schema.data_file_path << endl;
        return results;
    }

    // Open data file for reading
    ifstream data_file(schema.data_file_path, ios::binary);
    if (!data_file) {
        cerr << "Failed to open data file: " << schema.data_file_path << endl;
        return results;
    }

    // Read all records from the data file
    data_file.seekg(0, ios::end);
    size_t file_size = data_file.tellg();
    data_file.seekg(0);

    while (data_file.tellg() < file_size && data_file.good()) {
        results.push_back(loadRecord(data_file, schema));
    }

    return results;
}

bool evaluateSingleCondition(const Record& record, const string& column, const string& op, const FieldValue& value) {
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
        if (holds_alternative<int>(record_value) && holds_alternative<int>(value)) {
            return get<int>(record_value) > get<int>(value);
        }
        else if (holds_alternative<float>(record_value) && holds_alternative<float>(value)) {
            return get<float>(record_value) > get<float>(value);
        }
        // Could add string comparison for lexicographic ordering if needed
    }
    else if (op == "<") {
        if (holds_alternative<int>(record_value) && holds_alternative<int>(value)) {
            return get<int>(record_value) < get<int>(value);
        }
        else if (holds_alternative<float>(record_value) && holds_alternative<float>(value)) {
            return get<float>(record_value) < get<float>(value);
        }
    }
    else if (op == ">=") {
        if (holds_alternative<int>(record_value) && holds_alternative<int>(value)) {
            return get<int>(record_value) >= get<int>(value);
        }
        else if (holds_alternative<float>(record_value) && holds_alternative<float>(value)) {
            return get<float>(record_value) >= get<float>(value);
        }
    }
    else if (op == "<=") {
        if (holds_alternative<int>(record_value) && holds_alternative<int>(value)) {
            return get<int>(record_value) <= get<int>(value);
        }
        else if (holds_alternative<float>(record_value) && holds_alternative<float>(value)) {
            return get<float>(record_value) <= get<float>(value);
        }
    }
    else if (op == "LIKE" && holds_alternative<string>(record_value) && holds_alternative<string>(value)) {
        // Simple LIKE implementation - could be extended for patterns
        return get<string>(record_value).find(get<string>(value)) != string::npos;
    }

    return false;
}

bool DatabaseManager::evaluateCondition(
    const Record& record,
    const vector<tuple<string, string, FieldValue>>& conditions,
    const vector<string>& operators) {

    if (conditions.empty()) {
        return true; // No conditions means all records match
    }

    bool result = false;
    size_t op_index = 0;
    bool apply_not = false;

    for (size_t i = 0; i < conditions.size(); ++i) {
        // Check for NOT operator
        if (op_index < operators.size() && operators[op_index] == "NOT") {
            apply_not = true;
            cerr << "Applying operator: NOT" << endl;
            op_index++;
        }

        // Evaluate the current condition
        bool cond_result = evaluateSingleCondition(
            record,
            get<0>(conditions[i]),
            get<1>(conditions[i]),
            get<2>(conditions[i])
        );

        // Apply NOT if specified
        if (apply_not) {
            cond_result = !cond_result;
            apply_not = false;
        }

        cerr << "Evaluated condition " << i + 1 << ": " << (cond_result ? "true" : "false") << endl;

        // Combine with previous result
        if (i == 0) {
            result = cond_result;
        } else {
            if (op_index >= operators.size()) {
                cerr << "Error: Missing operator for condition " << i + 1 << endl;
                return false;
            }
            const string& op = operators[op_index];
            cerr << "Applying operator: " << op << endl;
            if (op == "AND") {
                result = result && cond_result;
            } else if (op == "OR") {
                result = result || cond_result;
            } else {
                cerr << "Error: Invalid operator '" << op << "'" << endl;
                return false;
            }
            op_index++;
        }
    }

    cerr << "Final condition result: " << (result ? "true" : "false") << endl;
    return result;
}

vector<Record> DatabaseManager::searchRecordsWithFilter(
    const string& table_name,
    const vector<tuple<string, string, FieldValue>>& conditions,
    const vector<string>& operators) {

    vector<Record> results;

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
        cerr << "Table '" << table_name << "' not found" << endl;
        return results;
    }

    // Check if data file exists
    if (!filesystem::exists(schema.data_file_path)) {
        cerr << "Data file not found: " << schema.data_file_path << endl;
        return results;
    }

    // Open data file for reading
    ifstream data_file(schema.data_file_path, ios::binary);
    if (!data_file) {
        cerr << "Failed to open data file: " << schema.data_file_path << endl;
        return results;
    }

    // Read all records and apply filter
    data_file.seekg(0, ios::end);
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
    const string& table_name,
    const map<string, FieldValue>& update_values,
    const vector<tuple<string, string, FieldValue>>& conditions,
    const vector<string>& operators) {

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
        cerr << "Table '" << table_name << "' not found" << endl;
        return false;
    }

    // Check if data file exists
    if (!filesystem::exists(schema.data_file_path)) {
        cerr << "Data file not found: " << schema.data_file_path << endl;
        return false;
    }

    // First, read all records and identify which ones need to be updated
    ifstream read_file(schema.data_file_path, ios::binary);
    if (!read_file) {
        cerr << "Failed to open data file: " << schema.data_file_path << endl;
        return false;
    }

    // Create a temporary file for the updated data
    string temp_file_path = schema.data_file_path + ".tmp";
    ofstream write_file(temp_file_path, ios::binary);
    if (!write_file) {
        cerr << "Failed to create temporary file" << endl;
        return false;
    }

    read_file.seekg(0, ios::end);
    size_t file_size = read_file.tellg();
    read_file.seekg(0);

    // Track record offsets for updating the index
    map<int, int> updated_offsets; // original_offset -> new_offset
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
                    int pk_value = get<int>(updated_record[column.name]);
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
    filesystem::rename(temp_file_path, schema.data_file_path);

    // Update the index with the new record offsets
    if (!updated_offsets.empty() && indexes.find(table_name) != indexes.end()) {
        BPlusTree* index = indexes[table_name];
        for (const auto& [key, new_offset] : updated_offsets) {
            index->insert(key, new_offset);
        }
    }

    cerr << "Updated " << records_updated << " records" << endl;
    return true;
}

int DatabaseManager::deleteRecordsWithFilter(
    const string& table_name,
    const vector<tuple<string, string, FieldValue>>& conditions,
    const vector<string>& operators) {

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
        cerr << "Table '" << table_name << "' not found" << endl;
        return 0;
    }

    // Check if data file exists
    if (!filesystem::exists(schema.data_file_path)) {
        cerr << "Data file not found: " << schema.data_file_path << endl;
        return 0;
    }

    // First, read all records and identify which ones need to be kept
    ifstream read_file(schema.data_file_path, ios::binary);
    if (!read_file) {
        cerr << "Failed to open data file: " << schema.data_file_path << endl;
        return 0;
    }

    // Create a temporary file that will contain only the kept records
    string temp_file_path = schema.data_file_path + ".tmp";
    ofstream write_file(temp_file_path, ios::binary);
    if (!write_file) {
        cerr << "Failed to create temporary file" << endl;
        return 0;
    }

    read_file.seekg(0, ios::end);
    size_t file_size = read_file.tellg();
    read_file.seekg(0);

    // Track deleted record primary keys for updating the index
    vector<int> deleted_keys;
    map<int, int> kept_records; // primary_key -> new_offset
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
                primary_key_value = get<int>(record[column.name]);
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
    filesystem::rename(temp_file_path, schema.data_file_path);

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

    cerr << "Deleted " << records_deleted << " records" << endl;
    return records_deleted;
}

vector<Record> DatabaseManager::joinTables(
    const string& table1_name,
    const string& table2_name,
    const Condition& join_condition,
    const vector<tuple<string, string, FieldValue>>& where_conditions,
    const vector<string>& where_operators) {
    
    vector<Record> results;
    
    TableSchema schema1, schema2;
    bool found1 = false, found2 = false;
    for (const auto& table : catalog.tables) {
        if (table.name == table1_name) {
            schema1 = table;
            found1 = true;
        } else if (table.name == table2_name) {
            schema2 = table;
            found2 = true;
        }
    }
    
    if (!found1) {
        cerr << "Table '" << table1_name << "' not found" << endl;
        return results;
    }
    if (!found2) {
        cerr << "Table '" << table2_name << "' not found" << endl;
        return results;
    }
    
    // Get all records from both tables
    vector<Record> records1 = getAllRecords(table1_name);
    vector<Record> records2 = getAllRecords(table2_name);
    
    // Extract join condition columns
    string left_col = join_condition.column; // e.g., users.id
    string right_col = holds_alternative<string>(join_condition.value)
        ? get<string>(join_condition.value) // e.g., orders.user_id
        : "";
    
    // Validate join condition columns
    string left_table = left_col.substr(0, left_col.find('.'));
    string left_col_name = left_col.substr(left_col.find('.') + 1);
    string right_table = right_col.substr(0, right_col.find('.'));
    string right_col_name = right_col.substr(right_col.find('.') + 1);
    
    bool left_valid = false, right_valid = false;
    for (const auto& col : schema1.columns) {
        if (col.name == left_col_name && left_table == table1_name) {
            left_valid = true;
            break;
        }
    }
    for (const auto& col : schema2.columns) {
        if (col.name == right_col_name && right_table == table2_name) {
            right_valid = true;
            break;
        }
    }
    
    if (!left_valid || !right_valid) {
        cerr << "Error: Invalid join condition columns: " << left_col << " = " << right_col << endl;
        return results;
    }
    
    // Perform nested loop join
    for (const auto& rec1 : records1) {
        for (const auto& rec2 : records2) {
            // Check join condition (equality)
            if (rec1.find(left_col_name) != rec1.end() && rec2.find(right_col_name) != rec2.end()) {
                if (rec1.at(left_col_name) == rec2.at(right_col_name)) {
                    // Combine records
                    Record combined;
                    for (const auto& [key, value] : rec1) {
                        combined[table1_name + "." + key] = value;
                    }
                    for (const auto& [key, value] : rec2) {
                        combined[table2_name + "." + key] = value;
                    }
                    
                    // Apply WHERE conditions if any
                    if (where_conditions.empty() || evaluateCondition(combined, where_conditions, where_operators)) {
                        results.push_back(combined);
                    }
                }
            }
        }
    }
    
    cerr << "Joined " << results.size() << " records" << endl;
    return results;
}

bool DatabaseManager::createDatabase(const string& db_name) {
    try {
        filesystem::path dbDir = getDatabasePath(db_name);
        cerr << "Creating database at path: " << dbDir << endl;

        if (filesystem::exists(dbDir)) {
            cerr << "Database '" << db_name << "' already exists at: " << dbDir << endl;
            return false;
        }

        // Create the database directory
        if (!filesystem::create_directories(dbDir)) {
            cerr << "Failed to create database directory at: " << dbDir << endl;
            return false;
        }

        // Create catalog file
        filesystem::path catalogPath = dbDir / "catalog.bin";
        ofstream catalog_file(catalogPath, ios::binary);
        if (!catalog_file.is_open()) {
            cerr << "Failed to create catalog file at: " << catalogPath << endl;
            return false;
        }
        catalog_file.close();

        cerr << "Successfully created database '" << db_name << "'" << endl;
        return true;
    }
    catch (const filesystem::filesystem_error& e) {
        cerr << "Filesystem error in createDatabase: " << e.what() << endl;
        return false;
    }
    catch (const exception& e) {
        cerr << "Error creating database: " << e.what() << endl;
        return false;
    }
}

filesystem::path DatabaseManager::getDatabasePath(const string& db_name) {
    try {
        // Use the same db_data directory as in constructor
        filesystem::path dataDir = "db_data";
        
        // Create the data directory if it doesn't exist
        if (!filesystem::exists(dataDir)) {
            filesystem::create_directories(dataDir);
        }
        
        // Return the database path without creating it
        return dataDir / db_name;
    }
    catch (const filesystem::filesystem_error& e) {
        cerr << "Filesystem error in getDatabasePath: " << e.what() << endl;
        cerr << "Path: " << e.path1() << endl;
        throw;
    }
    catch (const exception& e) {
        cerr << "Error in getDatabasePath: " << e.what() << endl;
        throw;
    }
}

bool DatabaseManager::dropDatabase(const string& db_name) {
    filesystem::path dbDir = getDatabasePath(db_name);

    if (!filesystem::exists(dbDir)) {
        cerr << "Database '" << db_name << "' does not exist." << endl;
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
        filesystem::remove_all(dbDir);
        return true;
    }
    catch (const filesystem::filesystem_error& e) {
        cerr << "Error dropping database: " << e.what() << endl;
        return false;
    }
}

bool DatabaseManager::useDatabase(const string& db_name) {
    // Check if already using this database
    if (current_database == db_name) {
        cout << "Already using database: " << db_name << endl;
        return true;
    }

    // Validate database exists
    filesystem::path db_path = "db_data/" + db_name;
    if (!filesystem::exists(db_path)) {
        cerr << "Database '" << db_name << "' does not exist." << endl;
        return false;
    }

    // Clear existing context
    current_database.clear();
    catalog.tables.clear();
    for (auto& index : indexes) {
        index.second->close();
        delete index.second;
    }
    indexes.clear();

    // Set new database
    current_database = db_name;
    catalog_path = (db_path / "catalog.bin").string(); // Convert path to string
    catalog.load(catalog_path);
    cout << "Switching to database: " << db_name << endl;

    // Load indexes
    loadIndexes();
    cout << "Loaded indexes for database: " << db_name << endl;

    return true;
}

bool DatabaseManager::dropTable(const string& table_name) {
    if (current_database.empty()) {
        cerr << "No database selected. Use 'USE DATABASE' first." << endl;
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
        cerr << "Table '" << table_name << "' does not exist." << endl;
        return false;
    }

    // Save current database
    string saved_database = current_database;

    try {
        // Close and remove the index from memory
        auto it = indexes.find(table_name);
        if (it != indexes.end()) {
            it->second->close(); // Close the BPlusTree file handle
            delete it->second;
            indexes.erase(it);
            cout << "Closed and removed index for table: " << table_name << endl;
        }

        // Remove table from catalog first
        if (!catalog.removeTable(table_name)) {
            cerr << "Failed to remove table from catalog." << endl;
            current_database = saved_database;
            return false;
        }

        // Save the updated catalog
        catalog.save(catalog_path);
        cout << "Catalog updated for table: " << table_name << endl;

        // Clear database context to prevent reloading
        current_database.clear(); // Temporarily "unuse" database
        indexes.clear(); // Ensure no indexes remain active
        cout << "Cleared database context for table: " << table_name << endl;

        // Delete the table's data file
        error_code ec;
        if (filesystem::exists(schema.data_file_path)) {
            ensureWritePermissions(schema.data_file_path);
            if (!filesystem::remove(schema.data_file_path, ec)) {
                cerr << "Failed to delete data file: " << ec.message() << endl;
                current_database = saved_database;
                return false;
            }
            cout << "Deleted data file: " << schema.data_file_path << endl;
        } else {
            cout << "Data file does not exist: " << schema.data_file_path << endl;
        }

        // Delete the table's index file with delay
        if (filesystem::exists(schema.index_file_path)) {
            ensureWritePermissions(schema.index_file_path);
            this_thread::sleep_for(chrono::milliseconds(100)); // Wait for handle release
            if (!filesystem::remove(schema.index_file_path, ec)) {
                cerr << "Failed to delete index file: " << ec.message() << endl;
                current_database = saved_database;
                return false;
            }
            cout << "Deleted index file: " << schema.index_file_path << endl;
        } else {
            cout << "Index file does not exist: " << schema.index_file_path << endl;
        }

        // Restore database context without reloading indexes
        current_database = saved_database;
        cout << "Restored database context: " << current_database << endl;

        return true;
    } catch (const filesystem::filesystem_error& e) {
        cerr << "Filesystem error in dropTable: " << e.what() << endl;
        cerr << "Path: " << e.path1() << endl;
        current_database = saved_database;
        return false;
    } catch (const exception& e) {
        cerr << "Unexpected error in dropTable: " << e.what() << endl;
        current_database = saved_database;
        return false;
    }
}

vector<string> DatabaseManager::listDatabases() const {
    vector<string> databases;
    filesystem::path dataDir = "db_data";

    if (!filesystem::exists(dataDir)) {
        return databases;
    }

    for (const auto& entry : filesystem::directory_iterator(dataDir)) {
        if (entry.is_directory()) {
            databases.push_back(entry.path().filename().string());
        }
    }

    return databases;
}

string DatabaseManager::getCurrentDatabase() const {
    return current_database;
}