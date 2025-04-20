#include "database_manager.h"
#include <iostream>
#include <filesystem>
#include <limits>

void clearInputBuffer() {
    std::cin.clear();
    std::cin.ignore((std::numeric_limits<std::streamsize>::max)(), '\n');
}

void displayTableData(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        std::cout << "No tables exist. Please create a table first.\n";
        return;
    }

    std::cout << "Available tables:\n";
    for (const auto& table : tables) {
        std::cout << " - " << table << "\n";
    }

    std::string tableName;
    std::cout << "Enter table name to display all data: ";
    std::cin >> tableName;
    std::cin.ignore((std::numeric_limits<std::streamsize>::max)(), '\n');

    try {
        auto schema = db.getTableSchema(tableName);

        // Get all records
        auto results = db.getAllRecords(tableName);

        if (results.empty()) {
            std::cout << "Table '" << tableName << "' is empty.\n";
            return;
        }

        // Print table header
        std::cout << "\nData in table '" << tableName << "':\n";
        for (const auto& column : schema.columns) {
            std::cout << std::setw(20) << column.name << " | ";
        }
        std::cout << "\n" << std::string(schema.columns.size() * 22, '-') << "\n";

        // Print each record
        for (const auto& record : results) {
            for (const auto& column : schema.columns) {
                if (record.find(column.name) != record.end()) {
                    std::visit([](auto&& arg) {
                        std::cout << std::setw(20) << arg << " | ";
                        }, record.at(column.name));
                }
                else {
                    std::cout << std::setw(20) << "NULL" << " | ";
                }
            }
            std::cout << "\n";
        }
    }
    catch (...) {
        std::cout << "Error: Table not found or error reading data.\n";
    }
}
void createTableMenu(DatabaseManager& db) {
    std::string tableName;
    std::cout << "Enter table name: ";
    std::cin >> tableName;
    clearInputBuffer();

    std::vector<std::tuple<std::string, std::string, int>> columns;
    std::string primaryKey;
    std::map<std::string, std::pair<std::string, std::string>> foreignKeys;

    int numColumns;
    std::cout << "Number of columns: ";
    std::cin >> numColumns;
    clearInputBuffer();

    for (int i = 0; i < numColumns; i++) {
        std::string name, type;
        int length = 0;

        std::cout << "Column " << i + 1 << " name: ";
        std::cin >> name;
        clearInputBuffer();

        std::cout << "Column " << i + 1 << " type (int/float/string/char/bool): ";
        std::cin >> type;
        clearInputBuffer();

        if (type == "string" || type == "char") {
            std::cout << "Length for " << type << ": ";
            std::cin >> length;
            clearInputBuffer();
        }

        columns.emplace_back(name, type, length);

        // Check if this should be primary key
        char isPrimary;
        std::cout << "Is this the primary key? (y/n): ";
        std::cin >> isPrimary;
        clearInputBuffer();
        if (isPrimary == 'y' || isPrimary == 'Y') {
            primaryKey = name;
        }

        // Check for foreign key
        char isForeign;
        std::cout << "Is this a foreign key? (y/n): ";
        std::cin >> isForeign;
        clearInputBuffer();
        if (isForeign == 'y' || isForeign == 'Y') {
            std::string refTable, refColumn;
            std::cout << "Reference table: ";
            std::cin >> refTable;
            std::cout << "Reference column: ";
            std::cin >> refColumn;
            foreignKeys[name] = { refTable, refColumn };
        }
    }

    if (db.createTable(tableName, columns, primaryKey, foreignKeys)) {
        std::cout << "Table created successfully!\n";
    }
    else {
        std::cout << "Failed to create table.\n";
    }
}

void insertRecordMenu(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        std::cout << "No tables exist. Please create a table first.\n";
        return;
    }

    std::cout << "Available tables:\n";
    for (const auto& table : tables) {
        std::cout << " - " << table << "\n";
    }

    std::string tableName;
    std::cout << "Enter table name: ";
    std::cin >> tableName;
    clearInputBuffer();

    // Check if table exists
    bool tableFound = false;
    for (const auto& table : tables) {
        if (table == tableName) {
            tableFound = true;
            break;
        }
    }

    if (!tableFound) {
        std::cout << "Error: Table '" << tableName << "' not found.\n";
        return;
    }

    try {
        auto schema = db.getTableSchema(tableName);
        Record record;

        for (const auto& column : schema.columns) {
            bool validInput = false;
            while (!validInput) {
                std::cout << "Enter value for " << column.name;

                // Show column type, constraints, and references
                std::cout << " (" << [&]() -> std::string {  // Explicit return type
                    switch (column.type) {
                    case Column::INT: return "int";
                    case Column::FLOAT: return "float";
                    case Column::STRING: return "string(max " + std::to_string(column.length) + " chars)";
                    case Column::CHAR: return "char(max " + std::to_string(column.length) + " chars)";
                    case Column::BOOL: return "bool";
                    default: return "unknown";
                    }
                    }() << ")";
                if (column.is_primary_key) {
                    std::cout << " [PRIMARY KEY]";
                }

                if (column.is_foreign_key) {
                    std::cout << " [REFERENCES " << column.references_table << "."
                        << column.references_column << "]";
                }

                std::cout << ": ";

                try {
                    switch (column.type) {
                    case Column::INT: {
                        int value;
                        std::cin >> value;
                        if (std::cin.fail()) {
                            throw std::runtime_error("Invalid integer input");
                        }
                        clearInputBuffer();
                        record[column.name] = value;
                        validInput = true;
                        break;
                    }
                    case Column::FLOAT: {
                        float value;
                        std::cin >> value;
                        if (std::cin.fail()) {
                            throw std::runtime_error("Invalid float input");
                        }
                        clearInputBuffer();
                        record[column.name] = value;
                        validInput = true;
                        break;
                    }
                    case Column::STRING: {
                        std::string value;
                        std::cin.ignore(); // Clear newline
                        std::getline(std::cin, value);
                        if (value.length() > column.length) {
                            throw std::runtime_error("String too long (max " +
                                std::to_string(column.length) + " chars)");
                        }
                        record[column.name] = value;
                        validInput = true;
                        break;
                    }
                    case Column::CHAR: {
                        std::string value;
                        std::cin >> value;
                        clearInputBuffer();
                        if (value.length() > column.length) {
                            throw std::runtime_error("String too long (max " +
                                std::to_string(column.length) + " chars)");
                        }
                        record[column.name] = value;
                        validInput = true;
                        break;
                    }
                    case Column::BOOL: {
                        std::string input;
                        std::cin >> input;
                        clearInputBuffer();
                        std::transform(input.begin(), input.end(), input.begin(), ::tolower);
                        if (input != "true" && input != "false" && input != "1" && input != "0" &&
                            input != "y" && input != "n") {
                            throw std::runtime_error("Invalid boolean input (use true/false, 1/0, or y/n)");
                        }
                        bool value = (input == "true" || input == "1" || input == "y");
                        record[column.name] = value;
                        validInput = true;
                        break;
                    }
                    }
                }
                catch (const std::exception& e) {
                    std::cout << "Error: " << e.what() << "\nPlease try again.\n";
                    std::cin.clear();
                    std::cin.ignore((std::numeric_limits<std::streamsize>::max)(), '\n');
                }
            }
        }

        if (db.insertRecord(tableName, record)) {
            std::cout << "Record inserted successfully!\n";
        }
        else {
            std::cout << "Failed to insert record. Check constraints and try again.\n";
        }
    }
    catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void updateRecordMenu(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        std::cout << "No tables exist. Please create a table first.\n";
        return;
    }

    std::cout << "Available tables:\n";
    for (const auto& table : tables) {
        std::cout << " - " << table << "\n";
    }

    std::string tableName;
    std::cout << "Enter table name: ";
    std::cin >> tableName;
    clearInputBuffer();

    // Check if table exists
    bool tableFound = false;
    for (const auto& table : tables) {
        if (table == tableName) {
            tableFound = true;
            break;
        }
    }

    if (!tableFound) {
        std::cout << "Error: Table '" << tableName << "' not found.\n";
        return;
    }

    try {
        auto schema = db.getTableSchema(tableName);

        // Select column to use for finding the record
        std::cout << "Enter column name to use for record selection: ";
        std::string keyColumn;
        std::cin >> keyColumn;
        clearInputBuffer();

        // Find column type
        Column::Type colType = Column::INT;
        bool columnFound = false;

        for (const auto& col : schema.columns) {
            if (col.name == keyColumn) {
                colType = col.type;
                columnFound = true;
                break;
            }
        }

        if (!columnFound) {
            std::cout << "Error: Column '" << keyColumn << "' not found in table.\n";
            return;
        }

        // Get search value for this column
        std::cout << "Enter value to find record: ";
        FieldValue searchValue;

        switch (colType) {
        case Column::INT: {
            int value;
            std::cin >> value;
            searchValue = value;
            break;
        }
        case Column::FLOAT: {
            float value;
            std::cin >> value;
            searchValue = value;
            break;
        }
        case Column::STRING: {
            std::string value;
            std::cin.ignore();
            std::getline(std::cin, value);
            searchValue = value;
            break;
        }
        case Column::CHAR: {
            std::string value;
            std::cin >> value;
            searchValue = value;
            break;
        }
        case Column::BOOL: {
            bool value;
            std::string input;
            std::cin >> input;
            value = (input == "true" || input == "1" || input == "y");
            searchValue = value;
            break;
        }
        }

        // Search for records to confirm they exist
        auto matchingRecords = db.searchRecords(tableName, keyColumn, searchValue);
        if (matchingRecords.empty()) {
            std::cout << "No records found matching the criteria.\n";
            return;
        }

        std::cout << "Found " << matchingRecords.size() << " matching records.\n";
        std::cout << "Enter new values for fields (leave blank to keep current value):\n";

        Record newValues;

        for (const auto& column : schema.columns) {
            std::cout << "Update " << column.name << "? (y/n): ";
            char updateField;
            std::cin >> updateField;
            clearInputBuffer();

            if (updateField == 'y' || updateField == 'Y') {
                bool validInput = false;
                while (!validInput) {
                    std::cout << "Enter new value for " << column.name;

                    std::cout << " (" << [&]() -> std::string {
                        switch (column.type) {
                        case Column::INT: return "int";
                        case Column::FLOAT: return "float";
                        case Column::STRING: return "string(max " + std::to_string(column.length) + " chars)";
                        case Column::CHAR: return "char(max " + std::to_string(column.length) + " chars)";
                        case Column::BOOL: return "bool";
                        default: return "unknown";
                        }
                        }() << "): ";

                    try {
                        switch (column.type) {
                        case Column::INT: {
                            int value;
                            std::cin >> value;
                            if (std::cin.fail()) {
                                throw std::runtime_error("Invalid integer input");
                            }
                            clearInputBuffer();
                            newValues[column.name] = value;
                            validInput = true;
                            break;
                        }
                        case Column::FLOAT: {
                            float value;
                            std::cin >> value;
                            if (std::cin.fail()) {
                                throw std::runtime_error("Invalid float input");
                            }
                            clearInputBuffer();
                            newValues[column.name] = value;
                            validInput = true;
                            break;
                        }
                        case Column::STRING: {
                            std::string value;
                            std::cin.ignore(); // Clear newline
                            std::getline(std::cin, value);
                            if (value.length() > column.length) {
                                throw std::runtime_error("String too long (max " +
                                    std::to_string(column.length) + " chars)");
                            }
                            newValues[column.name] = value;
                            validInput = true;
                            break;
                        }
                        case Column::CHAR: {
                            std::string value;
                            std::cin >> value;
                            clearInputBuffer();
                            if (value.length() > column.length) {
                                throw std::runtime_error("String too long (max " +
                                    std::to_string(column.length) + " chars)");
                            }
                            newValues[column.name] = value;
                            validInput = true;
                            break;
                        }
                        case Column::BOOL: {
                            std::string input;
                            std::cin >> input;
                            clearInputBuffer();
                            std::transform(input.begin(), input.end(), input.begin(), ::tolower);
                            if (input != "true" && input != "false" && input != "1" && input != "0" &&
                                input != "y" && input != "n") {
                                throw std::runtime_error("Invalid boolean input (use true/false, 1/0, or y/n)");
                            }
                            bool value = (input == "true" || input == "1" || input == "y");
                            newValues[column.name] = value;
                            validInput = true;
                            break;
                        }
                        }
                    }
                    catch (const std::exception& e) {
                        std::cout << "Error: " << e.what() << "\nPlease try again.\n";
                        std::cin.clear();
                        std::cin.ignore((std::numeric_limits<std::streamsize>::max)(), '\n');
                    }
                }
            }
        }

        if (newValues.empty()) {
            std::cout << "No fields selected for update. Operation cancelled.\n";
            return;
        }

        if (db.updateRecord(tableName, keyColumn, searchValue, newValues)) {
            std::cout << "Records updated successfully!\n";
        }
        else {
            std::cout << "Failed to update records.\n";
        }
    }
    catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void listTablesMenu(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        std::cout << "No tables exist.\n";
        return;
    }

    std::cout << "Available tables:\n";
    for (const auto& table : tables) {
        auto schema = db.getTableSchema(table);
        std::cout << "\nTable: " << table << "\n";
        std::cout << "Columns:\n";

        for (const auto& column : schema.columns) {
            std::cout << " - " << column.name << " (";
            switch (column.type) {
            case Column::INT: std::cout << "INT"; break;
            case Column::FLOAT: std::cout << "FLOAT"; break;
            case Column::STRING: std::cout << "STRING(" << column.length << ")"; break;
            case Column::CHAR: std::cout << "CHAR(" << column.length << ")"; break;
            case Column::BOOL: std::cout << "BOOL"; break;
            }

            if (column.is_primary_key) std::cout << ", PRIMARY KEY";
            if (column.is_foreign_key) {
                std::cout << ", FOREIGN KEY REFERENCES " << column.references_table
                    << "(" << column.references_column << ")";
            }
            std::cout << ")\n";
        }
    }
}

// First complete the deleteRecordMenu function that was cut off
void deleteRecordMenu(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        std::cout << "No tables exist. Please create a table first.\n";
        return;
    }

    std::cout << "Available tables:\n";
    for (const auto& table : tables) {
        std::cout << " - " << table << "\n";
    }

    std::string tableName;
    std::cout << "Enter table name: ";
    std::cin >> tableName;
    clearInputBuffer();

    // Check if table exists
    bool tableFound = false;
    for (const auto& table : tables) {
        if (table == tableName) {
            tableFound = true;
            break;
        }
    }

    if (!tableFound) {
        std::cout << "Error: Table '" << tableName << "' not found.\n";
        return;
    }

    try {
        auto schema = db.getTableSchema(tableName);

        // Select column to use for finding the record
        std::cout << "Enter column name to use for record selection: ";
        std::string keyColumn;
        std::cin >> keyColumn;
        clearInputBuffer();

        // Find column type
        Column::Type colType = Column::INT;
        bool columnFound = false;

        for (const auto& col : schema.columns) {
            if (col.name == keyColumn) {
                colType = col.type;
                columnFound = true;
                break;
            }
        }

        if (!columnFound) {
            std::cout << "Error: Column '" << keyColumn << "' not found in table.\n";
            return;
        }

        // Get search value for this column
        std::cout << "Enter value to find record(s) to delete: ";
        FieldValue searchValue;

        switch (colType) {
        case Column::INT: {
            int value;
            std::cin >> value;
            searchValue = value;
            break;
        }
        case Column::FLOAT: {
            float value;
            std::cin >> value;
            searchValue = value;
            break;
        }
        case Column::STRING: {
            std::string value;
            std::cin.ignore();
            std::getline(std::cin, value);
            searchValue = value;
            break;
        }
        case Column::CHAR: {
            std::string value;
            std::cin >> value;
            searchValue = value;
            break;
        }
        case Column::BOOL: {
            bool value;
            std::string input;
            std::cin >> input;
            value = (input == "true" || input == "1" || input == "y");
            searchValue = value;
            break;
        }
        }

        // Search for records to confirm they exist
        auto matchingRecords = db.searchRecords(tableName, keyColumn, searchValue);
        if (matchingRecords.empty()) {
            std::cout << "No records found matching the criteria.\n";
            return;
        }

        std::cout << "Found " << matchingRecords.size() << " matching record(s).\n";
        std::cout << "Are you sure you want to delete " <<
            (matchingRecords.size() == 1 ? "this record" : "these records") << "? (y/n): ";

        char confirm;
        std::cin >> confirm;
        clearInputBuffer();

        if (confirm != 'y' && confirm != 'Y') {
            std::cout << "Delete operation cancelled.\n";
            return;
        }

        if (db.deleteRecord(tableName, keyColumn, searchValue)) {
            std::cout << "Records deleted successfully!\n";
        }
        else {
            std::cout << "Failed to delete records.\n";
        }
    }
    catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

// Now add a function to display records from a table
void displayRecordsMenu(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        std::cout << "No tables exist. Please create a table first.\n";
        return;
    }

    std::cout << "Available tables:\n";
    for (const auto& table : tables) {
        std::cout << " - " << table << "\n";
    }

    std::string tableName;
    std::cout << "Enter table name: ";
    std::cin >> tableName;
    clearInputBuffer();

    // Check if table exists
    bool tableFound = false;
    for (const auto& table : tables) {
        if (table == tableName) {
            tableFound = true;
            break;
        }
    }

    if (!tableFound) {
        std::cout << "Error: Table '" << tableName << "' not found.\n";
        return;
    }

    try {
        auto schema = db.getTableSchema(tableName);
        auto records = db.getAllRecords(tableName);

        if (records.empty()) {
            std::cout << "No records found in table '" << tableName << "'.\n";
            return;
        }

        // Display column headers
        std::cout << "\nTable: " << tableName << "\n";
        std::cout << std::string(80, '-') << "\n";
        for (const auto& col : schema.columns) {
            std::cout << std::left << std::setw(15) << col.name << " | ";
        }
        std::cout << "\n" << std::string(80, '-') << "\n";

        // Display records
        for (const auto& record : records) {
            for (const auto& col : schema.columns) {
                if (record.find(col.name) != record.end()) {
                    std::cout << std::left << std::setw(15);
                    if (std::holds_alternative<int>(record.at(col.name))) {
                        std::cout << std::get<int>(record.at(col.name));
                    }
                    else if (std::holds_alternative<float>(record.at(col.name))) {
                        std::cout << std::get<float>(record.at(col.name));
                    }
                    else if (std::holds_alternative<std::string>(record.at(col.name))) {
                        std::cout << std::get<std::string>(record.at(col.name));
                    }
                    else if (std::holds_alternative<bool>(record.at(col.name))) {
                        std::cout << (std::get<bool>(record.at(col.name)) ? "true" : "false");
                    }
                    std::cout << " | ";
                }
                else {
                    std::cout << std::left << std::setw(15) << "NULL" << " | ";
                }
            }
            std::cout << "\n";
        }
        std::cout << std::string(80, '-') << "\n";
    }
    catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

// Function to search records with more advanced filtering options
void searchRecordsMenu(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        std::cout << "No tables exist. Please create a table first.\n";
        return;
    }

    std::cout << "Available tables:\n";
    for (const auto& table : tables) {
        std::cout << " - " << table << "\n";
    }

    std::string tableName;
    std::cout << "Enter table name: ";
    std::cin >> tableName;
    clearInputBuffer();

    // Check if table exists
    bool tableFound = false;
    for (const auto& table : tables) {
        if (table == tableName) {
            tableFound = true;
            break;
        }
    }

    if (!tableFound) {
        std::cout << "Error: Table '" << tableName << "' not found.\n";
        return;
    }

    try {
        auto schema = db.getTableSchema(tableName);

        // Show available columns
        std::cout << "Available columns:\n";
        for (const auto& col : schema.columns) {
            std::cout << " - " << col.name << " (";
            switch (col.type) {
            case Column::INT: std::cout << "int"; break;
            case Column::FLOAT: std::cout << "float"; break;
            case Column::STRING: std::cout << "string"; break;
            case Column::CHAR: std::cout << "char"; break;
            case Column::BOOL: std::cout << "bool"; break;
            }
            std::cout << ")\n";
        }

        std::cout << "Enter column name to search by: ";
        std::string keyColumn;
        std::cin >> keyColumn;
        clearInputBuffer();

        // Find column type
        Column::Type colType = Column::INT;
        bool columnFound = false;

        for (const auto& col : schema.columns) {
            if (col.name == keyColumn) {
                colType = col.type;
                columnFound = true;
                break;
            }
        }

        if (!columnFound) {
            std::cout << "Error: Column '" << keyColumn << "' not found in table.\n";
            return;
        }

        std::cout << "Available operators: =, !=, >, <, >=, <=\n";
        std::cout << "Enter operator: ";
        std::string op;
        std::cin >> op;
        clearInputBuffer();

        // Validate operator
        if (op != "=" && op != "!=" && op != ">" && op != "<" && op != ">=" && op != "<=") {
            std::cout << "Error: Invalid operator '" << op << "'.\n";
            return;
        }

        // Get search value
        std::cout << "Enter value to search for: ";
        FieldValue searchValue;

        switch (colType) {
        case Column::INT: {
            int value;
            std::cin >> value;
            searchValue = value;
            break;
        }
        case Column::FLOAT: {
            float value;
            std::cin >> value;
            searchValue = value;
            break;
        }
        case Column::STRING: {
            std::string value;
            std::cin.ignore();
            std::getline(std::cin, value);
            searchValue = value;
            break;
        }
        case Column::CHAR: {
            std::string value;
            std::cin >> value;
            searchValue = value;
            break;
        }
        case Column::BOOL: {
            bool value;
            std::string input;
            std::cin >> input;
            value = (input == "true" || input == "1" || input == "y");
            searchValue = value;
            break;
        }
        }

        // Create conditions vector for advanced search
        std::vector<std::tuple<std::string, FieldValue, std::string>> conditions;
        conditions.push_back(std::make_tuple(keyColumn, searchValue, op));

        // Search records
        auto records = db.searchRecordsAdvanced(tableName, conditions);

        if (records.empty()) {
            std::cout << "No records found matching the criteria.\n";
            return;
        }

        // Display results
        std::cout << "\nFound " << records.size() << " matching record(s):\n";
        std::cout << std::string(80, '-') << "\n";

        // Display column headers
        for (const auto& col : schema.columns) {
            std::cout << std::left << std::setw(15) << col.name << " | ";
        }
        std::cout << "\n" << std::string(80, '-') << "\n";

        // Display records
        for (const auto& record : records) {
            for (const auto& col : schema.columns) {
                if (record.find(col.name) != record.end()) {
                    std::cout << std::left << std::setw(15);
                    if (std::holds_alternative<int>(record.at(col.name))) {
                        std::cout << std::get<int>(record.at(col.name));
                    }
                    else if (std::holds_alternative<float>(record.at(col.name))) {
                        std::cout << std::get<float>(record.at(col.name));
                    }
                    else if (std::holds_alternative<std::string>(record.at(col.name))) {
                        std::cout << std::get<std::string>(record.at(col.name));
                    }
                    else if (std::holds_alternative<bool>(record.at(col.name))) {
                        std::cout << (std::get<bool>(record.at(col.name)) ? "true" : "false");
                    }
                    std::cout << " | ";
                }
                else {
                    std::cout << std::left << std::setw(15) << "NULL" << " | ";
                }
            }
            std::cout << "\n";
        }
        std::cout << std::string(80, '-') << "\n";
    }
    catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

void createDatabaseMenu(DatabaseManager& db) {
    std::string dbName;
    std::cout << "Enter database name to create: ";
    std::cin >> dbName;
    clearInputBuffer();

    if (db.createDatabase(dbName)) {
        std::cout << "Database '" << dbName << "' created successfully.\n";
    } else {
        std::cout << "Failed to create database '" << dbName << "'.\n";
    }
}

void dropDatabaseMenu(DatabaseManager& db) {
    auto databases = db.listDatabases();
    if (databases.empty()) {
        std::cout << "No databases exist.\n";
        return;
    }

    std::cout << "Available databases:\n";
    for (const auto& dbName : databases) {
        std::cout << " - " << dbName << "\n";
    }

    std::string dbName;
    std::cout << "Enter database name to drop: ";
    std::cin >> dbName;
    clearInputBuffer();

    if (db.dropDatabase(dbName)) {
        std::cout << "Database '" << dbName << "' dropped successfully.\n";
    } else {
        std::cout << "Failed to drop database '" << dbName << "'.\n";
    }
}

void useDatabaseMenu(DatabaseManager& db) {
    auto databases = db.listDatabases();
    if (databases.empty()) {
        std::cout << "No databases exist.\n";
        return;
    }

    std::cout << "Available databases:\n";
    for (const auto& dbName : databases) {
        std::cout << " - " << dbName << "\n";
    }

    std::string dbName;
    std::cout << "Enter database name to use: ";
    std::cin >> dbName;
    clearInputBuffer();

    if (db.useDatabase(dbName)) {
        std::cout << "Using database '" << dbName << "'.\n";
    } else {
        std::cout << "Failed to use database '" << dbName << "'.\n";
    }
}

void dropTableMenu(DatabaseManager& db) {
    if (db.getCurrentDatabase().empty()) {
        std::cout << "No database selected. Please use a database first.\n";
        return;
    }

    auto tables = db.listTables();
    if (tables.empty()) {
        std::cout << "No tables exist in the current database.\n";
        return;
    }

    std::cout << "Available tables:\n";
    for (const auto& table : tables) {
        std::cout << " - " << table << "\n";
    }

    std::string tableName;
    std::cout << "Enter table name to drop: ";
    std::cin >> tableName;
    clearInputBuffer();

    if (db.dropTable(tableName)) {
        std::cout << "Table '" << tableName << "' dropped successfully.\n";
    } else {
        std::cout << "Failed to drop table '" << tableName << "'.\n";
    }
}

void listDatabasesMenu(DatabaseManager& db) {
    auto databases = db.listDatabases();
    if (databases.empty()) {
        std::cout << "No databases exist.\n";
        return;
    }

    std::cout << "Available databases:\n";
    for (const auto& dbName : databases) {
        std::cout << " - " << dbName;
        if (dbName == db.getCurrentDatabase()) {
            std::cout << " (current)";
        }
        std::cout << "\n";
    }
}

void mainMenu() {
    // Create the database manager
    DatabaseManager db("catalog.dat");

    bool running = true;
    while (running) {
        std::cout << "\n=== Database Management System ===\n";
        std::cout << "1. Create Table\n";
        std::cout << "2. Insert Record\n";
        std::cout << "3. Update Record\n";
        std::cout << "4. Delete Record\n";
        std::cout << "5. Display Records\n";
        std::cout << "6. Search Records\n";
        std::cout << "7. Create Database\n";
        std::cout << "8. Drop Database\n";
        std::cout << "9. Use Database\n";
        std::cout << "10. Drop Table\n";
        std::cout << "11. List Databases\n";
        std::cout << "12. Exit\n";
        std::cout << "Select an option: ";

        int choice;
        std::cin >> choice;
        clearInputBuffer();

        switch (choice) {
        case 1:
            createTableMenu(db);
            break;
        case 2:
            insertRecordMenu(db);
            break;
        case 3:
            updateRecordMenu(db);
            break;
        case 4:
            deleteRecordMenu(db);
            break;
        case 5:
            displayRecordsMenu(db);
            break;
        case 6:
            searchRecordsMenu(db);
            break;
        case 7:
            createDatabaseMenu(db);
            break;
        case 8:
            dropDatabaseMenu(db);
            break;
        case 9:
            useDatabaseMenu(db);
            break;
        case 10:
            dropTableMenu(db);
            break;
        case 11:
            listDatabasesMenu(db);
            break;
        case 12:
            running = false;
            std::cout << "Exiting program. Goodbye!\n";
            break;
        default:
            std::cout << "Invalid choice. Please try again.\n";
            break;
        }
    }
}

int main() {
    mainMenu();
    return 0;
}