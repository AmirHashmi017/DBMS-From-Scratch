#include "database_manager.h"
#include <iostream>
#include <filesystem>
#include <limits>
#include <algorithm>
#include <iomanip>  
#include <variant> 
#include <map> 
void clearInputBuffer() {
    std::cin.clear();
    std::cin.ignore((std::numeric_limits<std::streamsize>::max)(), '\n');
}


// Add these functions to your main file

void searchRecordsWithFilterMenu(DatabaseManager& db) {
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

    TableSchema schema;
    try {
        schema = db.getTableSchema(tableName);
    }
    catch (...) {
        std::cout << "Error: Table not found.\n";
        return;
    }

    // Display available columns
    std::cout << "Available columns:\n";
    for (const auto& col : schema.columns) {
        std::cout << " - " << col.name << " (";
        switch (col.type) {
        case Column::INT: std::cout << "INT"; break;
        case Column::FLOAT: std::cout << "FLOAT"; break;
        case Column::STRING: std::cout << "STRING"; break;
        case Column::CHAR: std::cout << "CHAR"; break;
        case Column::BOOL: std::cout << "BOOL"; break;
        }
        std::cout << ")\n";
    }

    // Build conditions
    std::vector<std::tuple<std::string, std::string, FieldValue>> conditions;
    std::vector<std::string> operators;

    std::cout << "Enter number of conditions: ";
    int numConditions;
    std::cin >> numConditions;
    clearInputBuffer();

    for (int i = 0; i < numConditions; i++) {
        std::cout << "\nCondition " << (i + 1) << ":\n";

        // Get column
        std::string columnName;
        std::cout << "Enter column name: ";
        std::cin >> columnName;
        clearInputBuffer();

        // Find column type
        Column::Type colType = Column::INT;
        bool found = false;
        for (const auto& col : schema.columns) {
            if (col.name == columnName) {
                colType = col.type;
                found = true;
                break;
            }
        }

        if (!found) {
            std::cout << "Column not found. Using default type INT.\n";
        }

        // Get operator
        std::cout << "Available operators: =, !=, >, <, >=, <=, LIKE\n";
        std::cout << "Enter operator: ";
        std::string op;
        std::cin >> op;
        clearInputBuffer();

        // Get value
        std::cout << "Enter value: ";
        FieldValue value;

        switch (colType) {
        case Column::INT: {
            int val;
            std::cin >> val;
            value = val;
            break;
        }
        case Column::FLOAT: {
            float val;
            std::cin >> val;
            value = val;
            break;
        }
                          // Update all the case blocks for STRING and CHAR in main.cpp like this:

        case Column::STRING:
        case Column::CHAR: {
            clearInputBuffer();  // Add this to clear any remaining input first
            std::string val;
            std::getline(std::cin, val);
            value = val;
            break;
        }
        case Column::BOOL: {
            std::string val;
            std::cin >> val;
            bool boolVal = (val == "true" || val == "1" || val == "y" || val == "yes");
            value = boolVal;
            break;
        }
        }

        // Add condition
        conditions.push_back(std::make_tuple(columnName, op, value));

        // Add logical operator for the next condition if not the last one
        if (i < numConditions - 1) {
            std::cout << "Logical operator for next condition (AND/OR/NOT): ";
            std::string logicalOp;
            std::cin >> logicalOp;
            clearInputBuffer();
            operators.push_back(logicalOp);
        }
    }
    try {
        auto results = db.searchRecordsWithFilter(tableName, conditions, operators);

        // Display results with proper formatting
        std::cout << "\nFound " << results.size() << " records:\n";

        // Print column headers
        for (const auto& col : schema.columns) {
            std::cout << std::setw(20) << col.name << " | ";
        }
        std::cout << "\n" << std::string(schema.columns.size() * 22, '-') << "\n";

        // Print records with proper type handling
        for (const auto& record : results) {
            for (const auto& col : schema.columns) {
                if (record.find(col.name) != record.end()) {
                    std::visit([](auto&& arg) {
                        std::cout << std::setw(20) << arg << " | ";
                        }, record.at(col.name));
                }
                else {
                    std::cout << std::setw(20) << "NULL" << " | ";
                }
            }
            std::cout << "\n";
        }
    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
    }
}


void updateRecordsWithFilterMenu(DatabaseManager& db) {
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

    TableSchema schema;
    try {
        schema = db.getTableSchema(tableName);
    }
    catch (...) {
        std::cout << "Error: Table not found.\n";
        return;
    }

    // Display available columns
    std::cout << "Available columns:\n";
    for (const auto& col : schema.columns) {
        std::cout << " - " << col.name << " (";
        switch (col.type) {
        case Column::INT: std::cout << "INT"; break;
        case Column::FLOAT: std::cout << "FLOAT"; break;
        case Column::STRING: std::cout << "STRING"; break;
        case Column::CHAR: std::cout << "CHAR"; break;
        case Column::BOOL: std::cout << "BOOL"; break;
        }
        std::cout << ")\n";
    }

    // Build update values
    std::map<std::string, FieldValue> updateValues;

    std::cout << "Enter number of columns to update: ";
    int numColumns;
    std::cin >> numColumns;
    clearInputBuffer();

    for (int i = 0; i < numColumns; i++) {
        std::cout << "\nColumn " << (i + 1) << " to update:\n";

        // Get column
        std::string columnName;
        std::cout << "Enter column name: ";
        std::cin >> columnName;
        clearInputBuffer();

        // Find column type
        Column::Type colType = Column::INT;
        bool found = false;
        for (const auto& col : schema.columns) {
            if (col.name == columnName) {
                colType = col.type;
                found = true;
                break;
            }
        }

        if (!found) {
            std::cout << "Column not found. Using default type INT.\n";
        }

        // Get new value
        std::cout << "Enter new value: ";
        FieldValue value;

        switch (colType) {
        case Column::INT: {
            int val;
            std::cin >> val;
            value = val;
            break;
        }
        case Column::FLOAT: {
            float val;
            std::cin >> val;
            value = val;
            break;
        }
                          // Update all the case blocks for STRING and CHAR in main.cpp like this:

        case Column::STRING:
        case Column::CHAR: {
            clearInputBuffer();    // Add this line to fix string input
            std::string val;
            std::getline(std::cin, val);
            value = val;
            break;
        }
        case Column::BOOL: {
            std::string val;
            std::cin >> val;
            bool boolVal = (val == "true" || val == "1" || val == "y" || val == "yes");
            value = boolVal;
            break;
        }
        }

        // Add update value
        updateValues[columnName] = value;
    }

    // Build conditions
    std::vector<std::tuple<std::string, std::string, FieldValue>> conditions;
    std::vector<std::string> operators;

    std::cout << "\nEnter number of conditions: ";
    int numConditions;
    std::cin >> numConditions;
    clearInputBuffer();

    for (int i = 0; i < numConditions; i++) {
        std::cout << "\nCondition " << (i + 1) << ":\n";

        // Get column
        std::string columnName;
        std::cout << "Enter column name: ";
        std::cin >> columnName;
        clearInputBuffer();

        // Find column type
        Column::Type colType = Column::INT;
        bool found = false;
        for (const auto& col : schema.columns) {
            if (col.name == columnName) {
                colType = col.type;
                found = true;
                break;
            }
        }

        if (!found) {
            std::cout << "Column not found. Using default type INT.\n";
        }

        // Get operator
        std::cout << "Available operators: =, !=, >, <, >=, <=, LIKE\n";
        std::cout << "Enter operator: ";
        std::string op;
        std::cin >> op;
        clearInputBuffer();

        // Get value
        std::cout << "Enter value: ";
        FieldValue value;

        switch (colType) {
        case Column::INT: {
            int val;
            std::cin >> val;
            value = val;
            break;
        }
        case Column::FLOAT: {
            float val;
            std::cin >> val;
            value = val;
            break;
        }
        case Column::STRING:
        case Column::CHAR: {
            std::string val;
            std::getline(std::cin, val);
            value = val;
            break;
        }
        case Column::BOOL: {
            std::string val;
            std::cin >> val;
            bool boolVal = (val == "true" || val == "1" || val == "y" || val == "yes");
            value = boolVal;
            break;
        }
        }

        // Add condition
        conditions.push_back(std::make_tuple(columnName, op, value));

        // Add logical operator for the next condition if not the last one
        if (i < numConditions - 1) {
            std::cout << "Logical operator for next condition (AND/OR/NOT): ";
            std::string logicalOp;
            std::cin >> logicalOp;
            clearInputBuffer();
            operators.push_back(logicalOp);
        }
    }

    // Perform update
    bool success = db.updateRecordsWithFilter(tableName, updateValues, conditions, operators);

    if (success) {
        std::cout << "Records updated successfully.\n";
    }
    else {
        std::cout << "Failed to update records.\n";
    }
}
// Completing the deleteRecordsWithFilterMenu function that was cut off
void deleteRecordsWithFilterMenu(DatabaseManager& db) {
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

    TableSchema schema;
    try {
        schema = db.getTableSchema(tableName);
    }
    catch (...) {
        std::cout << "Error: Table not found.\n";
        return;
    }

    // Display available columns
    std::cout << "Available columns:\n";
    for (const auto& col : schema.columns) {
        std::cout << " - " << col.name << " (";
        switch (col.type) {
        case Column::INT: std::cout << "INT"; break;
        case Column::FLOAT: std::cout << "FLOAT"; break;
        case Column::STRING: std::cout << "STRING"; break;
        case Column::CHAR: std::cout << "CHAR"; break;
        case Column::BOOL: std::cout << "BOOL"; break;
        }
        std::cout << ")\n";
    }

    // Build conditions
    std::vector<std::tuple<std::string, std::string, FieldValue>> conditions;
    std::vector<std::string> operators;

    std::cout << "Enter number of conditions: ";
    int numConditions;
    std::cin >> numConditions;
    clearInputBuffer();

    for (int i = 0; i < numConditions; i++) {
        std::cout << "\nCondition " << (i + 1) << ":\n";

        // Get column
        std::string columnName;
        std::cout << "Enter column name: ";
        std::cin >> columnName;
        clearInputBuffer();

        // Find column type
        Column::Type colType = Column::INT;
        bool found = false;
        for (const auto& col : schema.columns) {
            if (col.name == columnName) {
                colType = col.type;
                found = true;
                break;
            }
        }

        if (!found) {
            std::cout << "Column not found. Using default type INT.\n";
        }

        // Get operator
        std::cout << "Available operators: =, !=, >, <, >=, <=, LIKE\n";
        std::cout << "Enter operator: ";
        std::string op;
        std::cin >> op;
        clearInputBuffer();

        // Get value
        std::cout << "Enter value: ";
        FieldValue value;

        switch (colType) {
        case Column::INT: {
            int val;
            std::cin >> val;
            value = val;
            break;
        }
        case Column::FLOAT: {
            float val;
            std::cin >> val;
            value = val;
            break;
        }
        case Column::STRING:
        case Column::CHAR: {
            clearInputBuffer();  // Add this to clear any remaining input first
            std::string val;
            std::getline(std::cin, val);
            value = val;
            break;
        }
        case Column::BOOL: {
            std::string val;
            std::cin >> val;
            bool boolVal = (val == "true" || val == "1" || val == "y" || val == "yes");
            value = boolVal;
            break;
        }
        }

        // Add condition
        conditions.push_back(std::make_tuple(columnName, op, value));

        // Add logical operator for the next condition if not the last one
        if (i < numConditions - 1) {
            std::cout << "Logical operator for next condition (AND/OR/NOT): ";
            std::string logicalOp;
            std::cin >> logicalOp;
            clearInputBuffer();
            operators.push_back(logicalOp);
        }
    }

    // Confirm deletion
    std::cout << "\nWARNING: This will delete all records matching your conditions.\n";
    std::cout << "Are you sure you want to proceed? (y/n): ";
    std::string confirm;
    std::cin >> confirm;

    if (confirm != "y" && confirm != "Y" && confirm != "yes" && confirm != "YES") {
        std::cout << "Delete operation cancelled.\n";
        return;
    }

    // Perform delete
    int deletedCount = db.deleteRecordsWithFilter(tableName, conditions, operators);

    std::cout << "Deleted " << deletedCount << " records.\n";
}

// Function to display all records in a table
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
    std::cout << "Enter table name: ";
    std::cin >> tableName;
    clearInputBuffer();

    TableSchema schema;
    try {
        schema = db.getTableSchema(tableName);
    }
    catch (...) {
        std::cout << "Error: Table not found.\n";
        return;
    }

    // Get all records
    auto records = db.getAllRecords(tableName);

    std::cout << "\nTable: " << tableName << ", Total Records: " << records.size() << "\n";

    // Print column headers
    for (const auto& col : schema.columns) {
        std::cout << col.name << " | ";
    }
    std::cout << "\n";
    std::cout << std::string(80, '-') << "\n";

    // Print records
    for (const auto& record : records) {
        for (const auto& col : schema.columns) {
            if (record.find(col.name) != record.end()) {
                std::visit([](auto&& arg) {
                    std::cout << arg << " | ";
                    }, record.at(col.name));
            }
            else {
                std::cout << "(null) | ";
            }
        }
        std::cout << "\n";
    }
}

// Function to create a table
void createTableMenu(DatabaseManager& db) {
    std::string tableName;
    std::cout << "Enter table name: ";
    std::cin >> tableName;
    clearInputBuffer();

    int numColumns;
    std::cout << "Enter number of columns: ";
    std::cin >> numColumns;
    clearInputBuffer();

    std::vector<std::tuple<std::string, std::string, int>> columns;
    std::string primaryKey;
    std::map<std::string, std::pair<std::string, std::string>> foreignKeys;

    for (int i = 0; i < numColumns; i++) {
        std::string colName, colType;
        int colLength = 0;

        std::cout << "\nColumn " << (i + 1) << ":\n";
        std::cout << "Name: ";
        std::cin >> colName;
        clearInputBuffer();

        std::cout << "Type (int, float, string, char, bool): ";
        std::cin >> colType;
        clearInputBuffer();

        if (colType == "string" || colType == "char") {
            std::cout << "Length: ";
            std::cin >> colLength;
            clearInputBuffer();
        }

        columns.push_back(std::make_tuple(colName, colType, colLength));

        std::cout << "Is this the primary key? (y/n): ";
        char isPk;
        std::cin >> isPk;
        clearInputBuffer();

        if (isPk == 'y' || isPk == 'Y') {
            primaryKey = colName;
        }

        std::cout << "Is this a foreign key? (y/n): ";
        char isFk;
        std::cin >> isFk;
        clearInputBuffer();

        if (isFk == 'y' || isFk == 'Y') {
            std::string refTable, refColumn;
            std::cout << "Referenced table: ";
            std::cin >> refTable;
            clearInputBuffer();

            std::cout << "Referenced column: ";
            std::cin >> refColumn;
            clearInputBuffer();

            foreignKeys[colName] = std::make_pair(refTable, refColumn);
        }
    }

    if (primaryKey.empty()) {
        std::cout << "Warning: No primary key specified.\n";
    }

    bool success = db.createTable(tableName, columns, primaryKey, foreignKeys);
    if (success) {
        std::cout << "Table created successfully.\n";
    }
    else {
        std::cout << "Failed to create table.\n";
    }
}

// Function to insert a record
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

    try {
        auto schema = db.getTableSchema(tableName);
        Record record;

        for (const auto& column : schema.columns) {
            std::cout << "Enter value for column '" << column.name << "' (";
            switch (column.type) {
            case Column::INT: std::cout << "INT"; break;
            case Column::FLOAT: std::cout << "FLOAT"; break;
            case Column::STRING: std::cout << "STRING"; break;
            case Column::CHAR: std::cout << "CHAR"; break;
            case Column::BOOL: std::cout << "BOOL"; break;
            }
            std::cout << "): ";

            FieldValue value;
            switch (column.type) {
            case Column::INT: {
                int val;
                std::cin >> val;
                value = val;
                break;
            }
            case Column::FLOAT: {
                float val;
                std::cin >> val;
                value = val;
                break;
            }
            case Column::STRING:
            case Column::CHAR: {
                std::string val;
                std::getline(std::cin, val);
                value = val;
                break;
            }

            case Column::BOOL: {
                std::string val;
                std::cin >> val;
                bool boolVal = (val == "true" || val == "1" || val == "y" || val == "yes");
                value = boolVal;
                break;
            }
            }

            record[column.name] = value;
            clearInputBuffer();
        }

        bool success = db.insertRecord(tableName, record);
        if (success) {
            std::cout << "Record inserted successfully.\n";
        }
        else {
            std::cout << "Failed to insert record.\n";
        }
    }
    catch (...) {
        std::cout << "Error: Table not found.\n";
    }
}

// Updated main function to include the new menu options


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
    clearInputBuffer();    // Add this line

    try {
        auto schema = db.getTableSchema(tableName);

        std::string columnName;
        std::cout << "Enter column name to search: ";
        std::cin >> columnName;

        // Find the column type
        Column::Type colType = Column::INT;
        for (const auto& col : schema.columns) {
            if (col.name == columnName) {
                colType = col.type;
                break;
            }
        }

        std::cout << "Enter search value: ";
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
        case Column::STRING:
        case Column::CHAR: {
            clearInputBuffer();  // Add this to clear any remaining input first
            std::string val;
            std::getline(std::cin, val);
            searchValue = val;
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

        auto results = db.searchRecords(tableName, columnName, searchValue);
        std::cout << "\nFound " << results.size() << " records:\n";

        for (const auto& record : results) {
            for (const auto& [key, value] : record) {
                std::visit([](auto&& arg) {
                    std::cout << arg << " | ";
                    }, value);
            }
            std::cout << "\n";
        }
    }
    catch (...) {
        std::cout << "Error: Table or column not found.\n";
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
#include <iostream>
#include <filesystem>
#include <limits>
#include <conio.h> // For _kbhit() and _getch()
#include <windows.h> // For system("cls")

// Function to clear the screen
void clearScreen() {
    system("cls");
}

// Function to display centered text
void displayCentered(const std::string& text) {
    const int consoleWidth = 80; // Standard console width
    int padding = (consoleWidth - text.length()) / 2;
    if (padding < 0) padding = 0;
    std::cout << std::string(padding, ' ') << text << "\n";
}

// Function to display menu with selection
int showMenu(const std::vector<std::string>& options, int currentSelection) {
    const std::string normalArrow = ">>>>>>>>>>>>>>>";
    const std::string selectedArrow = ">>>>>>>>>>>>>>>>>>>>>>>>>>>";

    clearScreen();
    displayCentered("Simple Database Management System");
    std::cout << "\n";

    for (int i = 0; i < options.size(); i++) {
        if (i == currentSelection) {
            std::cout << " " << (i + 1) << ". " << options[i] << " " << selectedArrow << "\n";
        }
        else {
            std::cout << " " << (i + 1) << ". " << options[i] << " " << normalArrow << "\n";
        }
    }

    std::cout << "\nUse arrow keys to navigate or enter option number: ";
    return currentSelection;
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
    }
    else {
        std::cout << "Failed to drop table '" << tableName << "'.\n";
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
    }
    else {
        std::cout << "Failed to use database '" << dbName << "'.\n";
    }
}
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
void createDatabaseMenu(DatabaseManager& db) {
    std::string dbName;
    std::cout << "Enter database name to create: ";
    std::cin >> dbName;
    clearInputBuffer();

    if (db.createDatabase(dbName)) {
        std::cout << "Database '" << dbName << "' created successfully.\n";
    }
    else {
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
    }
    else {
        std::cout << "Failed to drop database '" << dbName << "'.\n";
    }
}
int main() {
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
            updateRecordsWithFilterMenu(db);
            break;
        case 4:
            deleteRecordsWithFilterMenu(db);
            break;
        case 5:
            displayRecordsMenu(db);
            break;
        case 6:
            searchRecordsWithFilterMenu(db);
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
