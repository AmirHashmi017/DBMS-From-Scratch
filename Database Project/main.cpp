#include "SimpleHttpServer.h"
#include "database_manager.h"
#include "query_parser.h"
#include <iostream>
#include <filesystem>
#include <limits>
#include <algorithm>
#include <iomanip>  
#include <variant> 
#include <map> 
#include <conio.h> // For _kbhit() and _getch()
#include <windows.h> // For system("cls")

using namespace std;
namespace fs = filesystem;

// Function declarations
bool getYesNoInput(const string& prompt);
int getNumericInput(const string& prompt);

void clearInputBuffer() {
    cin.clear();
    cin.ignore((numeric_limits<streamsize>::max)(), '\n');
}


// Add these functions to your main file

void searchRecordsWithFilterMenu(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        cout << "No tables exist. Please create a table first.\n";
        return;
    }

    cout << "Available tables:\n";
    for (const auto& table : tables) {
        cout << " - " << table << "\n";
    }

    string tableName;
    cout << "Enter table name: ";
    cin >> tableName;
    clearInputBuffer();

    TableSchema schema;
    try {
        schema = db.getTableSchema(tableName);
    }
    catch (...) {
        cout << "Error: Table not found.\n";
        return;
    }

    // Display available columns
    cout << "Available columns:\n";
    for (const auto& col : schema.columns) {
        cout << " - " << col.name << " (";
        switch (col.type) {
        case Column::INT: cout << "INT"; break;
        case Column::FLOAT: cout << "FLOAT"; break;
        case Column::STRING: cout << "STRING"; break;
        case Column::CHAR: cout << "CHAR"; break;
        case Column::BOOL: cout << "BOOL"; break;
        }
        cout << ")\n";
    }

    // Build conditions
    vector<tuple<string, string, FieldValue>> conditions;
    vector<string> operators;

    cout << "Enter number of conditions: ";
    int numConditions;
    cin >> numConditions;
    clearInputBuffer();

    for (int i = 0; i < numConditions; i++) {
        cout << "\nCondition " << (i + 1) << ":\n";

        // Get column
        string columnName;
        cout << "Enter column name: ";
        cin >> columnName;
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
            cout << "Column not found. Using default type INT.\n";
        }

        // Get operator
        cout << "Available operators: =, !=, >, <, >=, <=, LIKE\n";
        cout << "Enter operator: ";
        string op;
        cin >> op;
        clearInputBuffer();

        // Get value
        cout << "Enter value: ";
        FieldValue value;

        switch (colType) {
        case Column::INT: {
            int val;
            cin >> val;
            value = val;
            break;
        }
        case Column::FLOAT: {
            float val;
            cin >> val;
            value = val;
            break;
        }
                          // Update all the case blocks for STRING and CHAR in main.cpp like this:

        case Column::STRING:
        case Column::CHAR: {
            string val;
            getline(cin, val);
            value = val;
            break;
        }
        case Column::BOOL: {
            string val;
            cin >> val;
            bool boolVal = (val == "true" || val == "1" || val == "y" || val == "yes");
            value = boolVal;
            break;
        }
        }

        // Add condition
        conditions.push_back(make_tuple(columnName, op, value));

        // Add logical operator for the next condition if not the last one
        if (i < numConditions - 1) {
            cout << "Logical operator for next condition (AND/OR/NOT): ";
            string logicalOp;
            cin >> logicalOp;
            clearInputBuffer();
            operators.push_back(logicalOp);
        }
    }
    try {
        auto results = db.searchRecordsWithFilter(tableName, conditions, operators);

        // Display results with proper formatting
        cout << "\nFound " << results.size() << " records:\n";

        // Print column headers
        for (const auto& col : schema.columns) {
            cout << setw(20) << col.name << " | ";
        }
        cout << "\n" << string(schema.columns.size() * 22, '-') << "\n";

        // Print records with proper type handling
        for (const auto& record : results) {
            for (const auto& col : schema.columns) {
                if (record.find(col.name) != record.end()) {
                    visit([](auto&& arg) {
                        cout << setw(20) << arg << " | ";
                        }, record.at(col.name));
                }
                else {
                    cout << setw(20) << "NULL" << " | ";
                }
            }
            cout << "\n";
        }
    }
    catch (const exception& e) {
        cerr << "Error: " << e.what() << "\n";
    }
}


void updateRecordsWithFilterMenu(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        cout << "No tables exist. Please create a table first.\n";
        return;
    }

    cout << "Available tables:\n";
    for (const auto& table : tables) {
        cout << " - " << table << "\n";
    }

    string tableName;
    cout << "Enter table name: ";
    cin >> tableName;
    clearInputBuffer();

    TableSchema schema;
    try {
        schema = db.getTableSchema(tableName);
    }
    catch (...) {
        cout << "Error: Table not found.\n";
        return;
    }

    // Display available columns
    cout << "Available columns:\n";
    for (const auto& col : schema.columns) {
        cout << " - " << col.name << " (";
        switch (col.type) {
        case Column::INT: cout << "INT"; break;
        case Column::FLOAT: cout << "FLOAT"; break;
        case Column::STRING: cout << "STRING"; break;
        case Column::CHAR: cout << "CHAR"; break;
        case Column::BOOL: cout << "BOOL"; break;
        }
        cout << ")\n";
    }

    // Get update values
    map<string, FieldValue> updateValues;
    cout << "Enter number of fields to update: ";
    int numFields;
    cin >> numFields;
    clearInputBuffer();

    for (int i = 0; i < numFields; i++) {
        cout << "\nField " << (i + 1) << ":\n";

        // Get column
        string columnName;
        cout << "Enter column name: ";
        cin >> columnName;
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
            cout << "Column not found. Using default type INT.\n";
        }

        // Get value
        cout << "Enter new value: ";
        FieldValue value;

        switch (colType) {
        case Column::INT: {
            int val;
            cin >> val;
            value = val;
            break;
        }
        case Column::FLOAT: {
            float val;
            cin >> val;
            value = val;
            break;
        }
        case Column::STRING:
        case Column::CHAR: {
            string val;
            getline(cin, val);
            value = val;
            break;
        }
        case Column::BOOL: {
            string val;
            cin >> val;
            bool boolVal = (val == "true" || val == "1" || val == "y" || val == "yes");
            value = boolVal;
            break;
        }
        }

        updateValues[columnName] = value;
    }

    // Build conditions
    vector<tuple<string, string, FieldValue>> conditions;
    vector<string> operators;

    cout << "Enter number of conditions: ";
    int numConditions;
    cin >> numConditions;
    clearInputBuffer();

    for (int i = 0; i < numConditions; i++) {
        cout << "\nCondition " << (i + 1) << ":\n";

        // Get column
        string columnName;
        cout << "Enter column name: ";
        cin >> columnName;
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
            cout << "Column not found. Using default type INT.\n";
        }

        // Get operator
        cout << "Available operators: =, !=, >, <, >=, <=, LIKE\n";
        cout << "Enter operator: ";
        string op;
        cin >> op;
        clearInputBuffer();

        // Get value
        cout << "Enter value: ";
        FieldValue value;

        switch (colType) {
        case Column::INT: {
            int val;
            cin >> val;
            value = val;
            break;
        }
        case Column::FLOAT: {
            float val;
            cin >> val;
            value = val;
            break;
        }
        case Column::STRING:
        case Column::CHAR: {
            string val;
            getline(cin, val);
            value = val;
            break;
        }
        case Column::BOOL: {
            string val;
            cin >> val;
            bool boolVal = (val == "true" || val == "1" || val == "y" || val == "yes");
            value = boolVal;
            break;
        }
        }

        // Add condition
        conditions.push_back(make_tuple(columnName, op, value));

        // Add logical operator for the next condition if not the last one
        if (i < numConditions - 1) {
            cout << "Logical operator for next condition (AND/OR/NOT): ";
            string logicalOp;
            cin >> logicalOp;
            clearInputBuffer();
            operators.push_back(logicalOp);
        }
    }

    try {
        bool success = db.updateRecordsWithFilter(tableName, updateValues, conditions, operators);
        if (success) {
            cout << "Records updated successfully.\n";
        }
        else {
            cout << "Failed to update records.\n";
        }
    }
    catch (const exception& e) {
        cerr << "Error: " << e.what() << "\n";
    }
}

void deleteRecordsWithFilterMenu(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        cout << "No tables exist. Please create a table first.\n";
        return;
    }

    cout << "Available tables:\n";
    for (const auto& table : tables) {
        cout << " - " << table << "\n";
    }

    string tableName;
    cout << "Enter table name: ";
    cin >> tableName;
    clearInputBuffer();

    TableSchema schema;
    try {
        schema = db.getTableSchema(tableName);
    }
    catch (...) {
        cout << "Error: Table not found.\n";
        return;
    }

    // Display available columns
    cout << "Available columns:\n";
    for (const auto& col : schema.columns) {
        cout << " - " << col.name << " (";
        switch (col.type) {
        case Column::INT: cout << "INT"; break;
        case Column::FLOAT: cout << "FLOAT"; break;
        case Column::STRING: cout << "STRING"; break;
        case Column::CHAR: cout << "CHAR"; break;
        case Column::BOOL: cout << "BOOL"; break;
        }
        cout << ")\n";
    }

    // Build conditions
    vector<tuple<string, string, FieldValue>> conditions;
    vector<string> operators;

    cout << "Enter number of conditions: ";
    int numConditions;
    cin >> numConditions;
    clearInputBuffer();

    for (int i = 0; i < numConditions; i++) {
        cout << "\nCondition " << (i + 1) << ":\n";

        // Get column
        string columnName;
        cout << "Enter column name: ";
        cin >> columnName;
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
            cout << "Column not found. Using default type INT.\n";
        }

        // Get operator
        cout << "Available operators: =, !=, >, <, >=, <=, LIKE\n";
        cout << "Enter operator: ";
        string op;
        cin >> op;
        clearInputBuffer();

        // Get value
        cout << "Enter value: ";
        FieldValue value;

        switch (colType) {
        case Column::INT: {
            int val;
            cin >> val;
            value = val;
            break;
        }
        case Column::FLOAT: {
            float val;
            cin >> val;
            value = val;
            break;
        }
        case Column::STRING:
        case Column::CHAR: {
            string val;
            getline(cin, val);
            value = val;
            break;
        }
        case Column::BOOL: {
            string val;
            cin >> val;
            bool boolVal = (val == "true" || val == "1" || val == "y" || val == "yes");
            value = boolVal;
            break;
        }
        }

        // Add condition
        conditions.push_back(make_tuple(columnName, op, value));

        // Add logical operator for the next condition if not the last one
        if (i < numConditions - 1) {
            cout << "Logical operator for next condition (AND/OR/NOT): ";
            string logicalOp;
            cin >> logicalOp;
            clearInputBuffer();
            operators.push_back(logicalOp);
        }
    }

    // Confirm deletion
    if (!getYesNoInput("\nWARNING: This will delete all records matching your conditions.\nAre you sure you want to proceed?")) {
        cout << "Delete operation cancelled.\n";
        return;
    }

    // Perform delete
    int deletedCount = db.deleteRecordsWithFilter(tableName, conditions, operators);

    cout << "Deleted " << deletedCount << " records.\n";
}

// Function to display all records in a table
void displayTableData(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        cout << "No tables exist. Please create a table first.\n";
        return;
    }

    cout << "Available tables:\n";
    for (const auto& table : tables) {
        cout << " - " << table << "\n";
    }

    string tableName;
    cout << "Enter table name: ";
    cin >> tableName;
    clearInputBuffer();

    TableSchema schema;
    try {
        schema = db.getTableSchema(tableName);
    }
    catch (...) {
        cout << "Error: Table not found.\n";
        return;
    }

    // Get all records
    auto records = db.getAllRecords(tableName);

    cout << "\nTable: " << tableName << ", Total Records: " << records.size() << "\n";

    // Print column headers
    for (const auto& col : schema.columns) {
        cout << col.name << " | ";
    }
    cout << "\n";
    cout << string(80, '-') << "\n";

    // Print records
    for (const auto& record : records) {
        for (const auto& col : schema.columns) {
            if (record.find(col.name) != record.end()) {
                visit([](auto&& arg) {
                    cout << arg << " | ";
                    }, record.at(col.name));
            }
            else {
                cout << "(null) | ";
            }
        }
        cout << "\n";
    }
}

// Function to create a table
void createTableMenu(DatabaseManager& db) {
    string tableName;
    cout << "Enter table name: ";
    cin >> tableName;
    clearInputBuffer();

    // Check if table already exists
    auto existingTables = db.listTables();
    if (find(existingTables.begin(), existingTables.end(), tableName) != existingTables.end()) {
        cout << "Error: A table with the name '" << tableName << "' already exists.\n";
        cout << "Please choose a different table name.\n";
        return;
    }

    int numColumns;
    cout << "Enter number of columns: ";
    cin >> numColumns;
    clearInputBuffer();

    vector<tuple<string, string, int>> columns;
    string primaryKey;
    map<string, pair<string, string>> foreignKeys;

    for (int i = 0; i < numColumns; i++) {
        string colName, colType;
        int colLength = 0;

        cout << "\nColumn " << (i + 1) << ":\n";
        cout << "Name: ";
        cin >> colName;
        clearInputBuffer();

        cout << "Type (int, float, string, char, bool): ";
        cin >> colType;
        clearInputBuffer();

        if (colType == "string" || colType == "char") {
            cout << "Length: ";
            cin >> colLength;
            clearInputBuffer();
        }

        columns.push_back(make_tuple(colName, colType, colLength));

        // Only ask about primary key if one hasn't been set yet
        if (primaryKey.empty()) {
            if (getYesNoInput("Is this the primary key?")) {
                primaryKey = colName;
            }
        } else {
            cout << "Primary key already set to '" << primaryKey << "'. Skipping primary key question.\n";
        }

        if (getYesNoInput("Is this a foreign key?")) {
            string refTable, refColumn;
            cout << "Referenced table: ";
            cin >> refTable;
            clearInputBuffer();

            // Check if referenced table exists
            auto tables = db.listTables();
            if (find(tables.begin(), tables.end(), refTable) == tables.end()) {
                cout << "Error: Referenced table '" << refTable << "' does not exist.\n";
                cout << "Foreign key constraint will not be set.\n";
                continue;
            }

            cout << "Referenced column: ";
            cin >> refColumn;
            clearInputBuffer();

            // Check if referenced column exists in the table
            try {
                auto schema = db.getTableSchema(refTable);
                bool columnExists = false;
                for (const auto& col : schema.columns) {
                    if (col.name == refColumn) {
                        columnExists = true;
                        break;
                    }
                }

                if (!columnExists) {
                    cout << "Error: Referenced column '" << refColumn << "' does not exist in table '" << refTable << "'.\n";
                    cout << "Foreign key constraint will not be set.\n";
                    continue;
                }

                // Check if the referenced column is a primary key
                bool isPrimaryKey = false;
                for (const auto& col : schema.columns) {
                    if (col.name == refColumn && col.is_primary_key) {
                        isPrimaryKey = true;
                        break;
                    }
                }

                if (!isPrimaryKey) {
                    cout << "Error: Referenced column '" << refColumn << "' is not a primary key in table '" << refTable << "'.\n";
                    cout << "Foreign keys must reference primary keys.\n";
                    cout << "Foreign key constraint will not be set.\n";
                    continue;
                }

                // If all checks pass, add the foreign key
                foreignKeys[colName] = make_pair(refTable, refColumn);
                cout << "Foreign key constraint set successfully.\n";
            }
            catch (...) {
                cout << "Error: Could not verify foreign key constraint.\n";
                cout << "Foreign key constraint will not be set.\n";
            }
        }
    }

    if (primaryKey.empty()) {
        cout << "Warning: No primary key specified.\n";
        if (!getYesNoInput("Do you want to continue without a primary key?")) {
            cout << "Table creation cancelled.\n";
            return;
        }
    }

    bool success = db.createTable(tableName, columns, primaryKey, foreignKeys);
    if (success) {
        cout << "Table created successfully.\n";
    }
    else {
        cout << "Failed to create table.\n";
    }
}

// Function to insert a record
void insertRecordMenu(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        cout << "No tables exist. Please create a table first.\n";
        return;
    }

    cout << "Available tables:\n";
    for (const auto& table : tables) {
        cout << " - " << table << "\n";
    }

    string tableName;
    cout << "Enter table name: ";
    cin >> tableName;

    try {
        auto schema = db.getTableSchema(tableName);
        Record record;

        for (const auto& column : schema.columns) {
            cout << "Enter value for column '" << column.name << "' (";
            switch (column.type) {
            case Column::INT: cout << "INT"; break;
            case Column::FLOAT: cout << "FLOAT"; break;
            case Column::STRING: cout << "STRING"; break;
            case Column::CHAR: cout << "CHAR"; break;
            case Column::BOOL: cout << "BOOL"; break;
            }
            cout << "): ";

            FieldValue value;
            switch (column.type) {
            case Column::INT: {
                int val;
                cin >> val;
                value = val;
                break;
            }
            case Column::FLOAT: {
                float val;
                cin >> val;
                value = val;
                break;
            }
            case Column::STRING:
            case Column::CHAR: {
                string val;
                getline(cin, val);
                value = val;
                break;
            }

            case Column::BOOL: {
                string val;
                cin >> val;
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
            cout << "Record inserted successfully.\n";
        }
        else {
            cout << "Failed to insert record.\n";
        }
    }
    catch (...) {
        cout << "Error: Table not found.\n";
    }
}

// Updated main function to include the new menu options


void searchRecordsMenu(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        cout << "No tables exist. Please create a table first.\n";
        return;
    }

    cout << "Available tables:\n";
    for (const auto& table : tables) {
        cout << " - " << table << "\n";
    }

    string tableName;
    cout << "Enter table name: ";
    cin >> tableName;
    clearInputBuffer();    // Add this line

    try {
        auto schema = db.getTableSchema(tableName);

        string columnName;
        cout << "Enter column name to search: ";
        cin >> columnName;

        // Find the column type
        Column::Type colType = Column::INT;
        for (const auto& col : schema.columns) {
            if (col.name == columnName) {
                colType = col.type;
                break;
            }
        }

        cout << "Enter search value: ";
        FieldValue searchValue;

        switch (colType) {
        case Column::INT: {
            int value;
            cin >> value;
            searchValue = value;
            break;
        }
        case Column::FLOAT: {
            float value;
            cin >> value;
            searchValue = value;
            break;
        }
        case Column::STRING:
        case Column::CHAR: {
            clearInputBuffer();  // Add this to clear any remaining input first
            string val;
            getline(cin, val);
            searchValue = val;
            break;
        }
        case Column::BOOL: {
            bool value;
            string input;
            cin >> input;
            value = (input == "true" || input == "1" || input == "y");
            searchValue = value;
            break;
        }
        }

        auto results = db.searchRecords(tableName, columnName, searchValue);
        cout << "\nFound " << results.size() << " records:\n";

        for (const auto& record : results) {
            for (const auto& [key, value] : record) {
                visit([](auto&& arg) {
                    cout << arg << " | ";
                    }, value);
            }
            cout << "\n";
        }
    }
    catch (...) {
        cout << "Error: Table or column not found.\n";
    }
}

void listTablesMenu(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        cout << "No tables exist.\n";
        return;
    }

    cout << "Available tables:\n";
    for (const auto& table : tables) {
        auto schema = db.getTableSchema(table);
        cout << "\nTable: " << table << "\n";
        cout << "Columns:\n";

        for (const auto& column : schema.columns) {
            cout << " - " << column.name << " (";
            switch (column.type) {
            case Column::INT: cout << "INT"; break;
            case Column::FLOAT: cout << "FLOAT"; break;
            case Column::STRING: cout << "STRING(" << column.length << ")"; break;
            case Column::CHAR: cout << "CHAR(" << column.length << ")"; break;
            case Column::BOOL: cout << "BOOL"; break;
            }

            if (column.is_primary_key) cout << ", PRIMARY KEY";
            if (column.is_foreign_key) {
                cout << ", FOREIGN KEY REFERENCES " << column.references_table
                    << "(" << column.references_column << ")";
            }
            cout << ")\n";
        }
    }
}

// Function to clear the screen
void clearScreen() {
    system("cls");
}

// Function to display centered text
void displayCentered(const string& text) {
    const int consoleWidth = 80; // Standard console width
    int padding = (consoleWidth - text.length()) / 2;
    if (padding < 0) padding = 0;
    cout << string(padding, ' ') << text << "\n";
}

// Function to display menu with selection
int showMenu(const vector<string>& options, int currentSelection) {
    const string normalArrow = ">>>>>>>>>>>>>>>";
    const string selectedArrow = ">>>>>>>>>>>>>>>>>>>>>>>>>>>";

    clearScreen();
    displayCentered("Simple Database Management System");
    cout << "\n";

    for (int i = 0; i < options.size(); i++) {
        if (i == currentSelection) {
            cout << " " << (i + 1) << ". " << selectedArrow << " " << options[i] << "\n";
        }
        else {
            cout << " " << (i + 1) << ". " << normalArrow << " " << options[i] << "\n";
        }
    }

    cout << "\nUse arrow keys to navigate or enter option number: ";
    return currentSelection;
}

void dropTableMenu(DatabaseManager& db) {
    if (db.getCurrentDatabase().empty()) {
        cout << "No database selected. Please use a database first.\n";
        return;
    }

    auto tables = db.listTables();
    if (tables.empty()) {
        cout << "No tables exist in the current database.\n";
        return;
    }

    cout << "Available tables:\n";
    for (const auto& table : tables) {
        cout << " - " << table << "\n";
    }

    string tableName;
    cout << "Enter table name to drop: ";
    cin >> tableName;
    clearInputBuffer();

    if (db.dropTable(tableName)) {
        cout << "Table '" << tableName << "' dropped successfully.\n";
    }
    else {
        cout << "Failed to drop table '" << tableName << "'.\n";
    }
}
void useDatabaseMenu(DatabaseManager& db) {
    auto databases = db.listDatabases();
    if (databases.empty()) {
        cout << "No databases exist.\n";
        return;
    }

    cout << "Available databases:\n";
    for (const auto& dbName : databases) {
        cout << " - " << dbName << "\n";
    }

    string dbName;
    cout << "Enter database name to use: ";
    cin >> dbName;
    clearInputBuffer();

    if (db.useDatabase(dbName)) {
        cout << "Using database '" << dbName << "'.\n";
    }
    else {
        cout << "Failed to use database '" << dbName << "'.\n";
    }
}
void displayRecordsMenu(DatabaseManager& db) {
    auto tables = db.listTables();
    if (tables.empty()) {
        cout << "No tables exist. Please create a table first.\n";
        return;
    }

    cout << "Available tables:\n";
    for (const auto& table : tables) {
        cout << " - " << table << "\n";
    }

    string tableName;
    cout << "Enter table name: ";
    cin >> tableName;
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
        cout << "Error: Table '" << tableName << "' not found.\n";
        return;
    }

    try {
        auto schema = db.getTableSchema(tableName);
        auto records = db.getAllRecords(tableName);

        if (records.empty()) {
            cout << "No records found in table '" << tableName << "'.\n";
            return;
        }

        // Display column headers
        cout << "\nTable: " << tableName << "\n";
        cout << string(80, '-') << "\n";
        for (const auto& col : schema.columns) {
            cout << left << setw(15) << col.name << " | ";
        }
        cout << "\n" << string(80, '-') << "\n";

        // Display records
        for (const auto& record : records) {
            for (const auto& col : schema.columns) {
                if (record.find(col.name) != record.end()) {
                    cout << left << setw(15);
                    if (holds_alternative<int>(record.at(col.name))) {
                        cout << get<int>(record.at(col.name));
                    }
                    else if (holds_alternative<float>(record.at(col.name))) {
                        cout << get<float>(record.at(col.name));
                    }
                    else if (holds_alternative<string>(record.at(col.name))) {
                        cout << get<string>(record.at(col.name));
                    }
                    else if (holds_alternative<bool>(record.at(col.name))) {
                        cout << (get<bool>(record.at(col.name)) ? "true" : "false");
                    }
                    cout << " | ";
                }
                else {
                    cout << left << setw(15) << "NULL" << " | ";
                }
            }
            cout << "\n";
        }
        cout << string(80, '-') << "\n";
    }
    catch (const exception& e) {
        cout << "Error: " << e.what() << "\n";
    }
}

void listDatabasesMenu(DatabaseManager& db) {
    auto databases = db.listDatabases();
    if (databases.empty()) {
        cout << "No databases exist.\n";
        return;
    }

    cout << "Available databases:\n";
    for (const auto& dbName : databases) {
        cout << " - " << dbName;
        if (dbName == db.getCurrentDatabase()) {
            cout << " (current)";
        }
        cout << "\n";
    }
}
void createDatabaseMenu(DatabaseManager& db) {
    string dbName;
    cout << "Enter database name to create: ";
    cin >> dbName;
    clearInputBuffer();

    if (db.createDatabase(dbName)) {
        cout << "Database '" << dbName << "' created successfully.\n";
    }
    else {
        cout << "Failed to create database '" << dbName << "'.\n";
    }
}
void dropDatabaseMenu(DatabaseManager& db) {
    auto databases = db.listDatabases();
    if (databases.empty()) {
        cout << "No databases exist.\n";
        return;
    }

    cout << "Available databases:\n";
    for (const auto& dbName : databases) {
        cout << " - " << dbName << "\n";
    }

    string dbName;
    cout << "Enter database name to drop: ";
    cin >> dbName;
    clearInputBuffer();

    if (db.dropDatabase(dbName)) {
        cout << "Database '" << dbName << "' dropped successfully.\n";
    }
    else {
        cout << "Failed to drop database '" << dbName << "'.\n";
    }
}

void executeQuery(DatabaseManager& db_manager, QueryParser& parser) {
    clearScreen();
    cout << "Query Executor (Type 'exit' to return to main menu)\n";
    cout << "Type 'help' for available commands\n\n";

    string query;
    while (true) {
        cout << "DBMS> ";
        getline(cin, query);

        if (query == "exit") {
            break;
        } else if (query == "help") {
            cout << "\nAvailable Commands:\n";
            cout << "CREATE DATABASE database_name\n";
            cout << "DROP DATABASE database_name\n";
            cout << "USE database_name\n";
            cout << "SHOW DATABASES\n";
            cout << "CREATE TABLE table_name (column1 type, column2 type, ...)\n";
            cout << "DROP TABLE table_name\n";
            cout << "SHOW TABLES\n";
            cout << "INSERT INTO table_name VALUES (value1, value2, ...)\n";
            cout << "SELECT * FROM table_name [WHERE condition]\n";
            cout << "UPDATE table_name SET column = value [WHERE condition]\n";
            cout << "DELETE FROM table_name [WHERE condition]\n\n";
        } else if (!query.empty()) {
            if (parser.parse(query)) {
                if (parser.execute()) {
                    cout << "Query executed successfully.\n";
                } else {
                    cout << "Error executing query.\n";
                }
            } else {
                cout << "Invalid query syntax.\n";
            }
        }
    }
}

// Function definitions
bool getYesNoInput(const string& prompt) {
    while (true) {
        cout << prompt << " (y/n): ";
        string input;
        cin >> input;
        clearInputBuffer();
        
        // Convert to lowercase for case-insensitive comparison
        transform(input.begin(), input.end(), input.begin(), ::tolower);
        
        if (input == "y" || input == "yes") {
            return true;
        }
        if (input == "n" || input == "no") {
            return false;
        }
        
        cout << "Invalid input. Please enter 'y' or 'n'.\n";
    }
}

int getNumericInput(const string& prompt) {
    int value;
    while (true) {
        cout << prompt;
        if (cin >> value) {
            clearInputBuffer();
            return value;
        }
        cin.clear();
        clearInputBuffer();
        cout << "Invalid input. Please enter a number.\n";
    }
}

int main(int argc, char* argv[]) {
    try {
        // Initialize database manager
        DatabaseManager dbManager("catalog.bin");
        
        // If command line argument is provided, execute it and exit
        if (argc > 1) {
            string query = argv[1];
            QueryParser parser(dbManager);
            if (parser.parse(query)) {
                if (parser.execute()) {
                    cout << "Query executed successfully.\n";
                } else {
                    cerr << "Error executing query.\n";
                }
            } else {
                cerr << "Invalid query syntax.\n";
            }
            return 0;
        }
        
        // Otherwise start HTTP server
        SimpleHttpServer server(dbManager, "127.0.0.1", 8080);
        server.start();
        
        cout << "Press Enter to exit..." << endl;
        cin.get();
        
        server.stop();
    }
    catch (const exception& e) {
        cerr << "Error: " << e.what() << endl;
        return 1;
    }
    
    return 0;
}
