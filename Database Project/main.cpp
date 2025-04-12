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

        // Get all records by searching with empty condition
        auto results = db.searchRecords(tableName, "", FieldValue{});

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

    try {
        auto schema = db.getTableSchema(tableName);
        Record record;

        for (const auto& column : schema.columns) {
            std::cout << "Enter value for " << column.name << " (" << [&]() {
                switch (column.type) {
                case Column::INT: return "int";
                case Column::FLOAT: return "float";
                case Column::STRING: return "string";
                case Column::CHAR: return "char";
                case Column::BOOL: return "bool";
                default: return "unknown";
                }
                }() << "): ";

            switch (column.type) {
            case Column::INT: {
                int value;
                std::cin >> value;
                clearInputBuffer();
                record[column.name] = value;
                break;
            }
            case Column::FLOAT: {
                float value;
                std::cin >> value;
                clearInputBuffer();
                record[column.name] = value;
                break;
            }
            case Column::STRING: {
                std::string value;
                std::cin.ignore(); // Clear newline
                std::getline(std::cin, value);
                record[column.name] = value;
                break;
            }
            case Column::CHAR: {
                std::string value;
                std::cin >> value;
                if (value.length() > column.length) {
                    value = value.substr(0, column.length);
                }
                record[column.name] = value;
                break;
            }
            case Column::BOOL: {
                bool value;
                std::string input;
                std::cin >> input;
                value = (input == "true" || input == "1" || input == "y");
                record[column.name] = value;
                break;
            }
            }
        }

        if (db.insertRecord(tableName, record)) {
            std::cout << "Record inserted successfully!\n";
        }
        else {
            std::cout << "Failed to insert record.\n";
        }
    }
    catch (...) {
        std::cout << "Error: Table not found or invalid input.\n";
    }
}

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

int main() {
    // Ensure data directory exists
    try {
        std::filesystem::create_directories("data");
    }
    catch (const std::exception& e) {
        std::cerr << "Warning: Could not create data directory: " << e.what() << "\n";
    }

    DatabaseManager db;
    std::cout << "Simple Database Management System\n";

    while (true) {
        std::cout << "\nMain Menu:\n";
        std::cout << "1. Create Table\n";
        std::cout << "2. Insert Record\n";
        std::cout << "3. Search Records\n";
        std::cout << "4. List Tables\n";
        std::cout << "5. Display Table Data\n";
        std::cout << "6. Exit\n";
        std::cout << "Enter choice: ";

        int choice;
        std::cin >> choice;

        switch (choice) {
        case 1:
            createTableMenu(db);
            break;
        case 2:
            insertRecordMenu(db);
            break;
        case 3:
            searchRecordsMenu(db);
            break;
        case 4:
            listTablesMenu(db);
            break;
        case 5:
            displayTableData(db);
            break;
        case 6:
            return 0;
        default:
            std::cout << "Invalid choice. Please try again.\n";
        }

        // Clear any remaining input
        std::cin.clear();
        std::cin.ignore((std::numeric_limits<std::streamsize>::max)(), '\n');
    }

    return 0;
}