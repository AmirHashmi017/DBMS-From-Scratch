#include "query_parser.h"
#include <algorithm>
#include <sstream>
#include <cctype>
#include <stdexcept>
#include <iostream>

using namespace std;

QueryParser::QueryParser(DatabaseManager& db_manager) : db_manager(db_manager) {}

bool QueryParser::parse(const string& query_string) {
    // Clear any existing commands
    commands.clear();
    
    // Split the query into individual commands by semicolon
    string current_command;
    bool in_quotes = false;
    
    // Clean up the query string
    string cleaned_query = query_string;
    // Replace all types of newlines and whitespace sequences with a single space
    for (size_t i = 0; i < cleaned_query.length(); ) {
        if (cleaned_query[i] == '\r' || cleaned_query[i] == '\n' || cleaned_query[i] == '\t') {
            cleaned_query[i] = ' ';
            i++;
        } else if (cleaned_query[i] == ' ' && i + 1 < cleaned_query.length() && cleaned_query[i + 1] == ' ') {
            cleaned_query.erase(i, 1);
        } else {
            i++;
        }
    }
    
    // Trim leading/trailing spaces
    while (!cleaned_query.empty() && cleaned_query[0] == ' ') {
        cleaned_query.erase(0, 1);
    }
    while (!cleaned_query.empty() && cleaned_query.back() == ' ') {
        cleaned_query.pop_back();
    }
    
    for (char c : cleaned_query) {
        if (c == '\'') {
            in_quotes = !in_quotes;
            current_command += c;
        } else if (c == ';' && !in_quotes) {
            if (!current_command.empty()) {
                commands.push_back(current_command);
                current_command.clear();
            }
        } else {
            current_command += c;
        }
    }
    
    // Add the last command if there is one
    if (!current_command.empty()) {
        commands.push_back(current_command);
    }
    
    // Process each command
    bool all_success = true;
    current_query.error_message.clear(); // Clear at start of parse
    for (const auto& cmd : commands) {
        vector<string> tokens = tokenize(cmd);
        if (tokens.empty()) continue;

        // Convert first token to uppercase for case-insensitive comparison
        string command = tokens[0];
        transform(command.begin(), command.end(), command.begin(), ::toupper);

        if (command == "CREATE") {
            if (tokens.size() < 2) {
                current_query.error_message = "Invalid CREATE syntax: missing object type";
                return false;
            }
            string object = tokens[1];
            transform(object.begin(), object.end(), object.begin(), ::toupper);
            
            if (object == "DATABASE") {
                current_query.type = QueryType::CREATE_DATABASE;
                all_success &= parseCreateDatabase(tokens);
            } else if (object == "TABLE") {
                current_query.type = QueryType::CREATE_TABLE;
                all_success &= parseCreateTable(tokens);
            } else {
                current_query.error_message = "Invalid CREATE syntax: unknown object '" + object + "'";
                return false;
            }
        } else if (command == "DROP") {
            if (tokens.size() < 2) {
                current_query.error_message = "Invalid DROP syntax: missing object type";
                return false;
            }
            string object = tokens[1];
            transform(object.begin(), object.end(), object.begin(), ::toupper);
            
            if (object == "DATABASE") {
                current_query.type = QueryType::DROP_DATABASE;
                all_success &= parseDropDatabase(tokens);
            } else if (object == "TABLE") {
                current_query.type = QueryType::DROP_TABLE;
                all_success &= parseDropTable(tokens);
            } else {
                current_query.error_message = "Invalid DROP syntax: unknown object '" + object + "'";
                return false;
            }
        } else if (command == "USE") {
            current_query.type = QueryType::USE_DATABASE;
            all_success &= parseUseDatabase(tokens);
        } else if (command == "SHOW") {
            if (tokens.size() < 2) {
                current_query.error_message = "Invalid SHOW syntax: missing object type";
                return false;
            }
            string object = tokens[1];
            transform(object.begin(), object.end(), object.begin(), ::toupper);
            
            if (object == "DATABASES") {
                current_query.type = QueryType::SHOW_DATABASES;
                all_success &= true;
            } else if (object == "TABLES") {
                current_query.type = QueryType::SHOW_TABLES;
                all_success &= true;
            } else {
                current_query.error_message = "Invalid SHOW syntax: unknown object '" + object + "'";
                return false;
            }
        } else if (command == "INSERT") {
            current_query.type = QueryType::INSERT;
            all_success &= parseInsert(tokens);
        } else if (command == "SELECT") {
            current_query.type = QueryType::SELECT;
            all_success &= parseSelect(tokens);
        } else if (command == "UPDATE") {
            current_query.type = QueryType::UPDATE;
            all_success &= parseUpdate(tokens);
        } else if (command == "DELETE") {
            current_query.type = QueryType::DELETE_OP;
            all_success &= parseDelete(tokens);
        } else {
            current_query.error_message = "Unknown command: '" + command + "'";
            return false;
        }
    }
    
    return all_success;
}

bool QueryParser::execute() {
    bool success = true;
    vector<Record> results;
    int records_found = 0;

    for (const auto& cmd : commands) {
        vector<string> tokens = tokenize(cmd);
        if (tokens.empty()) continue;

        // Convert first token to uppercase for case-insensitive comparison
        string command = tokens[0];
        transform(command.begin(), command.end(), command.begin(), ::toupper);

        // Clear results for this command, but preserve error_message if already set
        results.clear();
        records_found = 0;

        if (command == "CREATE") {
            if (tokens.size() < 2) {
                current_query.error_message = "Invalid CREATE command syntax";
                return false;
            }
            string object = tokens[1];
            transform(object.begin(), object.end(), object.begin(), ::toupper);
            
            if (object == "DATABASE") {
                current_query.type = QueryType::CREATE_DATABASE;
                if (!parseCreateDatabase(tokens)) {
                    current_query.error_message = "Failed to parse CREATE DATABASE command";
                    return false;
                }
                success &= db_manager.createDatabase(current_query.database_name);
                if (!success) {
                    current_query.error_message = "Failed to create database '" + current_query.database_name + "'";
                }
            } else if (object == "TABLE") {
                current_query.type = QueryType::CREATE_TABLE;
                if (!parseCreateTable(tokens)) {
                    current_query.error_message = "Failed to parse CREATE TABLE command";
                    return false;
                }
                success &= db_manager.createTable(
                    current_query.table_name,
                    current_query.columns,
                    current_query.primary_key,
                    current_query.foreign_keys
                );
                if (!success) {
                    current_query.error_message = "Failed to create table '" + current_query.table_name + "'";
                }
            }
        } else if (command == "DROP") {
            if (tokens.size() < 2) {
                current_query.error_message = "Invalid DROP command syntax";
                return false;
            }
            string object = tokens[1];
            transform(object.begin(), object.end(), object.begin(), ::toupper);
            
            if (object == "DATABASE") {
                current_query.type = QueryType::DROP_DATABASE;
                if (!parseDropDatabase(tokens)) {
                    current_query.error_message = "Failed to parse DROP DATABASE command";
                    return false;
                }
                success &= db_manager.dropDatabase(current_query.database_name);
                if (!success) {
                    current_query.error_message = "Failed to drop database '" + current_query.database_name + "'";
                }
            } else if (object == "TABLE") {
                current_query.type = QueryType::DROP_TABLE;
                if (!parseDropTable(tokens)) {
                    current_query.error_message = "Failed to parse DROP TABLE command";
                    return false;
                }
                success &= db_manager.dropTable(current_query.table_name);
                if (!success) {
                    current_query.error_message = "Failed to drop table '" + current_query.table_name + "'";
                }
            }
        } else if (command == "USE") {
            current_query.type = QueryType::USE_DATABASE;
            if (!parseUseDatabase(tokens)) {
                current_query.error_message = "Failed to parse USE DATABASE command";
                return false;
            }
            success &= db_manager.useDatabase(current_query.database_name);
            if (!success) {
                current_query.error_message = "Failed to use database '" + current_query.database_name + "'";
            }
        } else if (command == "SHOW") {
            if (tokens.size() < 2) {
                current_query.error_message = "Invalid SHOW command syntax";
                return false;
            }
            string object = tokens[1];
            transform(object.begin(), object.end(), object.begin(), ::toupper);
            
            if (object == "DATABASES") {
                current_query.type = QueryType::SHOW_DATABASES;
                auto databases = db_manager.listDatabases();
                for (const auto& db : databases) {
                    Record record;
                    record["name"] = db;
                    results.push_back(record);
                }
                records_found = results.size();
            } else if (object == "TABLES") {
                current_query.type = QueryType::SHOW_TABLES;
                auto tables = db_manager.listTables();
                for (const auto& table : tables) {
                    Record record;
                    record["name"] = table;
                    results.push_back(record);
                }
                records_found = results.size();
            }
        } else if (command == "INSERT") {
            current_query.type = QueryType::INSERT;
            if (!parseInsert(tokens)) {
                current_query.error_message = "Failed to parse INSERT command";
                return false;
            }
            success &= db_manager.insertRecord(current_query.table_name, current_query.record);
            if (!success) {
                current_query.error_message = "Failed to insert record into table '" + current_query.table_name + "'";
            }
        } else if (command == "SELECT") {
            current_query.type = QueryType::SELECT;
            if (!parseSelect(tokens)) {
                current_query.error_message = "Failed to parse SELECT command";
                return false;
            }
            if (current_query.join_condition.column.empty()) {
                // Simple SELECT
                if (current_query.where_conditions.empty()) {
                    results = db_manager.getAllRecords(current_query.table_name);
                } else {
                    results = db_manager.searchRecordsWithFilter(
                        current_query.table_name,
                        current_query.where_conditions,
                        current_query.where_operators
                    );
                }
            } else {
                // JOIN
                results = db_manager.joinTables(
                    current_query.table_name,
                    current_query.join_table,
                    current_query.join_condition,
                    current_query.where_conditions,
                    current_query.where_operators
                );
            }
            records_found = results.size();
        } else if (command == "UPDATE") {
            current_query.type = QueryType::UPDATE;
            if (!parseUpdate(tokens)) {
                current_query.error_message = "Failed to parse UPDATE command";
                return false;
            }
            success &= db_manager.updateRecordsWithFilter(
                current_query.table_name,
                current_query.update_values,
                current_query.where_conditions,
                current_query.where_operators
            );
            if (!success) {
                current_query.error_message = "Failed to update records in table '" + current_query.table_name + "'";
            }
        } else if (command == "DELETE") {
            current_query.type = QueryType::DELETE_OP;
            if (!parseDelete(tokens)) {
                current_query.error_message = "Failed to parse DELETE command";
                return false;
            }
            int deleted = db_manager.deleteRecordsWithFilter(
                current_query.table_name,
                current_query.where_conditions,
                current_query.where_operators
            );
            if (deleted < 0) {
                current_query.error_message = "Failed to delete records from table '" + current_query.table_name + "'";
                success = false;
            } else {
                records_found = deleted;
            }
        }
    }

    // Store results in current_query
    current_query.results = results;
    current_query.records_found = records_found;
    return success;
}

vector<string> QueryParser::tokenize(const string& input) {
    vector<string> tokens;
    string current_token;
    bool in_quotes = false;
    bool in_parentheses = false;
    
    for (size_t i = 0; i < input.length(); i++) {
        char c = input[i];
        
        if (c == '\'') {
            in_quotes = !in_quotes;
            current_token += c;
        } else if (c == '(') {
            in_parentheses = true;
            current_token += c;
        } else if (c == ')') {
            in_parentheses = false;
            current_token += c;
        } else if (c == ' ' && !in_quotes && !in_parentheses) {
            if (!current_token.empty()) {
                tokens.push_back(current_token);
                current_token.clear();
            }
        } else {
            current_token += c;
        }
    }
    
    if (!current_token.empty()) {
        tokens.push_back(current_token);
    }
    
    return tokens;
}

bool QueryParser::parseCreateDatabase(const vector<string>& tokens) {
    if (tokens.size() < 3) {
        current_query.error_message = "Invalid CREATE DATABASE syntax: missing database name";
        return false;
    }
    
    current_query.database_name = tokens[2];
    return true;
}

bool QueryParser::parseDropDatabase(const vector<string>& tokens) {
    if (tokens.size() < 3) {
        current_query.error_message = "Invalid DROP DATABASE syntax: missing database name";
        return false;
    }
    
    current_query.database_name = tokens[2];
    return true;
}

bool QueryParser::parseUseDatabase(const vector<string>& tokens) {
    if (tokens.size() < 3) {
        current_query.error_message = "Invalid USE DATABASE syntax: missing database name";
        return false;
    }
    
    current_query.database_name = tokens[2];
    return true;
}

bool QueryParser::parseCreateTable(const vector<string>& tokens) {
    if (tokens.size() < 4) {
        current_query.error_message = "Invalid CREATE TABLE syntax: missing table name or columns";
        return false;
    }
    
    current_query.table_name = tokens[2];
    current_query.columns.clear();
    current_query.primary_key.clear();
    current_query.foreign_keys.clear();
    
    // Parse columns
    size_t i = 3;
    while (i < tokens.size()) {
        if (tokens[i] == "PRIMARY" && i + 2 < tokens.size() && tokens[i + 1] == "KEY") {
            current_query.primary_key = tokens[i + 2];
            i += 3;
        } else if (tokens[i] == "FOREIGN" && i + 4 < tokens.size() && tokens[i + 1] == "KEY") {
            string fk_col = tokens[i + 2];
            if (i + 5 < tokens.size() && tokens[i + 3] == "REFERENCES") {
                string ref_table = tokens[i + 4];
                string ref_col = tokens[i + 5];
                current_query.foreign_keys[fk_col] = make_pair(ref_table, ref_col);
                i += 6;
            } else {
                current_query.error_message = "Invalid FOREIGN KEY syntax";
                return false;
            }
        } else {
            // Parse column definition
            if (i + 2 >= tokens.size()) {
                current_query.error_message = "Invalid column definition";
                return false;
            }
            
            string col_name = tokens[i];
            string col_type = tokens[i + 1];
            int col_length = 0;
            
            // Check for length specification
            if (i + 2 < tokens.size() && tokens[i + 2][0] == '(') {
                string length_str = tokens[i + 2].substr(1, tokens[i + 2].length() - 2);
                try {
                    col_length = stoi(length_str);
                } catch (const exception& e) {
                    current_query.error_message = "Invalid column length";
                    return false;
                }
                i += 3;
            } else {
                i += 2;
            }
            
            current_query.columns.push_back(make_tuple(col_name, col_type, col_length));
        }
    }
    
    return true;
}

bool QueryParser::parseDropTable(const vector<string>& tokens) {
    if (tokens.size() < 3) {
        current_query.error_message = "Invalid DROP TABLE syntax: missing table name";
        return false;
    }
    
    current_query.table_name = tokens[2];
    return true;
}

bool QueryParser::parseInsert(const vector<string>& tokens) {
    if (tokens.size() < 4) {
        current_query.error_message = "Invalid INSERT syntax: missing table name or values";
        return false;
    }
    
    current_query.table_name = tokens[2];
    current_query.record.clear();
    
    // Parse values
    size_t i = 3;
    while (i < tokens.size()) {
        if (tokens[i] == "VALUES") {
            i++;
            while (i < tokens.size()) {
                string value = tokens[i];
                if (value[0] == '\'') {
                    // String value
                    value = value.substr(1, value.length() - 2);
                    current_query.record[tokens[i - 1]] = value;
                } else if (value == "true" || value == "false") {
                    // Boolean value
                    current_query.record[tokens[i - 1]] = (value == "true");
                } else if (value.find('.') != string::npos) {
                    // Float value
                    try {
                        float f = stof(value);
                        current_query.record[tokens[i - 1]] = f;
                    } catch (const exception& e) {
                        current_query.error_message = "Invalid float value: " + value;
                        return false;
                    }
                } else {
                    // Integer value
                    try {
                        int n = stoi(value);
                        current_query.record[tokens[i - 1]] = n;
                    } catch (const exception& e) {
                        current_query.error_message = "Invalid integer value: " + value;
                        return false;
                    }
                }
                i += 2;
            }
        } else {
            i++;
        }
    }
    
    return true;
}

bool QueryParser::parseSelect(const vector<string>& tokens) {
    if (tokens.size() < 4) {
        current_query.error_message = "Invalid SELECT syntax: missing table name or columns";
        return false;
    }
    
    current_query.table_name = tokens[3];
    current_query.columns.clear();
    current_query.where_conditions.clear();
    current_query.where_operators.clear();
    current_query.join_condition = Condition();
    current_query.join_table.clear();
    
    // Parse columns
    size_t i = 1;
    while (i < tokens.size() && tokens[i] != "FROM") {
        if (tokens[i] != ",") {
            current_query.columns.push_back(tokens[i]);
        }
        i++;
    }
    
    // Parse FROM and JOIN
    if (i + 1 < tokens.size() && tokens[i] == "FROM") {
        i++;
        while (i < tokens.size()) {
            if (tokens[i] == "JOIN") {
                if (i + 3 >= tokens.size()) {
                    current_query.error_message = "Invalid JOIN syntax";
                    return false;
                }
                current_query.join_table = tokens[i + 1];
                if (tokens[i + 2] != "ON") {
                    current_query.error_message = "Invalid JOIN syntax: missing ON";
                    return false;
                }
                current_query.join_condition.column = tokens[i + 3];
                if (i + 5 < tokens.size() && tokens[i + 4] == "=") {
                    current_query.join_condition.value = tokens[i + 5];
                } else {
                    current_query.error_message = "Invalid JOIN condition";
                    return false;
                }
                i += 6;
            } else if (tokens[i] == "WHERE") {
                break;
            } else {
                i++;
            }
        }
    }
    
    // Parse WHERE conditions
    if (i < tokens.size() && tokens[i] == "WHERE") {
        i++;
        while (i < tokens.size()) {
            if (tokens[i] == "AND" || tokens[i] == "OR" || tokens[i] == "NOT") {
                current_query.where_operators.push_back(tokens[i]);
                i++;
            } else {
                string column = tokens[i];
                string op = tokens[i + 1];
                string value = tokens[i + 2];
                
                FieldValue field_value;
                if (value[0] == '\'') {
                    // String value
                    value = value.substr(1, value.length() - 2);
                    field_value = value;
                } else if (value == "true" || value == "false") {
                    // Boolean value
                    field_value = (value == "true");
                } else if (value.find('.') != string::npos) {
                    // Float value
                    try {
                        float f = stof(value);
                        field_value = f;
                    } catch (const exception& e) {
                        current_query.error_message = "Invalid float value: " + value;
                        return false;
                    }
                } else {
                    // Integer value
                    try {
                        int n = stoi(value);
                        field_value = n;
                    } catch (const exception& e) {
                        current_query.error_message = "Invalid integer value: " + value;
                        return false;
                    }
                }
                
                current_query.where_conditions.push_back(make_tuple(column, op, field_value));
                i += 3;
            }
        }
    }
    
    return true;
}

bool QueryParser::parseUpdate(const vector<string>& tokens) {
    if (tokens.size() < 4) {
        current_query.error_message = "Invalid UPDATE syntax: missing table name or SET clause";
        return false;
    }
    
    current_query.table_name = tokens[1];
    current_query.update_values.clear();
    current_query.where_conditions.clear();
    current_query.where_operators.clear();
    
    // Parse SET clause
    size_t i = 2;
    while (i < tokens.size() && tokens[i] != "WHERE") {
        if (tokens[i] == "SET") {
            i++;
            while (i < tokens.size() && tokens[i] != "WHERE") {
                string column = tokens[i];
                if (i + 2 >= tokens.size()) {
                    current_query.error_message = "Invalid SET syntax";
                    return false;
                }
                if (tokens[i + 1] != "=") {
                    current_query.error_message = "Invalid SET syntax: missing =";
                    return false;
                }
                string value = tokens[i + 2];
                
                FieldValue field_value;
                if (value[0] == '\'') {
                    // String value
                    value = value.substr(1, value.length() - 2);
                    field_value = value;
                } else if (value == "true" || value == "false") {
                    // Boolean value
                    field_value = (value == "true");
                } else if (value.find('.') != string::npos) {
                    // Float value
                    try {
                        float f = stof(value);
                        field_value = f;
                    } catch (const exception& e) {
                        current_query.error_message = "Invalid float value: " + value;
                        return false;
                    }
                } else {
                    // Integer value
                    try {
                        int n = stoi(value);
                        field_value = n;
                    } catch (const exception& e) {
                        current_query.error_message = "Invalid integer value: " + value;
                        return false;
                    }
                }
                
                current_query.update_values[column] = field_value;
                i += 3;
                
                if (i < tokens.size() && tokens[i] == ",") {
                    i++;
                }
            }
        } else {
            i++;
        }
    }
    
    // Parse WHERE conditions
    if (i < tokens.size() && tokens[i] == "WHERE") {
        i++;
        while (i < tokens.size()) {
            if (tokens[i] == "AND" || tokens[i] == "OR" || tokens[i] == "NOT") {
                current_query.where_operators.push_back(tokens[i]);
                i++;
            } else {
                string column = tokens[i];
                string op = tokens[i + 1];
                string value = tokens[i + 2];
                
                FieldValue field_value;
                if (value[0] == '\'') {
                    // String value
                    value = value.substr(1, value.length() - 2);
                    field_value = value;
                } else if (value == "true" || value == "false") {
                    // Boolean value
                    field_value = (value == "true");
                } else if (value.find('.') != string::npos) {
                    // Float value
                    try {
                        float f = stof(value);
                        field_value = f;
                    } catch (const exception& e) {
                        current_query.error_message = "Invalid float value: " + value;
                        return false;
                    }
                } else {
                    // Integer value
                    try {
                        int n = stoi(value);
                        field_value = n;
                    } catch (const exception& e) {
                        current_query.error_message = "Invalid integer value: " + value;
                        return false;
                    }
                }
                
                current_query.where_conditions.push_back(make_tuple(column, op, field_value));
                i += 3;
            }
        }
    }
    
    return true;
}

bool QueryParser::parseDelete(const vector<string>& tokens) {
    if (tokens.size() < 3) {
        current_query.error_message = "Invalid DELETE syntax: missing table name";
        return false;
    }
    
    current_query.table_name = tokens[2];
    current_query.where_conditions.clear();
    current_query.where_operators.clear();
    
    // Parse WHERE conditions
    size_t i = 3;
    if (i < tokens.size() && tokens[i] == "WHERE") {
        i++;
        while (i < tokens.size()) {
            if (tokens[i] == "AND" || tokens[i] == "OR" || tokens[i] == "NOT") {
                current_query.where_operators.push_back(tokens[i]);
                i++;
            } else {
                string column = tokens[i];
                string op = tokens[i + 1];
                string value = tokens[i + 2];
                
                FieldValue field_value;
                if (value[0] == '\'') {
                    // String value
                    value = value.substr(1, value.length() - 2);
                    field_value = value;
                } else if (value == "true" || value == "false") {
                    // Boolean value
                    field_value = (value == "true");
                } else if (value.find('.') != string::npos) {
                    // Float value
                    try {
                        float f = stof(value);
                        field_value = f;
                    } catch (const exception& e) {
                        current_query.error_message = "Invalid float value: " + value;
                        return false;
                    }
                } else {
                    // Integer value
                    try {
                        int n = stoi(value);
                        field_value = n;
                    } catch (const exception& e) {
                        current_query.error_message = "Invalid integer value: " + value;
                        return false;
                    }
                }
                
                current_query.where_conditions.push_back(make_tuple(column, op, field_value));
                i += 3;
            }
        }
    }
    
    return true;
}