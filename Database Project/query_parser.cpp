#include "query_parser.h"
#include <algorithm>
#include <sstream>
#include <cctype>
#include <stdexcept>
#include <iostream>

QueryParser::QueryParser(DatabaseManager& db_manager) : db_manager(db_manager) {}

bool QueryParser::parse(const std::string& query_string) {
    // Clear any existing commands
    commands.clear();
    
    // Split the query into individual commands by semicolon
    std::string current_command;
    bool in_quotes = false;
    
    // Clean up the query string
    std::string cleaned_query = query_string;
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
    for (const auto& cmd : commands) {
        std::vector<std::string> tokens = tokenize(cmd);
        if (tokens.empty()) continue;

        // Convert first token to uppercase for case-insensitive comparison
        std::string command = tokens[0];
        std::transform(command.begin(), command.end(), command.begin(), ::toupper);

        if (command == "CREATE") {
            if (tokens.size() < 2) return false;
            std::string object = tokens[1];
            std::transform(object.begin(), object.end(), object.begin(), ::toupper);
            
            if (object == "DATABASE") {
                current_query.type = QueryType::CREATE_DATABASE;
                all_success &= parseCreateDatabase(tokens);
            } else if (object == "TABLE") {
                current_query.type = QueryType::CREATE_TABLE;
                all_success &= parseCreateTable(tokens);
            }
        } else if (command == "DROP") {
            if (tokens.size() < 2) return false;
            std::string object = tokens[1];
            std::transform(object.begin(), object.end(), object.begin(), ::toupper);
            
            if (object == "DATABASE") {
                current_query.type = QueryType::DROP_DATABASE;
                all_success &= parseDropDatabase(tokens);
            } else if (object == "TABLE") {
                current_query.type = QueryType::DROP_TABLE;
                all_success &= parseDropTable(tokens);
            }
        } else if (command == "USE") {
            current_query.type = QueryType::USE_DATABASE;
            all_success &= parseUseDatabase(tokens);
        } else if (command == "SHOW") {
            if (tokens.size() < 2) return false;
            std::string object = tokens[1];
            std::transform(object.begin(), object.end(), object.begin(), ::toupper);
            
            if (object == "DATABASES") {
                current_query.type = QueryType::SHOW_DATABASES;
                all_success &= true;
            } else if (object == "TABLES") {
                current_query.type = QueryType::SHOW_TABLES;
                all_success &= true;
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
        }
    }
    
    return all_success;
}

bool QueryParser::execute() {
    bool success = true;

    for (const auto& cmd : commands) {
        std::vector<std::string> tokens = tokenize(cmd);
        if (tokens.empty()) continue;

        // Convert first token to uppercase for case-insensitive comparison
        std::string command = tokens[0];
        std::transform(command.begin(), command.end(), command.begin(), ::toupper);

        if (command == "CREATE") {
            if (tokens.size() < 2) return false;
            std::string object = tokens[1];
            std::transform(object.begin(), object.end(), object.begin(), ::toupper);
            
            if (object == "DATABASE") {
                current_query.type = QueryType::CREATE_DATABASE;
                if (!parseCreateDatabase(tokens)) return false;
                success &= db_manager.createDatabase(current_query.database_name);
            } else if (object == "TABLE") {
                current_query.type = QueryType::CREATE_TABLE;
                if (!parseCreateTable(tokens)) return false;
                std::cout << "Executing createTable with primary_key: '" << current_query.primary_key << "'" << std::endl;
                success &= db_manager.createTable(
                    current_query.table_name,
                    current_query.columns,
                    current_query.primary_key,
                    current_query.foreign_keys
                );
            }
        } else if (command == "DROP") {
            if (tokens.size() < 2) return false;
            std::string object = tokens[1];
            std::transform(object.begin(), object.end(), object.begin(), ::toupper);
            
            if (object == "DATABASE") {
                current_query.type = QueryType::DROP_DATABASE;
                if (!parseDropDatabase(tokens)) return false;
                success &= db_manager.dropDatabase(current_query.database_name);
            } else if (object == "TABLE") {
                current_query.type = QueryType::DROP_TABLE;
                if (!parseDropTable(tokens)) return false;
                success &= db_manager.dropTable(current_query.table_name);
            }
        } else if (command == "USE") {
            current_query.type = QueryType::USE_DATABASE;
            if (!parseUseDatabase(tokens)) return false;
            success &= db_manager.useDatabase(current_query.database_name);
        } else if (command == "SHOW") {
            if (tokens.size() < 2) return false;
            std::string object = tokens[1];
            std::transform(object.begin(), object.end(), object.begin(), ::toupper);
            
            if (object == "DATABASES") {
                current_query.type = QueryType::SHOW_DATABASES;
                auto databases = db_manager.listDatabases();
                for (const auto& db : databases) {
                    std::cout << db << std::endl;
                }
                success &= true;
            } else if (object == "TABLES") {
                current_query.type = QueryType::SHOW_TABLES;
                auto tables = db_manager.listTables();
                for (const auto& table : tables) {
                    std::cout << table << std::endl;
                }
                success &= true;
            }
        } else if (command == "INSERT") {
            current_query.type = QueryType::INSERT;
            if (!parseInsert(tokens)) return false;
            Record record;
            for (const auto& [key, value] : current_query.values) {
                record[key] = value;
            }
            success &= db_manager.insertRecord(current_query.table_name, record);
        } else if (command == "SELECT") {
    current_query.type = QueryType::SELECT;
    if (!parseSelect(tokens)) return false;
    std::vector<Record> results;
    if (current_query.conditions.empty()) {
        results = db_manager.getAllRecords(current_query.table_name);
    } else {
        std::vector<std::tuple<std::string, std::string, FieldValue>> conditions;
        std::vector<std::string> operators;
        
        for (const auto& cond : current_query.conditions) {
            conditions.push_back(std::make_tuple(cond.column, cond.op, cond.value));
        }
        operators = current_query.condition_operators;
        
        results = db_manager.searchRecordsWithFilter(
            current_query.table_name,
            conditions,
            operators
        );
    }
    
    // Get table schema to validate columns
    TableSchema schema = db_manager.getTableSchema(current_query.table_name);
    if (schema.name.empty()) {
        std::cerr << "Error: Table '" << current_query.table_name << "' does not exist" << std::endl;
        return false;
    }
    
    // Prepare columns to display
    std::vector<std::string> columns_to_display;
    if (current_query.select_columns.size() == 1 && current_query.select_columns[0] == "*") {
        for (const auto& col : schema.columns) {
            columns_to_display.push_back(col.name);
        }
    } else {
        columns_to_display = current_query.select_columns;
    }
    
    // Print only the selected columns
    for (const auto& record : results) {
        bool first = true;
        for (const auto& col : columns_to_display) {
            if (record.find(col) != record.end()) {
                if (!first) std::cout << ", ";
                std::cout << col << ": ";
                std::visit([](auto&& arg) { std::cout << arg; }, record.at(col));
                first = false;
            }
        }
        std::cout << std::endl;
    }
    success &= true;
} else if (command == "UPDATE") {
            current_query.type = QueryType::UPDATE;
            if (!parseUpdate(tokens)) return false;
            std::vector<std::tuple<std::string, std::string, FieldValue>> conditions;
            std::vector<std::string> operators;
            
            for (const auto& cond : current_query.conditions) {
                conditions.push_back(std::make_tuple(cond.column, cond.op, cond.value));
                operators.push_back(cond.op == "NOT" ? "NOT" : "AND");
            }
            
            success &= db_manager.updateRecordsWithFilter(
                current_query.table_name,
                current_query.values,
                conditions,
                operators
            );
        } else if (command == "DELETE") {
            current_query.type = QueryType::DELETE_OP;
            if (!parseDelete(tokens)) return false;
            std::vector<std::tuple<std::string, std::string, FieldValue>> conditions;
            std::vector<std::string> operators;
            
            for (const auto& cond : current_query.conditions) {
                conditions.push_back(std::make_tuple(cond.column, cond.op, cond.value));
                operators.push_back(cond.op == "NOT" ? "NOT" : "AND");
            }
            
            success &= db_manager.deleteRecordsWithFilter(
                current_query.table_name,
                conditions,
                operators
            ) >= 0;
        }
    }
    
    return success;
}

// Parsing methods
bool QueryParser::parseCreateDatabase(const std::vector<std::string>& tokens) {
    if (tokens.size() != 3) {
        std::cerr << "Error: Invalid CREATE DATABASE syntax" << std::endl;
        return false;
    }
    current_query.database_name = tokens[2];
    return true;
}

bool QueryParser::parseDropDatabase(const std::vector<std::string>& tokens) {
    if (tokens.size() != 3) {
        std::cerr << "Error: Invalid DROP DATABASE syntax" << std::endl;
        return false;
    }
    current_query.database_name = tokens[2];
    return true;
}

bool QueryParser::parseUseDatabase(const std::vector<std::string>& tokens) {
    if (tokens.size() != 2) {
        std::cerr << "Error: Invalid USE DATABASE syntax" << std::endl;
        return false;
    }
    current_query.database_name = tokens[1];
    return true;
}

bool QueryParser::parseCreateTable(const std::vector<std::string>& tokens) {
    if (tokens.size() < 4) {
        std::cerr << "Error: Invalid CREATE TABLE syntax" << std::endl;
        return false;
    }
    
    current_query.type = QueryType::CREATE_TABLE;
    current_query.table_name = tokens[2];
    
    // Parse column definitions and constraints
    std::vector<std::tuple<std::string, std::string, int>> columns;
    std::string primary_key;
    std::map<std::string, std::pair<std::string, std::string>> foreign_keys;
    
    // Find the opening parenthesis
    size_t i = 3;
    while (i < tokens.size() && tokens[i] != "(") {
        i++;
    }
    if (i >= tokens.size()) {
        std::cerr << "Error: Expected '(' after table name" << std::endl;
        return false;
    }
    i++; // Skip the opening parenthesis
    
    std::string current_col_name;
    std::string current_col_type;
    int current_col_length = 0; // Default to 0 (no length specified)
    bool is_primary_key = false;
    
    // Debug: Print tokens for inspection
    std::cout << "Tokens for CREATE TABLE: ";
    for (const auto& token : tokens) {
        std::cout << "'" << token << "' ";
    }
    std::cout << std::endl;
    
    while (i < tokens.size() && tokens[i] != ")") {
        std::string token = tokens[i];
        if (token.empty()) {
            i++;
            continue;
        }
        
        std::string token_upper = token;
        std::transform(token_upper.begin(), token_upper.end(), token_upper.begin(), ::toupper);
        
        // Handle PRIMARY KEY declaration
        if (token_upper == "PRIMARY" && i + 3 < tokens.size() && 
            tokens[i + 1] == "KEY" && tokens[i + 2] == "(") {
            if (tokens[i + 3] == ")") {
                std::cerr << "Error: PRIMARY KEY column name missing" << std::endl;
                return false;
            }
            primary_key = tokens[i + 3];
            std::cout << "Found PRIMARY KEY: " << primary_key << std::endl;
            i += 5; // Skip PRIMARY KEY (column_name)
            if (i < tokens.size() && tokens[i] == ",") {
                i++; // Skip comma if present
            }
            continue;
        }
        
        // Handle FOREIGN KEY constraint
        if (token_upper == "FOREIGN" && i + 6 < tokens.size() && 
            tokens[i + 1] == "KEY" && tokens[i + 2] == "(" && tokens[i + 4] == ")" && 
            tokens[i + 5] == "REFERENCES") {
            std::string local_column = tokens[i + 3];
            std::string ref_table = tokens[i + 6];
            std::string ref_column;
            
            if (i + 9 < tokens.size() && tokens[i + 7] == "(" && tokens[i + 9] == ")") {
                ref_column = tokens[i + 8];
                i += 10; // Skip FOREIGN KEY (col) REFERENCES table(col)
            } else {
                ref_column = local_column; // Default to same column name
                i += 7; // Skip FOREIGN KEY (col) REFERENCES table
            }
            
            foreign_keys[local_column] = std::make_pair(ref_table, ref_column);
            if (i < tokens.size() && tokens[i] == ",") {
                i++; // Skip comma if present
            }
            continue;
        }
        
        // Handle column definition
        if (current_col_name.empty()) {
            current_col_name = token;
            i++;
            continue;
        }
        
        if (current_col_type.empty()) {
            current_col_type = token_upper;
            // Handle types with length specifications (STRING, CHAR)
            if ((current_col_type == "STRING" || current_col_type == "CHAR") && 
                i + 3 < tokens.size() && tokens[i + 1] == "(" && tokens[i + 3] == ")") {
                try {
                    current_col_length = std::stoi(tokens[i + 2]);
                    i += 4; // Skip type ( length )
                } catch (...) {
                    std::cerr << "Error: Invalid length for " << current_col_type << std::endl;
                    return false;
                }
            } else {
                i++; // Skip type
            }
            
            // Look ahead for PRIMARY KEY or comma
            if (i < tokens.size()) {
                std::string next_token = tokens[i];
                std::transform(next_token.begin(), next_token.end(), next_token.begin(), ::toupper);
                
                if (next_token == "PRIMARY" && i + 1 < tokens.size() && tokens[i + 1] == "KEY") {
                    is_primary_key = true;
                    i += 2; // Skip PRIMARY KEY
                }
                
                if (i < tokens.size() && (tokens[i] == "," || tokens[i] == ")")) {
                    // End of column definition
                    columns.push_back(std::make_tuple(current_col_name, current_col_type, current_col_length));
                    if (is_primary_key) {
                        primary_key = current_col_name;
                        is_primary_key = false;
                    }
                    std::cout << "Added column: " << current_col_name << " " << current_col_type 
                              << (current_col_length > 0 ? "(" + std::to_string(current_col_length) + ")" : "") << std::endl;
                    current_col_name.clear();
                    current_col_type.clear();
                    current_col_length = 0;
                    if (tokens[i] == ",") {
                        i++; // Skip comma
                    }
                }
            }
            continue;
        }
        
        i++;
    }
    
    // Add the last column if it exists and hasn't been added
    if (!current_col_name.empty() && !current_col_type.empty()) {
        columns.push_back(std::make_tuple(current_col_name, current_col_type, current_col_length));
        if (is_primary_key) {
            primary_key = current_col_name;
        }
        std::cout << "Added final column: " << current_col_name << " " << current_col_type 
                  << (current_col_length > 0 ? "(" + std::to_string(current_col_length) + ")" : "") << std::endl;
    }
    
    if (columns.empty()) {
        std::cerr << "Error: No columns defined for table" << std::endl;
        return false;
    }
    
    // Validate primary key
    bool pk_found = false;
    for (const auto& col : columns) {
        if (std::get<0>(col) == primary_key) {
            pk_found = true;
            break;
        }
    }
    if (!primary_key.empty() && !pk_found) {
        std::cerr << "Error: Primary key column '" << primary_key << "' not found in column definitions" << std::endl;
        return false;
    }
    
    current_query.columns = columns;
    current_query.primary_key = primary_key;
    current_query.foreign_keys = foreign_keys;
    
    // Debug output
    std::cout << "Parsed CREATE TABLE command:" << std::endl;
    std::cout << "Table name: " << current_query.table_name << std::endl;
    std::cout << "Primary key: " << primary_key << std::endl;
    std::cout << "Columns:" << std::endl;
    for (const auto& col : columns) {
        std::cout << "  " << std::get<0>(col) << " " << std::get<1>(col);
        if (std::get<2>(col) > 0) {
            std::cout << "(" << std::get<2>(col) << ")";
        }
        std::cout << std::endl;
    }
    std::cout << "Foreign keys: " << foreign_keys.size() << std::endl;
    
    return true;
}

bool QueryParser::parseDropTable(const std::vector<std::string>& tokens) {
    if (tokens.size() != 3) {
        std::cerr << "Error: Invalid DROP TABLE syntax" << std::endl;
        return false;
    }
    current_query.table_name = tokens[2];
    return true;
}

bool QueryParser::parseInsert(const std::vector<std::string>& tokens) {
    if (tokens.size() < 6) {
        std::cerr << "Error: Invalid INSERT syntax" << std::endl;
        return false;
    }

    current_query.type = QueryType::INSERT;
    current_query.table_name = tokens[2];

    // Parse values
    std::map<std::string, FieldValue> values;
    int value_index = 0;

    // Get table schema to map values to column names
    TableSchema schema = db_manager.getTableSchema(current_query.table_name);
    if (schema.name.empty()) {
        std::cerr << "Error: Table '" << current_query.table_name << "' does not exist" << std::endl;
        return false;
    }

    // Start parsing after VALUES keyword
    size_t i = 4; // Should point to '(' after VALUES
    if (tokens[i] != "(") {
        std::cerr << "Error: Expected '(' after VALUES" << std::endl;
        return false;
    }
    i++; // Skip '('

    std::vector<std::string> value_tokens;
    bool in_quotes = false;
    std::string current_token;

    // Collect value tokens, preserving quoted strings
    while (i < tokens.size() && tokens[i] != ")") {
        std::string token = tokens[i];

        if (token == "," && !in_quotes) {
            if (!current_token.empty()) {
                value_tokens.push_back(current_token);
                current_token.clear();
            }
            i++;
            continue;
        }

        if (token == "'" && !in_quotes) {
            in_quotes = true;
            current_token += token;
            i++;
            continue;
        }

        if (token == "'" && in_quotes) {
            in_quotes = false;
            current_token += token;
            i++;
            continue;
        }

        current_token += token;
        if (!in_quotes && i + 1 < tokens.size() && tokens[i + 1] != "," && tokens[i + 1] != ")") {
            current_token += " ";
        }
        i++;
    }

    if (!current_token.empty()) {
        value_tokens.push_back(current_token);
    }

    if (i >= tokens.size() || tokens[i] != ")") {
        std::cerr << "Error: Expected ')' after values" << std::endl;
        return false;
    }

    // Debug: Print value tokens
    std::cout << "Value tokens: ";
    for (const auto& vt : value_tokens) {
        std::cout << "'" << vt << "' ";
    }
    std::cout << std::endl;

    // Parse each value token
    for (const auto& token : value_tokens) {
        if (value_index >= schema.columns.size()) {
            std::cerr << "Error: Too many values for table '" << current_query.table_name << "'" << std::endl;
            return false;
        }

        const std::string& column_name = schema.columns[value_index].name;
        Column::Type column_type = schema.columns[value_index].type;

        try {
            switch (column_type) {
            case Column::INT: {
                int int_val = std::stoi(token);
                values[column_name] = int_val;
                break;
            }
            case Column::FLOAT: {
                float float_val = std::stof(token);
                values[column_name] = float_val;
                break;
            }
            case Column::STRING:
            case Column::CHAR: {
                std::string str_val = token;
                if (str_val.front() == '\'' && str_val.back() == '\'') {
                    str_val = str_val.substr(1, str_val.length() - 2);
                }
                values[column_name] = str_val;
                break;
            }
            case Column::BOOL: {
                bool bool_val = (token == "true" || token == "TRUE" || token == "1");
                values[column_name] = bool_val;
                break;
            }
            }
            value_index++;
        }
        catch (...) {
            std::cerr << "Error: Invalid value '" << token << "' for column '" << column_name << "'" << std::endl;
            return false;
        }
    }

    if (value_index != schema.columns.size()) {
        std::cerr << "Error: Incorrect number of values for table '" << current_query.table_name << "'" << std::endl;
        return false;
    }

    current_query.values = values;
    return true;
}


bool QueryParser::parseSelect(const std::vector<std::string>& tokens) {
    if (tokens.size() < 4) {
        std::cerr << "Error: Invalid SELECT syntax" << std::endl;
        return false;
    }
    
    current_query.type = QueryType::SELECT;
    
    // Parse column list
    size_t from_pos = std::find(tokens.begin(), tokens.end(), "FROM") - tokens.begin();
    if (from_pos == tokens.size()) {
        std::cerr << "Error: Missing FROM clause" << std::endl;
        return false;
    }
    
    // Parse columns (between SELECT and FROM)
    std::vector<std::string> columns;
    for (size_t i = 1; i < from_pos; i++) {
        std::string col = tokens[i];
        col.erase(std::remove(col.begin(), col.end(), ','), col.end());
        if (!col.empty()) {
            columns.push_back(col);
        }
    }
    if (columns.empty()) {
        columns.push_back("*");
    }
    
    // Validate columns against table schema
    current_query.table_name = tokens[from_pos + 1];
    TableSchema schema = db_manager.getTableSchema(current_query.table_name);
    if (schema.name.empty()) {
        std::cerr << "Error: Table '" << current_query.table_name << "' does not exist" << std::endl;
        return false;
    }
    
    if (columns[0] != "*") {
        for (const auto& col : columns) {
            bool found = false;
            for (const auto& schema_col : schema.columns) {
                if (schema_col.name == col) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                std::cerr << "Error: Column '" << col << "' does not exist in table '" << current_query.table_name << "'" << std::endl;
                return false;
            }
        }
    }
    
    current_query.select_columns = columns;
    
    // Parse WHERE conditions if present
    size_t where_pos = std::find(tokens.begin(), tokens.end(), "WHERE") - tokens.begin();
    if (where_pos < tokens.size()) {
        current_query.conditions.clear();
        current_query.condition_operators.clear();
        
        for (size_t i = where_pos + 1; i < tokens.size(); ) {
            std::string token = tokens[i];
            std::transform(token.begin(), token.end(), token.begin(), ::toupper);
            
            if (token == "AND" || token == "OR" || token == "NOT") {
                current_query.condition_operators.push_back(token);
                std::cerr << "Parsed operator: " << token << std::endl;
                i++;
                continue;
            }
            
            if (i + 2 >= tokens.size()) {
                std::cerr << "Error: Incomplete WHERE condition" << std::endl;
                return false;
            }
            
            Condition cond;
            cond.column = tokens[i];
            cond.op = tokens[i + 1];
            cond.value = parseValue(tokens[i + 2]);
            current_query.conditions.push_back(cond);
            std::cerr << "Parsed condition: " << cond.column << " " << cond.op << " ";
            std::visit([](auto&& arg) { std::cerr << arg; }, cond.value);
            std::cerr << std::endl;
            i += 3;
        }
        
        // Debug: Print all operators
        std::cerr << "All condition operators: ";
        for (const auto& op : current_query.condition_operators) {
            std::cerr << "'" << op << "' ";
        }
        std::cerr << std::endl;
        
        // Validate operator count
        size_t expected_ops = current_query.conditions.size() - 1;
        size_t not_count = std::count(current_query.condition_operators.begin(), 
                                     current_query.condition_operators.end(), "NOT");
        if (current_query.condition_operators.size() < expected_ops || 
            current_query.condition_operators.size() > expected_ops + not_count) {
            std::cerr << "Error: Mismatched operators (" << current_query.condition_operators.size() 
                      << ") for conditions (" << current_query.conditions.size() << ")" << std::endl;
            return false;
        }
    }
    
    return true;
}

bool QueryParser::parseUpdate(const std::vector<std::string>& tokens) {
    if (tokens.size() < 6) {
        std::cerr << "Error: Invalid UPDATE syntax" << std::endl;
        return false;
    }
    
    current_query.type = QueryType::UPDATE;
    current_query.table_name = tokens[1];
    
    // Find SET keyword
    size_t set_pos = std::find(tokens.begin(), tokens.end(), "SET") - tokens.begin();
    if (set_pos == tokens.size()) {
        std::cerr << "Error: Missing SET clause" << std::endl;
        return false;
    }
    
    // Parse SET assignments
    std::map<std::string, FieldValue> values;
    for (size_t i = set_pos + 1; i < tokens.size(); i++) {
        if (tokens[i] == "WHERE") break;
        if (i + 2 >= tokens.size() || tokens[i + 1] != "=") continue;
        
        values[tokens[i]] = parseValue(tokens[i + 2]);
        i += 2;
    }
    current_query.values = values;
    
    // Parse WHERE conditions
    size_t where_pos = std::find(tokens.begin(), tokens.end(), "WHERE") - tokens.begin();
    if (where_pos < tokens.size()) {
        std::vector<Condition> conditions;
        std::vector<std::string> operators;
        
        for (size_t i = where_pos + 1; i < tokens.size(); i++) {
            if (i + 2 >= tokens.size()) break;
            
            std::string token = tokens[i];
            std::transform(token.begin(), token.end(), token.begin(), ::toupper);
            
            if (token == "AND" || token == "OR" || token == "NOT") {
                operators.push_back(token);
                continue;
            }
            
            Condition cond;
            cond.column = tokens[i];
            cond.op = tokens[i + 1];
            cond.value = parseValue(tokens[i + 2]);
            conditions.push_back(cond);
            i += 2;
        }
        
        current_query.conditions = conditions;
        current_query.condition_operators = operators;
    }
    
    return true;
}

bool QueryParser::parseDelete(const std::vector<std::string>& tokens) {
    if (tokens.size() < 3) {
        std::cerr << "Error: Invalid DELETE syntax" << std::endl;
        return false;
    }
    
    current_query.type = QueryType::DELETE_OP;
    current_query.table_name = tokens[2];
    
    // Parse WHERE conditions
    size_t where_pos = std::find(tokens.begin(), tokens.end(), "WHERE") - tokens.begin();
    if (where_pos < tokens.size()) {
        std::vector<Condition> conditions;
        std::vector<std::string> operators;
        
        for (size_t i = where_pos + 1; i < tokens.size(); i++) {
            if (i + 2 >= tokens.size()) break;
            
            std::string token = tokens[i];
            std::transform(token.begin(), token.end(), token.begin(), ::toupper);
            
            if (token == "AND" || token == "OR" || token == "NOT") {
                operators.push_back(token);
                continue;
            }
            
            Condition cond;
            cond.column = tokens[i];
            cond.op = tokens[i + 1];
            cond.value = parseValue(tokens[i + 2]);
            conditions.push_back(cond);
            i += 2;
        }
        
        current_query.conditions = conditions;
        current_query.condition_operators = operators;
    }
    
    return true;
}

// Helper methods
std::vector<std::string> QueryParser::tokenize(const std::string& query) {
    std::vector<std::string> tokens;
    std::string current_token;
    bool in_quotes = false;
    
    // Clean up the query string first
    std::string cleaned_query = query;
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
    
    // Enhanced tokenization to handle delimiters
    for (size_t i = 0; i < cleaned_query.length(); ) {
        char c = cleaned_query[i];
        
        if (c == '\'') {
            in_quotes = !in_quotes;
            current_token += c;
            i++;
        } else if (!in_quotes && (c == '(' || c == ')' || c == ',' || c == ';')) {
            if (!current_token.empty()) {
                tokens.push_back(current_token);
                current_token.clear();
            }
            tokens.push_back(std::string(1, c));
            i++;
        } else if (!in_quotes && isspace(c)) {
            if (!current_token.empty()) {
                tokens.push_back(current_token);
                current_token.clear();
            }
            i++;
        } else {
            current_token += c;
            i++;
        }
    }
    
    if (!current_token.empty()) {
        tokens.push_back(current_token);
    }
    
    // Debug: Print tokens
    std::cout << "Tokenized query: ";
    for (const auto& token : tokens) {
        std::cout << "'" << token << "' ";
    }
    std::cout << std::endl;
    
    return tokens;
}

FieldValue QueryParser::parseValue(const std::string& value_str) {
    // Try to parse as int
    try {
        return std::stoi(value_str);
    } catch (...) {}
    
    // Try to parse as float
    try {
        return std::stof(value_str);
    } catch (...) {}
    
    // Check for boolean
    if (value_str == "true" || value_str == "TRUE") return true;
    if (value_str == "false" || value_str == "FALSE") return false;
    
    // Treat as string (remove quotes if present)
    std::string str = value_str;
    if (str.front() == '\'' && str.back() == '\'') {
        str = str.substr(1, str.length() - 2);
    }
    return str;
}

Column::Type QueryParser::parseColumnType(const std::string& type_str) {
    std::string type = type_str;
    std::transform(type.begin(), type.end(), type.begin(), ::toupper);
    
    if (type == "INT") return Column::INT;
    if (type == "FLOAT") return Column::FLOAT;
    if (type == "STRING") return Column::STRING;
    if (type == "CHAR") return Column::CHAR;
    if (type == "BOOL") return Column::BOOL;
    
    throw std::runtime_error("Unknown column type: " + type_str);
}