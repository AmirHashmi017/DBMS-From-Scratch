#include "query_parser.h"
#include <algorithm>
#include <sstream>
#include <cctype>
#include <stdexcept>

QueryParser::QueryParser(DatabaseManager& db_manager) : db_manager(db_manager) {}

bool QueryParser::parse(const std::string& query_string) {
    std::vector<std::string> tokens = tokenize(query_string);
    if (tokens.empty()) return false;

    // Convert first token to uppercase for case-insensitive comparison
    std::string command = tokens[0];
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);

    if (command == "CREATE") {
        if (tokens.size() < 2) return false;
        std::string object = tokens[1];
        std::transform(object.begin(), object.end(), object.begin(), ::toupper);
        
        if (object == "DATABASE") {
            current_query.type = QueryType::CREATE_DATABASE;
            return parseCreateDatabase(tokens);
        } else if (object == "TABLE") {
            current_query.type = QueryType::CREATE_TABLE;
            return parseCreateTable(tokens);
        }
    } else if (command == "DROP") {
        if (tokens.size() < 2) return false;
        std::string object = tokens[1];
        std::transform(object.begin(), object.end(), object.begin(), ::toupper);
        
        if (object == "DATABASE") {
            current_query.type = QueryType::DROP_DATABASE;
            return parseDropDatabase(tokens);
        } else if (object == "TABLE") {
            current_query.type = QueryType::DROP_TABLE;
            return parseDropTable(tokens);
        }
    } else if (command == "USE") {
        current_query.type = QueryType::USE_DATABASE;
        return parseUseDatabase(tokens);
    } else if (command == "SHOW") {
        if (tokens.size() < 2) return false;
        std::string object = tokens[1];
        std::transform(object.begin(), object.end(), object.begin(), ::toupper);
        
        if (object == "DATABASES") {
            current_query.type = QueryType::SHOW_DATABASES;
            return true;
        } else if (object == "TABLES") {
            current_query.type = QueryType::SHOW_TABLES;
            return true;
        }
    } else if (command == "INSERT") {
        current_query.type = QueryType::INSERT;
        return parseInsert(tokens);
    } else if (command == "SELECT") {
        current_query.type = QueryType::SELECT;
        return parseSelect(tokens);
    } else if (command == "UPDATE") {
        current_query.type = QueryType::UPDATE;
        return parseUpdate(tokens);
    } else if (command == "DELETE") {
        current_query.type = QueryType::DELETE_OP;
        return parseDelete(tokens);
    }

    return false;
}

bool QueryParser::execute() {
    switch (current_query.type) {
        case QueryType::CREATE_DATABASE:
            return db_manager.createDatabase(current_query.database_name);
        case QueryType::DROP_DATABASE:
            return db_manager.dropDatabase(current_query.database_name);
        case QueryType::USE_DATABASE:
            return db_manager.useDatabase(current_query.database_name);
        case QueryType::SHOW_DATABASES: {
            auto databases = db_manager.listDatabases();
            for (const auto& db : databases) {
                std::cout << db << std::endl;
            }
            return true;
        }
        case QueryType::CREATE_TABLE:
            return db_manager.createTable(
                current_query.table_name,
                current_query.columns,
                current_query.primary_key,
                current_query.foreign_keys
            );
        case QueryType::DROP_TABLE:
            return db_manager.dropTable(current_query.table_name);
        case QueryType::SHOW_TABLES: {
            auto tables = db_manager.listTables();
            for (const auto& table : tables) {
                std::cout << table << std::endl;
            }
            return true;
        }
        case QueryType::INSERT: {
            Record record;
            for (const auto& [key, value] : current_query.values) {
                record[key] = value;
            }
            return db_manager.insertRecord(current_query.table_name, record);
        }
        case QueryType::SELECT: {
            std::vector<Record> results;
            if (current_query.conditions.empty()) {
                results = db_manager.getAllRecords(current_query.table_name);
            } else {
                std::vector<std::tuple<std::string, std::string, FieldValue>> conditions;
                std::vector<std::string> operators;
                
                for (const auto& cond : current_query.conditions) {
                    conditions.push_back(std::make_tuple(cond.column, cond.op, cond.value));
                    operators.push_back("AND"); // Default to AND for now
                }
                
                results = db_manager.searchRecordsWithFilter(
                    current_query.table_name,
                    conditions,
                    operators
                );
            }
            
            // Display results
            for (const auto& record : results) {
                for (const auto& [key, value] : record) {
                    std::cout << key << ": ";
                    std::visit([](auto&& arg) { std::cout << arg; }, value);
                    std::cout << " ";
                }
                std::cout << std::endl;
            }
            return true;
        }
        case QueryType::UPDATE: {
            std::vector<std::tuple<std::string, std::string, FieldValue>> conditions;
            std::vector<std::string> operators;
            
            for (const auto& cond : current_query.conditions) {
                conditions.push_back(std::make_tuple(cond.column, cond.op, cond.value));
                operators.push_back("AND");
            }
            
            return db_manager.updateRecordsWithFilter(
                current_query.table_name,
                current_query.values,
                conditions,
                operators
            );
        }
        case QueryType::DELETE_OP: {
            std::vector<std::tuple<std::string, std::string, FieldValue>> conditions;
            std::vector<std::string> operators;
            
            for (const auto& cond : current_query.conditions) {
                conditions.push_back(std::make_tuple(cond.column, cond.op, cond.value));
                operators.push_back("AND");
            }
            
            return db_manager.deleteRecordsWithFilter(
                current_query.table_name,
                conditions,
                operators
            ) > 0;
        }
        default:
            return false;
    }
}

// Helper method implementations
bool QueryParser::parseCreateDatabase(const std::vector<std::string>& tokens) {
    if (tokens.size() != 3) return false;
    current_query.database_name = tokens[2];
    return true;
}

bool QueryParser::parseDropDatabase(const std::vector<std::string>& tokens) {
    if (tokens.size() != 3) return false;
    current_query.database_name = tokens[2];
    return true;
}

bool QueryParser::parseUseDatabase(const std::vector<std::string>& tokens) {
    if (tokens.size() != 2) return false;
    current_query.database_name = tokens[1];
    return true;
}

bool QueryParser::parseCreateTable(const std::vector<std::string>& tokens) {
    if (tokens.size() < 4) return false; // Minimum: CREATE TABLE name (columns...)
    
    current_query.type = QueryType::CREATE_TABLE;
    current_query.table_name = tokens[2];
    
    // Parse column definitions
    std::vector<std::tuple<std::string, std::string, int>> columns;
    std::string current_col_name;
    std::string current_col_type;
    int current_col_length = 0;
    bool in_parentheses = false;
    bool reading_name = true;
    
    for (size_t i = 3; i < tokens.size(); i++) {
        std::string token = tokens[i];
        
        if (token == "(") {
            in_parentheses = true;
            continue;
        }
        if (token == ")") {
            in_parentheses = false;
            continue;
        }
        
        if (in_parentheses) {
            if (reading_name) {
                current_col_name = token;
                reading_name = false;
            } else {
                current_col_type = token;
                if (current_col_type == "STRING" || current_col_type == "CHAR") {
                    // Look for length specification
                    if (i + 2 < tokens.size() && tokens[i+1] == "(" && tokens[i+2] != ")") {
                        try {
                            current_col_length = std::stoi(tokens[i+2]);
                            i += 2; // Skip the length and closing parenthesis
                        } catch (...) {
                            return false;
                        }
                    }
                }
                columns.push_back(std::make_tuple(current_col_name, current_col_type, current_col_length));
                reading_name = true;
                current_col_length = 0;
            }
        }
    }
    
    current_query.columns = columns;
    return true;
}

bool QueryParser::parseDropTable(const std::vector<std::string>& tokens) {
    if (tokens.size() != 3) return false;
    current_query.table_name = tokens[2];
    return true;
}

bool QueryParser::parseInsert(const std::vector<std::string>& tokens) {
    if (tokens.size() < 6) return false; // Minimum: INSERT INTO table VALUES (values...)
    
    current_query.type = QueryType::INSERT;
    current_query.table_name = tokens[2];
    
    // Parse values
    std::map<std::string, FieldValue> values;
    bool in_parentheses = false;
    int value_index = 0;
    
    // Get table schema to map values to column names
    TableSchema schema;
    try {
        schema = db_manager.getTableSchema(current_query.table_name);
    } catch (...) {
        return false;
    }
    
    for (size_t i = 5; i < tokens.size(); i++) {
        std::string token = tokens[i];
        
        if (token == "(") {
            in_parentheses = true;
            continue;
        }
        if (token == ")") {
            in_parentheses = false;
            continue;
        }
        
        if (in_parentheses && token != ",") {
            if (value_index >= schema.columns.size()) {
                return false; // Too many values
            }
            
            const std::string& column_name = schema.columns[value_index].name;
            Column::Type column_type = schema.columns[value_index].type;
            
            // Try to determine the type of the value
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
                        // Remove quotes if present
                        if (token.front() == '\'' && token.back() == '\'') {
                            token = token.substr(1, token.length() - 2);
                        }
                        values[column_name] = token;
                        break;
                    }
                    case Column::BOOL: {
                        bool bool_val = (token == "true" || token == "TRUE" || token == "1");
                        values[column_name] = bool_val;
                        break;
                    }
                }
                value_index++;
            } catch (...) {
                return false;
            }
        }
    }
    
    if (value_index != schema.columns.size()) {
        return false; // Not enough values
    }
    
    current_query.values = values;
    return true;
}

bool QueryParser::parseSelect(const std::vector<std::string>& tokens) {
    if (tokens.size() < 4) return false; // Minimum: SELECT * FROM table
    
    current_query.type = QueryType::SELECT;
    current_query.table_name = tokens[3];
    
    // Parse conditions if present
    if (tokens.size() > 4 && tokens[4] == "WHERE") {
        std::vector<Condition> conditions;
        for (size_t i = 5; i < tokens.size(); i += 4) {
            if (i + 2 >= tokens.size()) return false;
            
            Condition cond;
            cond.column = tokens[i];
            cond.op = tokens[i + 1];
            cond.value = parseValue(tokens[i + 2]);
            conditions.push_back(cond);
        }
        current_query.conditions = conditions;
    }
    
    return true;
}

bool QueryParser::parseUpdate(const std::vector<std::string>& tokens) {
    if (tokens.size() < 6) return false; // Minimum: UPDATE table SET column = value
    
    current_query.type = QueryType::UPDATE;
    current_query.table_name = tokens[1];
    
    // Parse SET clause
    std::map<std::string, FieldValue> values;
    for (size_t i = 3; i < tokens.size(); i += 4) {
        if (i + 2 >= tokens.size() || tokens[i + 1] != "=") return false;
        
        values[tokens[i]] = parseValue(tokens[i + 2]);
    }
    current_query.values = values;
    
    // Parse WHERE clause if present
    if (tokens.back() == "WHERE") {
        std::vector<Condition> conditions;
        for (size_t i = tokens.size() - 3; i < tokens.size(); i += 4) {
            if (i + 2 >= tokens.size()) return false;
            
            Condition cond;
            cond.column = tokens[i];
            cond.op = tokens[i + 1];
            cond.value = parseValue(tokens[i + 2]);
            conditions.push_back(cond);
        }
        current_query.conditions = conditions;
    }
    
    return true;
}

bool QueryParser::parseDelete(const std::vector<std::string>& tokens) {
    if (tokens.size() < 3) return false; // Minimum: DELETE FROM table
    
    current_query.type = QueryType::DELETE_OP;
    current_query.table_name = tokens[2];
    
    // Parse WHERE clause if present
    if (tokens.size() > 3 && tokens[3] == "WHERE") {
        std::vector<Condition> conditions;
        for (size_t i = 4; i < tokens.size(); i += 4) {
            if (i + 2 >= tokens.size()) return false;
            
            Condition cond;
            cond.column = tokens[i];
            cond.op = tokens[i + 1];
            cond.value = parseValue(tokens[i + 2]);
            conditions.push_back(cond);
        }
        current_query.conditions = conditions;
    }
    
    return true;
}

std::vector<std::string> QueryParser::tokenize(const std::string& query) {
    std::vector<std::string> tokens;
    std::string current_token;
    bool in_quotes = false;
    
    for (char c : query) {
        if (c == '\'') {
            in_quotes = !in_quotes;
            current_token += c;
        } else if (isspace(c) && !in_quotes) {
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