#ifndef QUERY_PARSER_H
#define QUERY_PARSER_H

#include "database_manager.h"
#include <string>
#include <vector>
#include <variant>
#include <map>
#include <memory>

// Define query types
enum class QueryType {
    CREATE_DATABASE,
    DROP_DATABASE,
    USE_DATABASE,
    SHOW_DATABASES,
    CREATE_TABLE,
    DROP_TABLE,
    SHOW_TABLES,
    DESCRIBE_TABLE,
    INSERT,
    SELECT,
    UPDATE,
    DELETE_OP,
    UNKNOWN
};

// Define condition structure
struct Condition {
    std::string column;
    std::string op;
    std::variant<int, float, std::string, bool> value;
};

// Define query structure
struct Query {
    QueryType type;
    std::string database_name;
    std::string table_name;
    std::vector<std::tuple<std::string, std::string, int>> columns; // For CREATE TABLE
    std::string primary_key;  // Changed from vector to string
    std::map<std::string, std::pair<std::string, std::string>> foreign_keys;  // Changed from vector to map
    std::vector<std::string> select_columns; // For SELECT
    std::vector<Condition> conditions; // For WHERE clauses
    std::vector<std::string> condition_operators;  // Added for AND/OR/NOT operators
    std::map<std::string, std::variant<int, float, std::string, bool>> values; // For INSERT and UPDATE
};

class QueryParser {
public:
    QueryParser(DatabaseManager& db_manager);
    bool parse(const std::string& query_string);
    bool execute();

private:
    DatabaseManager& db_manager;
    Query current_query;
    std::vector<std::string> commands;

    // Parsing helper methods
    bool parseCreateDatabase(const std::vector<std::string>& tokens);
    bool parseDropDatabase(const std::vector<std::string>& tokens);
    bool parseUseDatabase(const std::vector<std::string>& tokens);
    bool parseCreateTable(const std::vector<std::string>& tokens);
    bool parseDropTable(const std::vector<std::string>& tokens);
    bool parseInsert(const std::vector<std::string>& tokens);
    bool parseSelect(const std::vector<std::string>& tokens);
    bool parseUpdate(const std::vector<std::string>& tokens);
    bool parseDelete(const std::vector<std::string>& tokens);
    bool parseConditions(const std::vector<std::string>& tokens, size_t& index);
    std::variant<int, float, std::string, bool> parseValue(const std::string& value_str);
    Column::Type parseColumnType(const std::string& type_str);
    std::vector<std::string> tokenize(const std::string& query);
};

#endif // QUERY_PARSER_H 