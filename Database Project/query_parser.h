#ifndef QUERY_PARSER_H
#define QUERY_PARSER_H

#include "database_manager.h"
#include <string>
#include <vector>
#include <map>
#include <variant>
#include <tuple>

using FieldValue = std::variant<int, float, std::string, bool>;

enum class QueryType {
    CREATE_DATABASE,
    DROP_DATABASE,
    USE_DATABASE,
    SHOW_DATABASES,
    CREATE_TABLE,
    DROP_TABLE,
    SHOW_TABLES,
    INSERT,
    SELECT,
    UPDATE,
    DELETE_OP
};

struct Condition {
    std::string column;
    std::string op;
    FieldValue value;
};

struct Query {
    QueryType type;
    std::string database_name;
    std::string table_name;
    std::string join_table_name;
    std::vector<std::tuple<std::string, std::string, int>> columns; // name, type, length
    std::string primary_key;
    std::map<std::string, std::pair<std::string, std::string>> foreign_keys; // col -> (ref_table, ref_col)
    std::map<std::string, FieldValue> values;
    std::vector<Condition> conditions;
    std::vector<std::string> condition_operators;
    std::vector<std::string> select_columns; // Added for SELECT column selection
    // Join-related fields
    Condition join_condition;
    // New fields for structured response
    std::vector<Record> results;
    std::string error_message;
    int records_found;
};

class QueryParser {
public:
    Query current_query;
    QueryParser(DatabaseManager& db_manager);
    bool parse(const std::string& query_string);
    bool execute();

private:
    DatabaseManager& db_manager;
    std::vector<std::string> commands;
    

    // Parsing methods
    bool parseCreateDatabase(const std::vector<std::string>& tokens);
    bool parseDropDatabase(const std::vector<std::string>& tokens);
    bool parseUseDatabase(const std::vector<std::string>& tokens);
    bool parseCreateTable(const std::vector<std::string>& tokens);
    bool parseDropTable(const std::vector<std::string>& tokens);
    bool parseInsert(const std::vector<std::string>& tokens);
    bool parseSelect(const std::vector<std::string>& tokens);
    bool parseUpdate(const std::vector<std::string>& tokens);
    bool parseDelete(const std::vector<std::string>& tokens);
    std::vector<Record> filterRecordsByColumns(const std::vector<Record>& records, const std::vector<std::string>& columns);

    // Helper methods
    std::vector<std::string> tokenize(const std::string& query);
    FieldValue parseValue(const std::string& value_str);
    Column::Type parseColumnType(const std::string& type_str);
};

#endif // QUERY_PARSER_H