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
    std::vector<std::tuple<std::string, std::string, int>> columns; // name, type, length
    std::string primary_key;
    std::map<std::string, std::pair<std::string, std::string>> foreign_keys; // col -> (ref_table, ref_col)
    std::map<std::string, FieldValue> values;
    std::vector<Condition> conditions;
    std::vector<std::string> condition_operators;
};

class QueryParser {
public:
    QueryParser(DatabaseManager& db_manager);
    bool parse(const std::string& query_string);
    bool execute();

private:
    DatabaseManager& db_manager;
    std::vector<std::string> commands;
    Query current_query;

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

    // Helper methods
    std::vector<std::string> tokenize(const std::string& query);
    FieldValue parseValue(const std::string& value_str);
    Column::Type parseColumnType(const std::string& type_str);
};

#endif // QUERY_PARSER_H