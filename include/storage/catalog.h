#ifndef CATALOG_H
#define CATALOG_H

#include <vector>
#include <string>

// Enhanced TableSchema in catalog.h
struct Column {
    std::string name;
    enum Type { INT, FLOAT, STRING, CHAR, BOOL } type;
    int length; // For strings and chars
    bool is_primary_key;
    bool is_foreign_key;
    std::string references_table;
    std::string references_column;
};

struct TableSchema {
    std::string name;
    std::vector<Column> columns;
    std::string data_file_path;
    std::string index_file_path;
};

class Catalog {
public:
    void load(const std::string& path);
    void save(const std::string& path);
    std::vector<TableSchema> tables;
};

#endif