#ifndef CATALOG_H
#define CATALOG_H

#include <vector>
#include <string>

struct Column {
    std::string name;
    enum Type { INT, STRING, BOOL } type;
    int length; // For strings
};

struct TableSchema {
    std::string name;
    std::vector<Column> columns;
    std::string primary_key;
};

class Catalog {
public:
    void load(const std::string& path);
    void save(const std::string& path);
    std::vector<TableSchema> tables;
};

#endif