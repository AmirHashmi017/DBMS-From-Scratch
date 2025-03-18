#include "../include/storage/catalog.h"
#include <fstream>

void Catalog::load(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (file) {
        int table_count;
        file.read(reinterpret_cast<char*>(&table_count), sizeof(table_count));
        for (int i = 0; i < table_count; i++) {
            TableSchema table;
            int name_length;
            file.read(reinterpret_cast<char*>(&name_length), sizeof(name_length));
            table.name.resize(name_length);
            file.read(&table.name[0], name_length);

            int column_count;
            file.read(reinterpret_cast<char*>(&column_count), sizeof(column_count));
            for (int j = 0; j < column_count; j++) {
                Column column;
                int column_name_length;
                file.read(reinterpret_cast<char*>(&column_name_length), sizeof(column_name_length));
                column.name.resize(column_name_length);
                file.read(&column.name[0], column_name_length);
                file.read(reinterpret_cast<char*>(&column.type), sizeof(column.type));
                file.read(reinterpret_cast<char*>(&column.length), sizeof(column.length));
                table.columns.push_back(column);
            }

            int primary_key_length;
            file.read(reinterpret_cast<char*>(&primary_key_length), sizeof(primary_key_length));
            table.primary_key.resize(primary_key_length);
            file.read(&table.primary_key[0], primary_key_length);

            tables.push_back(table);
        }
    }
}

void Catalog::save(const std::string& path) {
    std::ofstream file(path, std::ios::binary);
    if (file) {
        int table_count = tables.size();
        file.write(reinterpret_cast<const char*>(&table_count), sizeof(table_count));
        for (const auto& table : tables) {
            int name_length = table.name.size();
            file.write(reinterpret_cast<const char*>(&name_length), sizeof(name_length));
            file.write(table.name.c_str(), name_length);

            int column_count = table.columns.size();
            file.write(reinterpret_cast<const char*>(&column_count), sizeof(column_count));
            for (const auto& column : table.columns) {
                int column_name_length = column.name.size();
                file.write(reinterpret_cast<const char*>(&column_name_length), sizeof(column_name_length));
                file.write(column.name.c_str(), column_name_length);
                file.write(reinterpret_cast<const char*>(&column.type), sizeof(column.type));
                file.write(reinterpret_cast<const char*>(&column.length), sizeof(column.length));
            }

            int primary_key_length = table.primary_key.size();
            file.write(reinterpret_cast<const char*>(&primary_key_length), sizeof(primary_key_length));
            file.write(table.primary_key.c_str(), primary_key_length);
        }
    }
}