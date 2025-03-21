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
                file.read(reinterpret_cast<char*>(&column.is_primary_key), sizeof(column.is_primary_key));
                file.read(reinterpret_cast<char*>(&column.is_foreign_key), sizeof(column.is_foreign_key));
                
                // Read references fields if it's a foreign key
                if (column.is_foreign_key) {
                    int ref_table_length;
                    file.read(reinterpret_cast<char*>(&ref_table_length), sizeof(ref_table_length));
                    column.references_table.resize(ref_table_length);
                    file.read(&column.references_table[0], ref_table_length);
                    
                    int ref_column_length;
                    file.read(reinterpret_cast<char*>(&ref_column_length), sizeof(ref_column_length));
                    column.references_column.resize(ref_column_length);
                    file.read(&column.references_column[0], ref_column_length);
                }
                
                table.columns.push_back(column);
            }

            // Read data and index file paths
            int data_file_length;
            file.read(reinterpret_cast<char*>(&data_file_length), sizeof(data_file_length));
            table.data_file_path.resize(data_file_length);
            file.read(&table.data_file_path[0], data_file_length);
            
            int index_file_length;
            file.read(reinterpret_cast<char*>(&index_file_length), sizeof(index_file_length));
            table.index_file_path.resize(index_file_length);
            file.read(&table.index_file_path[0], index_file_length);

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
                file.write(reinterpret_cast<const char*>(&column.is_primary_key), sizeof(column.is_primary_key));
                file.write(reinterpret_cast<const char*>(&column.is_foreign_key), sizeof(column.is_foreign_key));
                
                // Write references fields if it's a foreign key
                if (column.is_foreign_key) {
                    int ref_table_length = column.references_table.size();
                    file.write(reinterpret_cast<const char*>(&ref_table_length), sizeof(ref_table_length));
                    file.write(column.references_table.c_str(), ref_table_length);
                    
                    int ref_column_length = column.references_column.size();
                    file.write(reinterpret_cast<const char*>(&ref_column_length), sizeof(ref_column_length));
                    file.write(column.references_column.c_str(), ref_column_length);
                }
            }

            // Write data and index file paths
            int data_file_length = table.data_file_path.size();
            file.write(reinterpret_cast<const char*>(&data_file_length), sizeof(data_file_length));
            file.write(table.data_file_path.c_str(), data_file_length);
            
            int index_file_length = table.index_file_path.size();
            file.write(reinterpret_cast<const char*>(&index_file_length), sizeof(index_file_length));
            file.write(table.index_file_path.c_str(), index_file_length);
        }
    }
}