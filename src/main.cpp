#include "storage/catalog.h"
#include "storage/bptree.h"
#include "storage/record.h"
#include <iostream>
#include <queue>

void print_catalog(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        std::cerr << "Error: Could not open catalog file." << std::endl;
        return;
    }

    int table_count;
    file.read(reinterpret_cast<char*>(&table_count), sizeof(table_count));
    std::cout << "Number of tables: " << table_count << std::endl;

    for (int i = 0; i < table_count; i++) {
        TableSchema table;
        int name_length;
        file.read(reinterpret_cast<char*>(&name_length), sizeof(name_length));
        table.name.resize(name_length);
        file.read(&table.name[0], name_length);

        int column_count;
        file.read(reinterpret_cast<char*>(&column_count), sizeof(column_count));
        std::cout << "Table: " << table.name << " (Columns: " << column_count << ")" << std::endl;

        for (int j = 0; j < column_count; j++) {
            Column column;
            int column_name_length;
            file.read(reinterpret_cast<char*>(&column_name_length), sizeof(column_name_length));
            column.name.resize(column_name_length);
            file.read(&column.name[0], column_name_length);
            file.read(reinterpret_cast<char*>(&column.type), sizeof(column.type));
            file.read(reinterpret_cast<char*>(&column.length), sizeof(column.length));

            std::cout << "  Column: " << column.name << ", Type: " << column.type << ", Length: " << column.length << std::endl;
        }

        int primary_key_length;
        file.read(reinterpret_cast<char*>(&primary_key_length), sizeof(primary_key_length));
        table.primary_key.resize(primary_key_length);
        file.read(&table.primary_key[0], primary_key_length);

        std::cout << "  Primary Key: " << table.primary_key << std::endl;
    }
}

void print_records(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        std::cerr << "Error: Could not open data file." << std::endl;
        return;
    }

    std::cout << "Records in " << path << ":" << std::endl;
    while (file) {
        Record record = Record::deserialize(file);
        if (file) {
            std::cout << "  ID: " << record.id << ", Name: " << record.name << ", Active: " << record.active << std::endl;
        }
    }
}

void print_bptree(const std::string& path) {
    BPlusTree index(path);
    std::cout << "B+ Tree Index in " << path << ":" << std::endl;

    int root_offset = index.get_root_offset();
    if (root_offset == -1) {
        std::cout << "  Tree is empty." << std::endl;
        return;
    }

    // Traverse the tree and print all nodes
    std::queue<std::pair<int, int>> nodes; // (offset, level)
    nodes.push(std::make_pair(root_offset, 0));

    while (!nodes.empty()) {
        std::pair<int, int> current = nodes.front();
        int offset = current.first;
        int level = current.second;
        nodes.pop();

        BPlusNode node = index.get_node(offset);
        std::cout << "  Level " << level << ", Offset " << offset << ": ";
        for (int key : node.keys) {
            std::cout << key << " ";
        }
        std::cout << std::endl;

        if (!node.is_leaf) {
            for (int child : node.children) {
                nodes.push(std::make_pair(child, level + 1));
            }
        }
    }
}

int main() {
    // Initialize catalog
    Catalog catalog;
    catalog.load("data/catalog.bin");

    // Create a table
    TableSchema users;
    users.name = "users";
    users.columns = {{"id", Column::INT}, {"name", Column::STRING, 50}, {"active", Column::BOOL}};
    users.primary_key = "id";
    catalog.tables.push_back(users);
    catalog.save("data/catalog.bin");

    // Insert records
    BPlusTree index("data/users.idx");
    Record user1{1, "Alice", true};
    Record user2{2, "Bob", false};
    Record user3{3, "Amir", false};
    Record user4{4, "Ashir", false};
    Record user5{5, "Ali", false};

    std::ofstream data_file("data/users.dat", std::ios::binary | std::ios::app);
    int offset1 = data_file.tellp();
    std::cout << "Inserting user1 at offset " << offset1 << std::endl;
    user1.serialize(data_file);
    int offset2 = data_file.tellp();
    std::cout << "Inserting user2 at offset " << offset2 << std::endl;
    user2.serialize(data_file);
    int offset3 = data_file.tellp();
    std::cout << "Inserting user3 at offset " << offset3 << std::endl;
    user3.serialize(data_file);
    data_file.close();
    int offset4 = data_file.tellp();
    std::cout << "Inserting user4 at offset " << offset4 << std::endl;
    user4.serialize(data_file);
    data_file.close();
    int offset5 = data_file.tellp();
    std::cout << "Inserting user5 at offset " << offset5 << std::endl;
    user5.serialize(data_file);
    data_file.close();

    index.insert(user1.id, offset1);
    index.insert(user2.id, offset2);
    index.insert(user3.id, offset3);
    index.insert(user4.id, offset4);
    index.insert(user5.id, offset5);

    // Print catalog
    std::cout << "\nCatalog:\n";
    print_catalog("data/catalog.bin");

    // Print records
    std::cout << "\nRecords:\n";
    print_records("data/users.dat");

    // Print B+ tree index
    std::cout << "\nB+ Tree Index:\n";
    print_bptree("data/users.idx");

    // Search for a record
    int search_key = 1;
    std::vector<int> search_result = index.search(search_key);
    if (!search_result.empty()) {
        std::cout << "\nSearch result for key " << search_key << ":\n";
        for (int offset : search_result) {
            std::ifstream data_file("data/users.dat", std::ios::binary);
            data_file.seekg(offset);
            Record record = Record::deserialize(data_file);
            std::cout << "  ID: " << record.id << ", Name: " << record.name << ", Active: " << record.active << std::endl;
        }
    } else {
        std::cout << "\nNo record found for key " << search_key << std::endl;
    }
    //Command for running
    //g++ -std=c++11 -Iinclude src/main.cpp src/storage/catalog.cpp src/storage/bptree.cpp src/storage/record.cpp -o main
    return 0;
}