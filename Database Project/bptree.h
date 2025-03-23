#ifndef BPTREE_H
#define BPTREE_H

#include <vector>
#include <fstream>
#include <iostream>

constexpr int FANOUT = 4; // Maximum number of keys per node

struct BPlusNode {
    bool is_leaf;
    int parent;
    std::vector<int> keys;
    std::vector<int> children;    // Non-leaf nodes
    std::vector<int> data_ptrs;   // Leaf nodes
};

class BPlusTree {
public:
    BPlusTree(const std::string& index_file);
    ~BPlusTree();
    void insert(int key, int data_offset);
    std::vector<int> search(int key);
    int get_root_offset() const;
    BPlusNode get_node(int offset) const;

private:
    mutable std::fstream file; // Mark file as mutable
    int root_offset;

    BPlusNode read_node(int offset) const;
    void write_node(int offset, const BPlusNode& node);
    void split_node(BPlusNode& node, int offset);
};

#endif
