#include "storage/bptree.h"
#include <iostream>

BPlusTree::BPlusTree(const std::string& index_file) : root_offset(-1) {
    file.open(index_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!file) {
        file.clear();
        file.open(index_file, std::ios::binary | std::ios::out | std::ios::trunc); // Create file if it doesn't exist
        file.close();
        file.open(index_file, std::ios::binary | std::ios::in | std::ios::out);
    }

    // Read root_offset from the first 4 bytes of the file
    if (file.tellg() > 0) {
        file.seekg(0);
        file.read(reinterpret_cast<char*>(&root_offset), sizeof(root_offset));
    }
}

BPlusTree::~BPlusTree() {
    if (file.is_open()) {
        // Always write root_offset to the start of the file
        file.seekp(0);
        file.write(reinterpret_cast<const char*>(&root_offset), sizeof(root_offset));
        file.close();
    }
}

int BPlusTree::get_root_offset() const {
    return root_offset;
}

BPlusNode BPlusTree::get_node(int offset) const {
    return read_node(offset);
}

BPlusNode BPlusTree::read_node(int offset) const {
    BPlusNode node;
    file.seekg(offset);
    if (!file) {
        std::cerr << "Error: Failed to seek to offset " << offset << std::endl;
        file.clear();
        return node;
    }

    file.read(reinterpret_cast<char*>(&node.is_leaf), sizeof(node.is_leaf));
    file.read(reinterpret_cast<char*>(&node.parent), sizeof(node.parent));

    int key_count;
    file.read(reinterpret_cast<char*>(&key_count), sizeof(key_count));
    if (key_count < 0 || key_count > FANOUT) {
        std::cerr << "Corrupted key_count at offset " << offset << ": " << key_count << std::endl;
        file.clear();
        return node;
    }

    node.keys.resize(key_count);
    file.read(reinterpret_cast<char*>(node.keys.data()), key_count * sizeof(int));

    if (node.is_leaf) {
        node.data_ptrs.resize(key_count);
        file.read(reinterpret_cast<char*>(node.data_ptrs.data()), key_count * sizeof(int));
    } else {
        node.children.resize(key_count + 1);
        file.read(reinterpret_cast<char*>(node.children.data()), (key_count + 1) * sizeof(int));
    }

    return node;
}

void BPlusTree::write_node(int offset, const BPlusNode& node) {
    file.seekp(offset);
    if (!file) {
        std::cerr << "Error: Failed to seek to offset " << offset << std::endl;
        return;
    }

    file.write(reinterpret_cast<const char*>(&node.is_leaf), sizeof(node.is_leaf));
    file.write(reinterpret_cast<const char*>(&node.parent), sizeof(node.parent));

    int key_count = node.keys.size();
    file.write(reinterpret_cast<const char*>(&key_count), sizeof(key_count));
    file.write(reinterpret_cast<const char*>(node.keys.data()), key_count * sizeof(int));

    if (node.is_leaf) {
        file.write(reinterpret_cast<const char*>(node.data_ptrs.data()), key_count * sizeof(int));
    } else {
        file.write(reinterpret_cast<const char*>(node.children.data()), (key_count + 1) * sizeof(int));
    }

    file.flush();
}
void BPlusTree::insert(int key, int data_offset) {
    if (root_offset == -1) {
        // Create root node at the end of the file
        BPlusNode root;
        root.is_leaf = true;
        root.keys.push_back(key);
        root.data_ptrs.push_back(data_offset);

        file.seekp(0, std::ios::end);
        root_offset = file.tellp();
        write_node(root_offset, root);
        
        // Update root_offset at the start of the file
        file.seekp(0);
        file.write(reinterpret_cast<const char*>(&root_offset), sizeof(root_offset));
        file.flush();

        std::cout << "Created root node at offset: " << root_offset << std::endl;
    } else {
        BPlusNode current = read_node(root_offset);
        // ... (rest of the insert logic remains the same)
        while (!current.is_leaf) {
            int i = 0;
            while (i < current.keys.size() && key > current.keys[i]) {
                i++;
            }
            current = read_node(current.children[i]);
        }

        // Insert the key into the leaf node
        int i = 0;
        while (i < current.keys.size() && key > current.keys[i]) {
            i++;
        }
        current.keys.insert(current.keys.begin() + i, key);
        current.data_ptrs.insert(current.data_ptrs.begin() + i, data_offset);

        // Write the updated node back to the file
        write_node(root_offset, current);
        std::cout << "Inserted key: " << key << " into leaf node at offset: " << root_offset << std::endl;

        // Check if the node needs to be split
        if (current.keys.size() > FANOUT) {
            std::cout << "Splitting node at offset: " << root_offset << std::endl;
            split_node(current, root_offset);
        }
    }
}

void BPlusTree::split_node(BPlusNode& node, int offset) {
    BPlusNode new_node;
    new_node.is_leaf = node.is_leaf;
    new_node.parent = node.parent;

    int mid = node.keys.size() / 2;
    new_node.keys.assign(node.keys.begin() + mid, node.keys.end());
    node.keys.erase(node.keys.begin() + mid, node.keys.end());

    if (node.is_leaf) {
        new_node.data_ptrs.assign(node.data_ptrs.begin() + mid, node.data_ptrs.end());
        node.data_ptrs.erase(node.data_ptrs.begin() + mid, node.data_ptrs.end());
    } else {
        new_node.children.assign(node.children.begin() + mid, node.children.end());
        node.children.erase(node.children.begin() + mid, node.children.end());
    }

    // Write new node at the end of the file
    file.seekp(0, std::ios::end);
    int new_offset = file.tellp();
    write_node(new_offset, new_node);

    // Update original node
    write_node(offset, node);

    if (offset == root_offset) {
        // Create new root
        BPlusNode new_root;
        new_root.is_leaf = false;
        new_root.keys.push_back(new_node.keys[0]);
        new_root.children.push_back(offset);
        new_root.children.push_back(new_offset);

        file.seekp(0, std::ios::end);
        root_offset = file.tellp();
        write_node(root_offset, new_root);

        // Update root_offset at the start of the file
        file.seekp(0);
        file.write(reinterpret_cast<const char*>(&root_offset), sizeof(root_offset));
        file.flush();
    } else {
        // Update parent node
        BPlusNode parent = read_node(node.parent);
        int i = 0;
        while (i < parent.keys.size() && new_node.keys[0] > parent.keys[i]) i++;
        parent.keys.insert(parent.keys.begin() + i, new_node.keys[0]);
        parent.children.insert(parent.children.begin() + i + 1, new_offset);
        write_node(parent.parent, parent);
    }
}


std::vector<int> BPlusTree::search(int key) {
    std::vector<int> result;
    if (root_offset == -1) {
        return result;
    }

    BPlusNode current = read_node(root_offset);
    while (!current.is_leaf) {
        int i = 0;
        while (i < current.keys.size() && key > current.keys[i]) {
            i++;
        }
        current = read_node(current.children[i]);
    }

    for (size_t i = 0; i < current.keys.size(); i++) {
        if (current.keys[i] == key) {
            result.push_back(current.data_ptrs[i]);
        }
    }

    return result;
}