#include "bptree.h"
#include <iostream>

BPlusTree::BPlusTree(const std::string& index_file) : root_offset(-1) {
    // Make sure the directory exists before opening the file
    std::filesystem::path indexPath(index_file);
    std::filesystem::create_directories(indexPath.parent_path());

    file.open(index_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!file) {
        std::cout << "Creating new index file: " << index_file << std::endl;
        file.clear();
        file.open(index_file, std::ios::binary | std::ios::out | std::ios::trunc); // Create file if it doesn't exist
        file.close();
        file.open(index_file, std::ios::binary | std::ios::in | std::ios::out);
    }

    // Read root_offset from the first 4 bytes of the file
    file.seekg(0, std::ios::end);
    if (file.tellg() >= sizeof(root_offset)) {
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
    }
    else {
        node.children.resize(key_count + 1);
        file.read(reinterpret_cast<char*>(node.children.data()), (key_count + 1) * sizeof(int));
    }

    return node;
}

void BPlusTree::write_node(int offset, const BPlusNode& node) {
    file.seekp(offset);
    if (!file) {
        std::cerr << "Error: Failed to seek to offset " << offset << std::endl;
        file.clear();
        return;
    }

    file.write(reinterpret_cast<const char*>(&node.is_leaf), sizeof(node.is_leaf));
    file.write(reinterpret_cast<const char*>(&node.parent), sizeof(node.parent));

    int key_count = node.keys.size();
    file.write(reinterpret_cast<const char*>(&key_count), sizeof(key_count));
    file.write(reinterpret_cast<const char*>(node.keys.data()), key_count * sizeof(int));

    if (node.is_leaf) {
        file.write(reinterpret_cast<const char*>(node.data_ptrs.data()), key_count * sizeof(int));
    }
    else {
        file.write(reinterpret_cast<const char*>(node.children.data()), (key_count + 1) * sizeof(int));
    }

    file.flush();
}

void BPlusTree::insert(int key, int data_offset) {
    if (root_offset == -1) {
        // Create root node at the end of the file
        BPlusNode root;
        root.is_leaf = true;
        root.parent = -1;  // Root has no parent
        root.keys.push_back(key);
        root.data_ptrs.push_back(data_offset);

        file.seekp(sizeof(root_offset), std::ios::beg);  // Skip the root_offset storage
        root_offset = file.tellp();
        write_node(root_offset, root);

        // Update root_offset at the start of the file
        file.seekp(0);
        file.write(reinterpret_cast<const char*>(&root_offset), sizeof(root_offset));
        file.flush();

        std::cout << "Created root node at offset: " << root_offset << std::endl;
    }
    else {
        // Find the leaf node where the key should be inserted
        int current_offset = root_offset;
        BPlusNode current = read_node(current_offset);

        std::cout << "Starting search at root offset: " << root_offset << std::endl;

        // Traverse to leaf
        while (!current.is_leaf) {
            int i = 0;
            while (i < current.keys.size() && key > current.keys[i]) {
                i++;
            }
            std::cout << "Moving from node at offset " << current_offset
                << " to child at index " << i << " (offset " << current.children[i] << ")" << std::endl;
            current_offset = current.children[i];
            current = read_node(current_offset);
        }

        std::cout << "Found leaf node at offset: " << current_offset << std::endl;

        // Insert the key into the leaf node
        int i = 0;
        while (i < current.keys.size() && key > current.keys[i]) {
            i++;
        }

        // Check for duplicate key
        if (i < current.keys.size() && current.keys[i] == key) {
            std::cout << "Key " << key << " already exists, updating data pointer" << std::endl;
            current.data_ptrs[i] = data_offset;
        }
        else {
            std::cout << "Inserting key " << key << " at position " << i << std::endl;
            current.keys.insert(current.keys.begin() + i, key);
            current.data_ptrs.insert(current.data_ptrs.begin() + i, data_offset);
        }

        // Write the updated node back to the file
        write_node(current_offset, current);
        std::cout << "Updated leaf node at offset: " << current_offset << std::endl;

        // Check if the node needs to be split
        if (current.keys.size() > FANOUT) {
            std::cout << "Node at offset " << current_offset << " needs splitting" << std::endl;
            split_node(current, current_offset);
        }
    }
}

void BPlusTree::split_node(BPlusNode& node, int node_offset) {
    std::cout << "Splitting node at offset: " << node_offset << std::endl;

    // Create a new node
    BPlusNode new_node;
    new_node.is_leaf = node.is_leaf;
    new_node.parent = node.parent;

    int mid = node.keys.size() / 2;
    int promoted_key = node.keys[mid];

    // For non-leaf nodes, we promote the middle key but don't keep it in the children
    if (!node.is_leaf) {
        new_node.keys.assign(node.keys.begin() + mid + 1, node.keys.end());
        new_node.children.assign(node.children.begin() + mid + 1, node.children.end());

        // Remove the promoted key and the children to the right from the original node
        node.keys.erase(node.keys.begin() + mid, node.keys.end());
        node.children.erase(node.children.begin() + mid + 1, node.children.end());
    }
    else {
        // For leaf nodes, we copy the middle key to the parent but keep it in the leaf
        new_node.keys.assign(node.keys.begin() + mid, node.keys.end());
        new_node.data_ptrs.assign(node.data_ptrs.begin() + mid, node.data_ptrs.end());

        // Remove the keys and data_ptrs from the original node
        node.keys.erase(node.keys.begin() + mid, node.keys.end());
        node.data_ptrs.erase(node.data_ptrs.begin() + mid, node.data_ptrs.end());
    }

    // Write new node at the end of the file
    file.seekp(0, std::ios::end);
    int new_offset = file.tellp();
    write_node(new_offset, new_node);
    std::cout << "Created new node at offset: " << new_offset << std::endl;

    // Update original node
    write_node(node_offset, node);
    std::cout << "Updated original node at offset: " << node_offset << std::endl;

    // Update parent pointers for all children of the new node
    if (!new_node.is_leaf) {
        for (int child_offset : new_node.children) {
            BPlusNode child = read_node(child_offset);
            child.parent = new_offset;
            write_node(child_offset, child);
            std::cout << "Updated parent pointer for child at offset " << child_offset << std::endl;
        }
    }

    // Handle the parent node creation or update
    if (node.parent == -1) {
        // This was the root node, create a new root
        BPlusNode new_root;
        new_root.is_leaf = false;
        new_root.parent = -1;
        new_root.keys.push_back(promoted_key);
        new_root.children.push_back(node_offset);
        new_root.children.push_back(new_offset);

        file.seekp(0, std::ios::end);
        int new_root_offset = file.tellp();
        write_node(new_root_offset, new_root);
        std::cout << "Created new root at offset: " << new_root_offset << std::endl;

        // Update parent pointers in the split nodes
        node.parent = new_root_offset;
        write_node(node_offset, node);

        new_node.parent = new_root_offset;
        write_node(new_offset, new_node);

        // Update root_offset
        root_offset = new_root_offset;
        file.seekp(0);
        file.write(reinterpret_cast<const char*>(&root_offset), sizeof(root_offset));
        file.flush();
        std::cout << "Updated root_offset to: " << root_offset << std::endl;
    }
    else {
        // Update the parent node
        BPlusNode parent = read_node(node.parent);
        std::cout << "Updating parent node at offset: " << node.parent << std::endl;

        // Find where to insert the promoted key
        int i = 0;
        while (i < parent.keys.size() && promoted_key > parent.keys[i]) {
            i++;
        }

        // Insert the promoted key and the new child pointer
        parent.keys.insert(parent.keys.begin() + i, promoted_key);
        parent.children.insert(parent.children.begin() + i + 1, new_offset);

        // Write the updated parent back to the file
        write_node(node.parent, parent);
        std::cout << "Updated parent node at offset: " << node.parent << std::endl;

        // Check if the parent now needs to be split
        if (parent.keys.size() > FANOUT) {
            std::cout << "Parent node needs splitting" << std::endl;
            split_node(parent, node.parent);
        }
    }
}

std::vector<int> BPlusTree::search(int key) {
    std::vector<int> result;
    if (root_offset == -1) {
        std::cout << "Tree is empty, returning empty result" << std::endl;
        return result;
    }

    int current_offset = root_offset;
    BPlusNode current = read_node(current_offset);
    std::cout << "Starting search for key " << key << " at root offset: " << root_offset << std::endl;

    // Find the leaf node that might contain the key
    while (!current.is_leaf) {
        int i = 0;
        while (i < current.keys.size() && key > current.keys[i]) {
            i++;
        }
        std::cout << "Moving from node at offset " << current_offset
            << " to child at index " << i << " (offset " << current.children[i] << ")" << std::endl;
        current_offset = current.children[i];
        current = read_node(current_offset);
    }

    std::cout << "Found leaf node at offset: " << current_offset << std::endl;

    // Search for the key in the leaf node
    for (size_t i = 0; i < current.keys.size(); i++) {
        if (current.keys[i] == key) {
            std::cout << "Found key " << key << " at position " << i << std::endl;
            result.push_back(current.data_ptrs[i]);
        }
    }

    if (result.empty()) {
        std::cout << "Key " << key << " not found" << std::endl;
    }
    else {
        std::cout << "Found " << result.size() << " matching records" << std::endl;
    }

    return result;
}