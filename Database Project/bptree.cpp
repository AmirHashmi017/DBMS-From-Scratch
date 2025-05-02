#include "bptree.h"
#include <iostream>

using namespace std;
namespace fs = filesystem;

BPlusTree::BPlusTree(const string& index_file) : root_offset(-1), is_closed(false) {
    fs::path indexPath(index_file);
    fs::create_directories(indexPath.parent_path());

    file.open(index_file, ios::binary | ios::in | ios::out);
    if (!file) {
        cout << "Creating new index file: " << index_file << endl;
        file.clear();
        file.open(index_file, ios::binary | ios::out | ios::trunc);
        file.close();
        file.open(index_file, ios::binary | ios::in | ios::out);
    }

    file.seekg(0, ios::end);
    if (file.tellg() >= sizeof(root_offset)) {
        file.seekg(0);
        file.read(reinterpret_cast<char*>(&root_offset), sizeof(root_offset));
    }
}

BPlusTree::~BPlusTree() {
    close(); // Reuse close method for consistency
}

int BPlusTree::get_root_offset() const {
    return root_offset;
}

BPlusNode BPlusTree::get_node(int offset) const {
    return read_node(offset);
}

BPlusNode BPlusTree::read_node(int offset) const {
    if (is_closed) {
        cerr << "Error: Attempt to read from closed BPlusTree file" << endl;
        return BPlusNode();
    }
    BPlusNode node;
    file.seekg(offset);
    if (!file) {
        cerr << "Error: Failed to seek to offset " << offset << endl;
        file.clear();
        return node;
    }
    // Rest unchanged
    file.read(reinterpret_cast<char*>(&node.is_leaf), sizeof(node.is_leaf));
    file.read(reinterpret_cast<char*>(&node.parent), sizeof(node.parent));
    int key_count;
    file.read(reinterpret_cast<char*>(&key_count), sizeof(key_count));
    if (key_count < 0 || key_count > FANOUT) {
        cerr << "Corrupted key_count at offset " << offset << ": " << key_count << endl;
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
    if (is_closed) {
        cerr << "Error: Attempt to write to closed BPlusTree file" << endl;
        return;
    }
    file.seekp(offset);
    if (!file) {
        cerr << "Error: Failed to seek to offset " << offset << endl;
        file.clear();
        return;
    }
    // Rest unchanged
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
        root.parent = -1;  // Root has no parent
        root.keys.push_back(key);
        root.data_ptrs.push_back(data_offset);

        file.seekp(sizeof(root_offset), ios::beg);  // Skip the root_offset storage
        root_offset = file.tellp();
        write_node(root_offset, root);

        // Update root_offset at the start of the file
        file.seekp(0);
        file.write(reinterpret_cast<const char*>(&root_offset), sizeof(root_offset));
        file.flush();

        cout << "Created root node at offset: " << root_offset << endl;
    }
    else {
        // Find the leaf node where the key should be inserted
        int current_offset = root_offset;
        BPlusNode current = read_node(current_offset);

        cout << "Starting search at root offset: " << root_offset << endl;

        // Traverse to leaf
        while (!current.is_leaf) {
            int i = 0;
            while (i < current.keys.size() && key > current.keys[i]) {
                i++;
            }
            cout << "Moving from node at offset " << current_offset
                << " to child at index " << i << " (offset " << current.children[i] << ")" << endl;
            current_offset = current.children[i];
            current = read_node(current_offset);
        }

        cout << "Found leaf node at offset: " << current_offset << endl;

        // Insert the key into the leaf node
        int i = 0;
        while (i < current.keys.size() && key > current.keys[i]) {
            i++;
        }

        // Check for duplicate key
        if (i < current.keys.size() && current.keys[i] == key) {
            cout << "Key " << key << " already exists, updating data pointer" << endl;
            current.data_ptrs[i] = data_offset;
        }
        else {
            cout << "Inserting key " << key << " at position " << i << endl;
            current.keys.insert(current.keys.begin() + i, key);
            current.data_ptrs.insert(current.data_ptrs.begin() + i, data_offset);
        }

        // Write the updated node back to the file
        write_node(current_offset, current);
        cout << "Updated leaf node at offset: " << current_offset << endl;

        // Check if the node needs to be split
        if (current.keys.size() > FANOUT) {
            cout << "Node at offset " << current_offset << " needs splitting" << endl;
            split_node(current, current_offset);
        }
    }
}

void BPlusTree::split_node(BPlusNode& node, int node_offset) {
    cout << "Splitting node at offset: " << node_offset << endl;

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
    file.seekp(0, ios::end);
    int new_offset = file.tellp();
    write_node(new_offset, new_node);
    cout << "Created new node at offset: " << new_offset << endl;

    // Update parent node
    if (node.parent == -1) {
        // Create new root
        BPlusNode root;
        root.is_leaf = false;
        root.parent = -1;
        root.keys.push_back(promoted_key);
        root.children.push_back(node_offset);
        root.children.push_back(new_offset);

        // Write new root at the end of the file
        file.seekp(0, ios::end);
        root_offset = file.tellp();
        write_node(root_offset, root);

        // Update root_offset at the start of the file
        file.seekp(0);
        file.write(reinterpret_cast<const char*>(&root_offset), sizeof(root_offset));
        file.flush();

        cout << "Created new root at offset: " << root_offset << endl;
    }
    else {
        // Update existing parent
        BPlusNode parent = read_node(node.parent);
        int i = 0;
        while (i < parent.keys.size() && promoted_key > parent.keys[i]) {
            i++;
        }

        parent.keys.insert(parent.keys.begin() + i, promoted_key);
        parent.children.insert(parent.children.begin() + i + 1, new_offset);

        write_node(node.parent, parent);
        cout << "Updated parent node at offset: " << node.parent << endl;

        // Check if parent needs splitting
        if (parent.keys.size() > FANOUT) {
            cout << "Parent node at offset " << node.parent << " needs splitting" << endl;
            split_node(parent, node.parent);
        }
    }

    // Update parent pointers
    node.parent = node.parent == -1 ? root_offset : node.parent;
    new_node.parent = node.parent;
    write_node(node_offset, node);
    write_node(new_offset, new_node);
}

void BPlusTree::close() {
    if (!is_closed) {
        file.close();
        is_closed = true;
    }
}

vector<int> BPlusTree::search(int key) {
    vector<int> results;
    if (root_offset == -1) {
        return results;
    }

    int current_offset = root_offset;
    BPlusNode current = read_node(current_offset);

    // Traverse to leaf
    while (!current.is_leaf) {
        int i = 0;
        while (i < current.keys.size() && key > current.keys[i]) {
            i++;
        }
        current_offset = current.children[i];
        current = read_node(current_offset);
    }

    // Search in leaf node
    int i = 0;
    while (i < current.keys.size() && key > current.keys[i]) {
        i++;
    }

    if (i < current.keys.size() && current.keys[i] == key) {
        results.push_back(current.data_ptrs[i]);
    }

    return results;
}