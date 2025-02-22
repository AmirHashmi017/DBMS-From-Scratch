package main

import (
    "encoding/binary"
    "fmt"
)

const (
    BNODE_NODE         = 1    // Internal node
    BNODE_LEAF         = 2    // Leaf node
    HEADER             = 4    // 4-byte header for metadata
    BTREE_PAGE_SIZE    = 4096 // Node size
    BTREE_MAX_KEY_SIZE = 1000
    BTREE_MAX_VAL_SIZE = 3000
)

type BNode struct {
    data []byte // Serialized node data
}

// Get node type
func (node BNode) btype() uint16 {
    return binary.LittleEndian.Uint16(node.data)
}

// Get number of keys
func (node BNode) nkeys() uint16 {
    return binary.LittleEndian.Uint16(node.data[2:4])
}

// Set header values
func (node *BNode) setHeader(btype uint16, nkeys uint16) {
    binary.LittleEndian.PutUint16(node.data[0:2], btype)
    binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// Get position of a key-value pair
func (node BNode) kvPos(idx uint16) uint16 {
    if idx >= node.nkeys() {
        return 0 // Prevent invalid index access
    }
    // Base offset: header + space for nkeys offsets
    offset := HEADER + 2*node.nkeys()
    // Add the lengths of previous key-value pairs
    for i := uint16(0); i < idx; i++ {
        pos := offset
        klen := binary.LittleEndian.Uint16(node.data[pos:])
        vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
        offset += 4 + klen + vlen // Move to the next key-value pair
    }
    return offset
}

// Get the key at a specific index
func (node BNode) getKey(idx uint16) []byte {
    if idx >= node.nkeys() {
        return nil
    }
    pos := node.kvPos(idx)
    klen := binary.LittleEndian.Uint16(node.data[pos:])
    if pos+4+uint16(klen) > uint16(len(node.data)) {
        fmt.Println("Error: Key out of bounds")
        return nil
    }
    return node.data[pos+4 : pos+4+klen]
}

// Get the value at a specific index
func (node BNode) getVal(idx uint16) []byte {
    if idx >= node.nkeys() {
        return nil
    }
    pos := node.kvPos(idx)
    klen := binary.LittleEndian.Uint16(node.data[pos:])
    vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
    if pos+4+klen+vlen > uint16(len(node.data)) {
        fmt.Println("Error: Value out of bounds")
        return nil
    }
    return node.data[pos+4+klen : pos+4+klen+vlen]
}

// Insert a key-value pair
func (node *BNode) insertKeyVal(idx uint16, key, val []byte) {
    nkeys := node.nkeys()

    // Ensure we do not exceed the node size
    neededSize := HEADER + 2*(nkeys+1) + 4 + uint16(len(key)) + uint16(len(val))
    if neededSize > BTREE_PAGE_SIZE {
        fmt.Println("Node is full, cannot insert more keys")
        return
    }

    // Calculate the position for the new key-value pair
    pos := node.kvPos(idx)

    // Write the key and value lengths
    binary.LittleEndian.PutUint16(node.data[pos:], uint16(len(key)))
    binary.LittleEndian.PutUint16(node.data[pos+2:], uint16(len(val)))

    // Copy the key and value data
    copy(node.data[pos+4:], key)
    copy(node.data[pos+4+uint16(len(key)):], val)

    // Update the node's header
    node.setHeader(node.btype(), nkeys+1)
}

func main() {
    // Initialize an empty node
    node := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    node.setHeader(BNODE_LEAF, 0)

    // Insert key-value pairs
    node.insertKeyVal(0, []byte("name"), []byte("Amir"))
    node.insertKeyVal(1, []byte("age"), []byte("25"))

    // Retrieve and print values
    fmt.Println("Key: name, Value:", string(node.getVal(0)))
    fmt.Println("Key: age, Value:", string(node.getVal(1)))
}