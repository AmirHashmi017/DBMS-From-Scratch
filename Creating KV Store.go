package main

import (
    "bytes"
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

type BTree struct {
    root uint64         // Pointer to the root node
    get  func(uint64) BNode // Callback to retrieve a node
    new  func(BNode) uint64 // Callback to allocate a new node
    del  func(uint64)       // Callback to deallocate a node
}

// Helper function to assert conditions
func assert(condition bool) {
    if !condition {
        panic("Assertion failed")
    }
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

// Get pointer at a specific index
func (node BNode) getPtr(idx uint16) uint64 {
    assert(idx < node.nkeys())
    pos := HEADER + 8*idx
    return binary.LittleEndian.Uint64(node.data[pos:])
}

// Set pointer at a specific index
func (node *BNode) setPtr(idx uint16, val uint64) {
    assert(idx < node.nkeys())
    pos := HEADER + 8*idx
    binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offsetPos returns the position of the offset for the given index.
func offsetPos(node BNode, idx uint16) uint16 {
    assert(1 <= idx && idx <= node.nkeys())
    return HEADER + 8*node.nkeys() + 2*(idx-1)
}

// Get offset at a specific index
func (node *BNode) getOffset(idx uint16) uint16 {
    if idx == 0 {
        return 0
    }
    return binary.LittleEndian.Uint16(node.data[offsetPos(*node, idx):])
}

// Set offset at a specific index
func (node *BNode) setOffset(idx uint16, offset uint16) {
    binary.LittleEndian.PutUint16(node.data[offsetPos(*node, idx):], offset)
}

// Get the position of a key-value pair
func (node BNode) kvPos(idx uint16) uint16 {
    assert(idx <= node.nkeys())
    return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

// Get the key at a specific index
func (node BNode) getKey(idx uint16) []byte {
    assert(idx < node.nkeys())
    pos := node.kvPos(idx)
    klen := binary.LittleEndian.Uint16(node.data[pos:])
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
    return node.data[pos+4+klen : pos+4+klen+vlen]
}

// Get the size of the node in bytes
func (node BNode) nbytes() uint16 {
    return node.kvPos(node.nkeys())
}

// Lookup the first kid node whose range intersects the key
func nodeLookupLE(node BNode, key []byte) uint16 {
    nkeys := node.nkeys()
    if nkeys == 0 {
        return 0 // Return 0 for empty nodes
    }
    found := uint16(0)
    for i := uint16(1); i < nkeys; i++ {
        cmp := bytes.Compare(node.getKey(i), key)
        if cmp <= 0 {
            found = i
        }
        if cmp >= 0 {
            break
        }
    }
    return found
}

// Insert a new key into a leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
    new.setHeader(BNODE_LEAF, old.nkeys()+1)

    // Copy keys and values before the insertion point
    nodeAppendRange(new, old, 0, 0, idx)

    // Insert the new key-value pair
    nodeAppendKV(new, idx, 0, key, val)

    // Copy keys and values after the insertion point
    nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// Copy multiple KVs into the position
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
    assert(srcOld+n <= old.nkeys())
    assert(dstNew+n <= new.nkeys())
    if n == 0 {
        return
    }
    // Copy pointers
    for i := uint16(0); i < n; i++ {
        new.setPtr(dstNew+i, old.getPtr(srcOld+i))
    }
    // Copy offsets
    dstBegin := new.getOffset(dstNew)
    srcBegin := old.getOffset(srcOld)
    for i := uint16(1); i <= n; i++ {
        offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
        new.setOffset(dstNew+i, offset)
    }
    // Copy KVs
    begin := old.kvPos(srcOld)
    end := old.kvPos(srcOld + n)
    copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

// Copy a KV into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
    // Set pointer
    new.setPtr(idx, ptr)

    // Calculate the position for the new key-value pair
    pos := new.kvPos(idx)

    // Write the key and value lengths
    binary.LittleEndian.PutUint16(new.data[pos:], uint16(len(key)))
    binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))

    // Copy the key and value data
    copy(new.data[pos+4:], key)
    copy(new.data[pos+4+uint16(len(key)):], val)

    // Update the offset for the next key
    new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
}

// Insert a KV into a node
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
    new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
    idx := nodeLookupLE(node, key)
    fmt.Printf("Inserting key: %s, idx: %d, nkeys: %d\n", key, idx, node.nkeys()) // Debugging print

    // Ensure idx is within bounds
    if idx >= node.nkeys() {
        idx = node.nkeys() // Insert at the end
    }

    switch node.btype() {
    case BNODE_LEAF:
        if node.nkeys() > 0 && bytes.Equal(key, node.getKey(idx)) {
            // Update existing key
            leafUpdate(new, node, idx, key, val)
        } else {
            // Insert new key
            leafInsert(new, node, idx, key, val)
        }
    case BNODE_NODE:
        // Insert into internal node
        nodeInsert(tree, new, node, idx, key, val)
    default:
        panic("bad node!")
    }
    return new
}

// Update a key in a leaf node
func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
    new.setHeader(BNODE_LEAF, old.nkeys())
    nodeAppendRange(new, old, 0, 0, idx)
    nodeAppendKV(new, idx, 0, key, val)
    nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

// Insert into an internal node
func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
    kptr := node.getPtr(idx)
    knode := tree.get(kptr)
    tree.del(kptr)
    knode = treeInsert(tree, knode, key, val)
    nsplit, splited := nodeSplit3(knode)
    nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

// Replace a kid node with multiple nodes
func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
    inc := uint16(len(kids))
    new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
    nodeAppendRange(new, old, 0, 0, idx)
    for i, node := range kids {
        nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
    }
    nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// Split a node into two
func nodeSplit2(left BNode, right BNode, old BNode) {
    // Implementation omitted for brevity
}

// Split a node into 1-3 nodes
func nodeSplit3(old BNode) (uint16, [3]BNode) {
    if old.nbytes() <= BTREE_PAGE_SIZE {
        old.data = old.data[:BTREE_PAGE_SIZE]
        return 1, [3]BNode{old}
    }
    left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)}
    right := BNode{make([]byte, BTREE_PAGE_SIZE)}
    nodeSplit2(left, right, old)
    if left.nbytes() <= BTREE_PAGE_SIZE {
        left.data = left.data[:BTREE_PAGE_SIZE]
        return 2, [3]BNode{left, right}
    }
    leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
    middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
    nodeSplit2(leftleft, middle, left)
    assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
    return 3, [3]BNode{leftleft, middle, right}
}

func main() {
    // Initialize the B-tree
    tree := &BTree{
        root: 1,
        get:  func(uint64) BNode { return BNode{} }, // Placeholder
        new:  func(BNode) uint64 { return 0 },       // Placeholder
        del:  func(uint64) {},                       // Placeholder
    }

    // Initialize an empty node
    node := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    node.setHeader(BNODE_LEAF, 0) // Initialize as an empty leaf node

    // Insert some key-value pairs
    node = treeInsert(tree, node, []byte("name"), []byte("Amir"))
    node = treeInsert(tree, node, []byte("age"), []byte("25"))

    // Retrieve and print values
    fmt.Println("Key: name, Value:", string(node.getVal(0)))
    fmt.Println("Key: age, Value:", string(node.getVal(1)))
}