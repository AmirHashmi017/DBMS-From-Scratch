#include <gtest/gtest.h>
#include "storage/bptree.h"

TEST(BPlusTreeTest, InsertAndSearch) {
    BPlusTree tree("test.idx");
    tree.insert(42, 0);
    auto result = tree.search(42);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], 0);
}