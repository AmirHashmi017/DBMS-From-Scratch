#ifndef PAGE_H
#define PAGE_H

#include <cstdint>

constexpr uint16_t PAGE_SIZE = 4096; // 4KB

struct PageHeader {
    uint16_t free_space;
    uint32_t next_page;
    uint8_t flags;
    uint8_t reserved[7]; // Padding
};

struct Page {
    PageHeader header;
    char data[PAGE_SIZE - sizeof(PageHeader)];
};

#endif