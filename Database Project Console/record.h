#ifndef RECORD_H
#define RECORD_H

#include <string>
#include <fstream>

struct Record {
    int id;
    std::string name;
    bool active;

    void serialize(std::ofstream& os) const;
    static Record deserialize(std::ifstream& is);
};

#endif