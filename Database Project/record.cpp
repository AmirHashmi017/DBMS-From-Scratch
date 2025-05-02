#include "record.h"

using namespace std;

void Record::serialize(ofstream& os) const {
    os.write(reinterpret_cast<const char*>(&id), sizeof(id));
    int name_len = name.size();
    os.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
    os.write(name.c_str(), name_len);
    os.write(reinterpret_cast<const char*>(&active), sizeof(active));
}

Record Record::deserialize(ifstream& is) {
    Record record;
    is.read(reinterpret_cast<char*>(&record.id), sizeof(record.id));
    int name_len;
    is.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
    record.name.resize(name_len);
    is.read(&record.name[0], name_len);
    is.read(reinterpret_cast<char*>(&record.active), sizeof(record.active));
    return record;
}