Usage
---
```cpp
#include <bpdb/db.h>

#include <iostream>

void traveldb(bpdb::DB& db)
{
    auto it = db.new_iterator();
    for (it.seek_to_first(); it.valid(); it.next()) {
        std::cout << "(" << it.key().c_str() << ", " << it.value().c_str() << ")\n";
    }
}

int main(int argc, char *argv[])
{
    bpdb::DB db;
    // for insert
    db.insert("key1", "value1");
    db.insert("key2", "value2");
    db.insert("key3", "value3");
    db.insert("key4", "value4");
    traveldb(db);
    // for update
    db.insert("key1", "hello1");
    // for search
    std::string value;
    auto s = db.find("key1", &value);
    if (s) std::cout << value.c_str() << "\n";
    else std::cout << "not found\n";
    // for erase
    db.erase("key2");
    db.erase("key4");
    traveldb(db);
}
```
