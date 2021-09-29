Usage
---
```cpp
#include <bpdb/db.h>

#include <iostream>

void traveldb(bplus_tree_db::DB& db)
{
    for (auto it = db.first(); it.valid(); it.next()) {
        std::cout << "(" << it.key()->c_str() << ", " << it.value()->c_str() << ")\n";
    }
}

int main(int argc, char *argv[])
{
    bplus_tree_db::DB db;
    // for insert
    db.insert("key1", "value1");
    db.insert("key2", "value2");
    db.insert("key3", "value3");
    db.insert("key4", "value4");
    traveldb(db);
    // for update
    db.insert("key1", "hello1");
    auto it = db.find("key1");
    if (it.valid()) std::cout << it.value()->c_str() << "\n";
    else std::cout << "not found\n";
    // for erase
    db.erase("key2");
    db.erase("key4");
    traveldb(db);
}
```
