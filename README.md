```
 _               _ _     
| |__  _ __   __| | |__  
| '_ \| '_ \ / _` | '_ \ 
| |_) | |_) | (_| | |_) |
|_.__/| .__/ \__,_|_.__/ 
      |_|                
```
Usage
---
#### Basic
```cpp
#include <bpdb/db.h>

#include <iostream>

using namespace std;

int main()
{
    bpdb::DB db(bpdb::options(), "tmpdb");
    db.insert("key", "value");
    auto s = db.insert("key", "value");
    if (s.is_exists()) cout << s.to_str().c_str() << "\n";
    db.update("key", "hello");
    string value;
    s = db.find("key", &value);
    if (s.is_ok()) cout << value.c_str() << "\n";
    db.erase("key");
    s = db.find("key", &value);
    if (s.is_not_found()) cout << s.to_str().c_str() << "\n";
}
```
#### Range
```cpp
void traveldb(bpdb::DB& db)
{
    auto *it = db.new_iterator();
    for (it->seek_to_first(); it->valid(); it->next()) {
        std::cout << "(" << it->key().c_str() << ", " << it->value().c_str() << ")\n";
    }
    delete it;
}

int main()
{
    bpdb::DB db(bpdb::options(), "tmpdb");
    for (int i = 0; i < 100; i++) {
        db.insert("key#" + to_string(i), "value#" + to_string(i));
    }
    traveldb(db);
}
```
#### Key Comparator
```cpp
void traveldb(bpdb::DB& db)
{
    auto *it = db.new_iterator();
    for (it->seek_to_first(); it->valid(); it->next()) {
        std::cout << "(" << it->key().c_str() << ", " << it->value().c_str() << ")\n";
    }
    delete it;
}

bool comparator(const bpdb::key_t& l, const bpdb::key_t& r)
{
   return atoi(strchr(l.c_str(), '#') + 1) < atoi(strchr(r.c_str(), '#') + 1);
}

int main()
{
    bpdb::options ops;
    ops.keycomp = comparator;
    bpdb::DB db(ops, "tmpdb");
    for (int i = 0; i < 100; i++) {
        db.insert("key#" + to_string(i), "value#" + to_string(i));
    }
    traveldb(db);
}
```
#### Transaction
```cpp
int main()
{
    bpdb::DB db(bpdb::options(), "tmpdb");
    auto tx = db.begin();
    tx->insert("key#1", "value#1");
    tx->insert("key#2", "value#2");
    // tx->rollback();
    tx->commit();
    delete tx;
}
```
