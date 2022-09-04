#ifndef _BPLUS_TREE_VERSION_H
#define _BPLUS_TREE_VERSION_H

#include <unordered_map>
#include <list>
#include <future>

#include "common.h"

namespace bpdb {

class transaction;
class versions;

class version_info {
public:
    version_info(trx_id_t trx_id, const std::string& value)
        : trx_id(trx_id), value(value), refcnt(0) {  }
    void ref() { ++refcnt; }
    void unref() { --refcnt; }
    const std::string& get_value() { return value; }
private:
    trx_id_t trx_id;
    std::string value;
    std::atomic_int refcnt;
    friend class versions;
};

class versions {
public:
    versions();
    ~versions();
    void add(const std::string& key, const std::string& value, trx_id_t trx_id);
    version_info *get(const std::string& key, transaction *tx);
private:
    void purge();

    struct version_map {
        std::shared_mutex mtx;
        std::unordered_map<std::string, std::list<std::unique_ptr<version_info>>> keys;
    };

    std::vector<std::unique_ptr<version_map>> version_maps;
    std::atomic_size_t memory_usage;
    std::future<void> purge_future;
};
}

#endif // _BPLUS_TREE_VERSION_H
