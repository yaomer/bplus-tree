#ifndef _BPLUS_TREE_TRANSACTION_LOCK_H
#define _BPLUS_TREE_TRANSACTION_LOCK_H

#include <string>
#include <vector>
#include <unordered_map>

#include "common.h"

namespace bpdb {

class transaction_locker {
public:
    transaction_locker();
    ~transaction_locker();
    transaction_locker(const transaction_locker&) = delete;
    transaction_locker& operator=(const transaction_locker&) = delete;
    void lock(trx_id_t trx_id, const std::string& key, bool exclusive);
    void unlock(trx_id_t trx_id, const std::string& key);
private:
    struct lock_info {
        bool exclusive; // is XL?
        int waiters = 0;
        std::vector<trx_id_t> trx_ids;
    };
    struct lock_map {
        std::mutex mtx;
        std::condition_variable cv;
        std::unordered_map<std::string, lock_info> keys;
    };
    int get_stripe(const std::string& key);
    std::vector<std::unique_ptr<lock_map>> lock_maps;
    const int stripes;
};
}

#endif // _BPLUS_TREE_TRANSACTION_LOCK_H
