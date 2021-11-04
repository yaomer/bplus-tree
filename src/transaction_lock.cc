#include "transaction_lock.h"

namespace bpdb {

transaction_locker::transaction_locker() : stripes(16)
{
    lock_maps.resize(stripes);
    for (int i = 0; i < stripes; i++)
        lock_maps[i].reset(new lock_map());
}

transaction_locker::~transaction_locker()
{
}

int transaction_locker::get_stripe(const std::string& key)
{
    return std::hash<std::string>()(key) % stripes;
}

void transaction_locker::lock(trx_id_t trx_id, const std::string& key, bool exclusive)
{
    int i = get_stripe(key);
    auto& lk_map = *lock_maps[i];
    lk_map.mtx.lock();
    auto it = lk_map.keys.find(key);
    if (it != lk_map.keys.end()) {
        auto& lk_info = it->second;
        if (lk_info.trx_ids.size() == 1 && lk_info.trx_ids[0] == trx_id) {
            // hold by self, just take it
            lk_info.exclusive = exclusive;
            lk_map.mtx.unlock();
        } else if (exclusive || lk_info.exclusive) {
            // 1) require XL
            // 2) require SL, lk_info hold XL
            lk_map.mtx.unlock();
            std::unique_lock<std::mutex> ulk(lk_map.mtx);
            lk_info.waiters++;
            lk_map.cv.wait(ulk, [&lk_info]{ return lk_info.trx_ids.empty(); });
            lk_info.exclusive = exclusive;
            lk_info.trx_ids.push_back(trx_id);
            lk_info.waiters--;
        } else {
            // join the reader list
            lk_info.trx_ids.push_back(trx_id);
            lk_map.mtx.unlock();
        }
    } else {
        lock_info lk_info;
        lk_info.exclusive = exclusive;
        lk_info.trx_ids.push_back(trx_id);
        lk_map.keys.emplace(key, std::move(lk_info));
        lk_map.mtx.unlock();
    }
}

void transaction_locker::unlock(trx_id_t trx_id, const std::string& key)
{
    int i = get_stripe(key);
    auto& lk_map = *lock_maps[i];
    lk_map.mtx.lock();
    auto it = lk_map.keys.find(key);
    assert(it != lk_map.keys.end());
    auto& lk_info = it->second;
    for (auto& id : lk_info.trx_ids) {
        if (id == trx_id) {
            std::swap(id, lk_info.trx_ids.back());
            lk_info.trx_ids.pop_back();
            if (lk_info.trx_ids.empty()) {
                if (lk_info.waiters > 0) {
                    lk_map.mtx.unlock();
                    lk_map.cv.notify_all();
                    return;
                }
                lk_map.keys.erase(key);
            }
            break;
        }
    }
    lk_map.mtx.unlock();
}

} // namespace bpdb
