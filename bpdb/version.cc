#include "version.h"
#include "transaction.h"

namespace bpdb {

static const int stripes = 64;

static const size_t memory_threshold = 1024 * 1024 * 16;

versions::versions() : memory_usage(0)
{
    version_maps.resize(stripes);
    for (int i = 0; i < stripes; i++)
        version_maps[i].reset(new version_map());
}

versions::~versions() = default;

static int get_stripes(const std::string& key)
{
    return std::hash<std::string>()(key) % stripes;
}

void versions::add(const std::string& key, const std::string& value, trx_id_t trx_id)
{
    int i = get_stripes(key);
    auto& vmap = *version_maps[i];
    {
        wlock_t wlk(vmap.mtx);
        auto it = vmap.keys.find(key);
        if (it != vmap.keys.end()) {
            it->second.emplace_front(new version_info(trx_id, value));
        } else {
            memory_usage.fetch_add(sizeof(key) + key.size());
            vmap.keys[key].emplace_front(new version_info(trx_id, value));
        }
    }
    memory_usage.fetch_add(sizeof(version_info) + value.size(), std::memory_order_relaxed);
    if (!purge_future.valid() && memory_usage.load(std::memory_order_relaxed) >= memory_threshold) {
        purge_future = std::async(std::launch::async, [this]{ this->purge(); });
    }
}

version_info *versions::get(const std::string& key, transaction *tx)
{
    int i = get_stripes(key);
    auto& vmap = *version_maps[i];
    rlock_t rlk(vmap.mtx);
    auto it = vmap.keys.find(key);
    if (it == vmap.keys.end()) return nullptr;
    for (auto& vinfo : it->second) {
        if (tx->is_visibility(vinfo->trx_id)) {
            return vinfo.get();
        }
    }
    return nullptr;
}

void versions::purge()
{
    for (int i = 0; i < stripes; i++) {
        auto& vmap = *version_maps[i];
        wlock_t wlk(vmap.mtx);
        for (auto it = vmap.keys.begin(); it != vmap.keys.end(); ) {
            auto& vlist = it->second;
            for (auto it = vlist.begin(); it != vlist.end(); ) {
                if ((*it)->refcnt == 0) {
                    memory_usage.fetch_sub(sizeof(version_info) + (*it)->value.size(), std::memory_order_relaxed);
                    it = vlist.erase(it);
                } else {
                    ++it;
                }
            }
            if (vlist.empty()) {
                memory_usage.fetch_sub(sizeof(it->first) + it->first.size(), std::memory_order_relaxed);
                it = vmap.keys.erase(it);
            } else {
                ++it;
            }
        }
    }
}

} // namespace bpdb
