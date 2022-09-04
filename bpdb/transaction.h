#ifndef _BPLUS_TREE_TRANSACTION_H
#define _BPLUS_TREE_TRANSACTION_H

#include <set>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <map>

#include "transaction_lock.h"
#include "version.h"

namespace bpdb {

class DB;
class transaction_manager;

struct readview {
    bool is_visibility(trx_id_t data_id);
    std::vector<trx_id_t> trx_ids;
    trx_id_t up_trx_id;
    trx_id_t create_trx_id;
};

class transaction {
public:
    transaction() : db(nullptr), trx_id(0) {  }
    ~transaction();
    transaction(const transaction&) = delete;
    transaction& operator=(const transaction&) = delete;
    status find(const std::string& key, std::string *value);
    status insert(const std::string& key, const std::string& value);
    status update(const std::string& key, const std::string& value);
    void erase(const std::string& key);
    void commit();
    void rollback();
private:
    void record(char op, const std::string& key, value_t *value = nullptr);
    void end();
    void wait_commit();
    bool is_visibility(trx_id_t data_id) { return view->is_visibility(data_id); }

    struct undo_log {
        undo_log(char op, trx_id_t xid, const std::string& key, const std::string& value)
            : op(op), trx_id(xid), key(key), value(value) {  }
        char op;
        trx_id_t trx_id;
        std::string key;
        std::string value;
    };

    DB *db;
    trx_id_t trx_id;
    readview *view = nullptr;
    std::stack<undo_log> roll_logs;
    std::unordered_set<std::string> xlock_keys;
    std::unordered_set<version_info*> version_set;
    std::mutex latch;
    std::atomic_int trx_sync_point = 0;
    bool committed = false;
    friend class DB;
    friend class transaction_manager;
    friend class versions;
};

class transaction_manager {
public:
    transaction_manager(DB *db) : db(db) {  }
    transaction_manager(const transaction_manager&) = delete;
    transaction_manager& operator=(const transaction_manager&) = delete;
    void init();
    void clear();
    transaction *begin();
    bool have_active_transaction();
    void set_blocking(bool isblock) { blocking = isblock; }
    void clear_xid_file();
    std::set<trx_id_t> get_xid_set();
private:
    void write_xid(trx_id_t xid);
    void write_trx_id(trx_id_t trx_id);
    std::set<trx_id_t> get_xid_set(const std::string& file);

    readview *build_readview(trx_id_t trx_id);

    DB *db;
    trx_id_t g_trx_id = 0;
    std::map<trx_id_t, transaction*> active_trx_map;
    std::mutex trx_latch;
    std::string info_file;
    int info_fd;
    std::string xid_file;
    int xid_fd;
    // 阻塞生成新事务
    std::atomic_bool blocking = false;
    transaction_locker locker;
    versions versions;
    friend class transaction;
};
}

#endif // _BPLUS_TREE_TRANSACTION_H
