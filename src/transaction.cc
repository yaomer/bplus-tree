#include "transaction.h"
#include "db.h"
#include "codec.h"
#include "util.h"

#include <sys/stat.h>
#include <sys/mman.h>

// 简单的事务实现(isolation(RU))
// TODO: MVCC

namespace bpdb {

void transaction_manager::init()
{
    // 目前用于保存全局递增事务id
    info_file = db->dbname + "trx_info";
    // 保存自上一次checkpoint之后已提交的事务id
    xid_file = db->dbname + "trx_xid_list";
    xid_fd = open(xid_file.c_str(), O_RDWR | O_APPEND | O_CREAT, 0666);
    info_fd = open(info_file.c_str(), O_RDWR | O_APPEND | O_CREAT, 0666);
    auto trx_id_set = get_xid_set(info_file);
    if (!trx_id_set.empty()) {
        g_trx_id = *trx_id_set.rbegin();
    }
}

void transaction_manager::clear()
{
    // 进程退出时还有未提交的事务，就全部回滚
    // (一般是用户没调用commit() or rollback())
    active_trx_map.clear();
}

// 开启一个事务
transaction *transaction_manager::begin()
{
    if (blocking) while (blocking) ;
    transaction *tx = new transaction();
    tx->db = db;
    {
        lock_t lk(trx_latch);
        tx->trx_id = ++g_trx_id;
        active_trx_map.emplace(tx->trx_id, tx);
    }
    write_trx_id(tx->trx_id);
    return tx;
}

bool transaction_manager::have_active_transaction()
{
    lock_t lk(trx_latch);
    return !active_trx_map.empty();
}

transaction::~transaction()
{
    // 未执行完的事务就需要回滚
    if (!committed) rollback();
    if (view) delete view;
    lock_t lk(db->trmgr.trx_latch);
    db->trmgr.active_trx_map.erase(trx_id);
    if (db->trmgr.active_trx_map.empty() && db->trmgr.blocking)
        db->logger.check_point();
}

void transaction::end()
{
    for (auto& key : xlock_keys) {
        db->trmgr.locker.unlock(trx_id, key);
    }
    for (auto& vinfo : version_set) {
        vinfo->unref();
    }
    db->trmgr.write_xid(trx_id);
}

void transaction::wait_commit()
{
    if (trx_sync_point > 0)
        while (trx_sync_point > 0) ;
}

// 事务提交时，只需flush wal即可保证持久性
// 同样，因为wal中包含了相应的undo log，所以并不影响recovery
void transaction::commit()
{
    assert(!committed);
    lock_t lk(latch);
    committed = true;
    wait_commit();
    // Write Transaction?
    if (!roll_logs.empty()) {
        db->logger.flush_wal(true);
    }
    end();
}

// 回滚时，为了保证recovery时的正确性，我们再一次记录了wal
void transaction::rollback()
{
    assert(!committed);
    lock_t lk(latch);
    committed = true;
    wait_commit();
    while (!roll_logs.empty()) {
        auto& ulog = roll_logs.top();
        switch (ulog.op) {
        case Insert: db->insert(ulog.key, ulog.value); break;
        case Update: db->update(ulog.key, ulog.value); break;
        case Delete: db->erase(ulog.key); break;
        }
        roll_logs.pop();
    }
    end();
}

status transaction::find(const std::string& key, std::string *value)
{
    if (!view) {
        lock_t lk(latch);
        if (!view) view = db->trmgr.build_readview(trx_id);
    }
    assert(!committed);
    trx_sync_point++;
    {
        lock_t lk(latch);
        if (xlock_keys.count(key)) {
            auto s = db->find(key, value);
            trx_sync_point--;
            return s;
        }
    }
    auto vinfo = db->trmgr.versions.get(key, this);
    if (vinfo) {
        value->assign(vinfo->get_value());
        auto iter = version_set.emplace(vinfo);
        if (iter.second) vinfo->ref();
        trx_sync_point--;
        return status::ok();
    } else {
        auto s = db->find(key, value);
        trx_sync_point--;
        return s;
    }
}

status transaction::insert(const std::string& key, const std::string& value)
{
    assert(!committed);
    trx_sync_point++;
    auto s = db->insert(key, value, Insert, this);
    trx_sync_point--;
    return s;
}

status transaction::update(const std::string& key, const std::string& value)
{
    assert(!committed);
    trx_sync_point++;
    auto s = db->insert(key, value, Update, this);
    trx_sync_point--;
    return s;
}

void transaction::erase(const std::string& key)
{
    assert(!committed);
    trx_sync_point++;
    db->erase(key, this);
    trx_sync_point--;
}

// 我们会将undo log当作普通数据一样写入WAL中
// (因为undo log必须先于wal落盘，分别持久化会使情况变得相当复杂)
void transaction::record(char op, const std::string& key, value_t *value)
{
    std::string *realval = value->val;
    std::string saved_value;
    if (op != Delete && value->reallen > limit.over_value) {
        db->translation_table.load_real_value(value, &saved_value);
        realval = &saved_value;
    }
    db->logger.append_wal(op, key, value, realval);
    db->trmgr.locker.lock(trx_id, key, true);
    {
        lock_t lk(latch);
        xlock_keys.emplace(key);
        roll_logs.emplace(undo_log(op, trx_id, key, *realval));
    }
    db->trmgr.versions.add(key, *realval, value->trx_id);
}

// 针对同一个fd，多线程write(O_APPEND)是安全的

void transaction_manager::write_trx_id(trx_id_t trx_id)
{
    write(info_fd, &trx_id, sizeof(trx_id));
    sync_fd(info_fd);
}

void transaction_manager::write_xid(trx_id_t xid)
{
    write(xid_fd, &xid, sizeof(xid));
    sync_fd(xid_fd);
}

void transaction_manager::clear_xid_file()
{
    // 此时的xid_file不再被需要
    // 因此不必考虑删除与重新打开之间的原子性
    unlink(xid_file.c_str());
    close(xid_fd);
    xid_fd = open(xid_file.c_str(), O_RDWR | O_APPEND | O_CREAT, 0666);
    // info_file仍然是需要的，但我们想在这里截断它
    // 这需要保证原子性，最坏情形下，旧的info_file必须被保留
    char tmpfile[] = "tmp.XXXXX";
    mktemp(tmpfile);
    int fd = open(tmpfile, O_RDONLY);
    write(fd, &g_trx_id, sizeof(g_trx_id));
    sync_fd(fd);
    rename(tmpfile, info_file.c_str());
    close(info_fd);
    info_fd = fd;
}

std::set<trx_id_t> transaction_manager::get_xid_set()
{
    auto xid_set = get_xid_set(xid_file);
    // 不使用事务的单条语句的xid = 0
    // 可以认为是默认提交的
    xid_set.insert(0);
    return xid_set;
}

std::set<trx_id_t> transaction_manager::get_xid_set(const std::string& file)
{
    int fd = open(file.c_str(), O_RDONLY);
    if (fd < 0) {
        panic("get_xid_set: open(%s): %s", file.c_str(), strerror(errno));
    }
    struct stat st;
    fstat(fd, &st);
    std::set<trx_id_t> xid_set;
    if (st.st_size == 0) { close(fd); return xid_set; }
    void *start = mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    assert(start != MAP_FAILED);
    char *ptr = reinterpret_cast<char*>(start);
    char *end = ptr + st.st_size;
    while (ptr < end) {
        trx_id_t xid = *reinterpret_cast<trx_id_t*>(ptr);
        xid_set.insert(xid);
        ptr += sizeof(xid);
    }
    close(fd);
    return xid_set;
}

readview *transaction_manager::build_readview(trx_id_t trx_id)
{
    readview *view = new readview();
    {
        lock_t lk(trx_latch);
        for (auto& [trx_id, tx] : active_trx_map)
            view->trx_ids.push_back(trx_id);
    }
    view->create_trx_id = trx_id;
    view->up_trx_id = g_trx_id + 1;
    return view;
}

bool readview::is_visibility(trx_id_t data_id)
{
    if (data_id < trx_ids[0] || data_id == create_trx_id) return true;
    if (data_id >= up_trx_id) return false;
    return !std::binary_search(trx_ids.begin(), trx_ids.end(), data_id);
}

} // namespace bpdb
