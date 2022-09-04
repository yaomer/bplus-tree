#include <sys/uio.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "db.h"
#include "codec.h"
#include "util.h"

namespace bpdb {

void logger::init()
{
    log_file = db->dbname + "redo.log";
    if ((log_fd = open(log_file.c_str(), O_RDWR | O_APPEND)) < 0) {
        if (errno != ENOENT) {
            panic("logger::init: open(%s): %s", log_file.c_str(), strerror(errno));
        }
        open_log_file();
    } else {
        replay();
        check_point();
    }
}

void logger::open_log_file()
{
    log_fd = open(log_file.c_str(), O_RDWR | O_APPEND | O_CREAT, 0666);
}

void logger::append_wal(char type, const std::string& key, value_t *value, std::string *realval)
{
    int cur_buf_size;
    if (recovery) return;
    {
        lock_t lk(log_mtx);
        format_wal(type, key, value, realval);
        cur_buf_size = write_buf.size();
    }
    if (db->ops.wal_sync == 0) {
        flush_wal();
    } else if (db->ops.wal_sync == 1) {
        if (cur_buf_size >= db->ops.wal_sync_buffer_size) {
            flush_wal();
        }
    }
}

void logger::format_wal(char type, const std::string& key, value_t *value, std::string *realval)
{
    write_buf.append(1, type);
    encode64(write_buf, value->trx_id);
    encode8(write_buf, key.size());
    write_buf.append(key);
    if (type == Insert || type == Update) {
        if (realval) {
            encode32(write_buf, realval->size());
            write_buf.append(*realval);
        } else {
            encode32(write_buf, value->val->size());
            write_buf.append(*value->val);
        }
    }
}

void logger::flush_wal(bool wait)
{
    sync_wal = true;
    log_cv.notify_one();
    if (wait) while (sync_wal) ;
}

void logger::sync_log_handler()
{
    while (!quit_sync_logger) {
        {
            std::unique_lock<std::mutex> ulock(log_mtx);
            log_cv.wait_for(ulock, std::chrono::seconds(db->ops.wal_wake_interval));
            if (write_buf.empty()) { sync_wal = false; continue; }
            write_buf.swap(flush_buf);
        }
        write(log_fd, flush_buf.data(), flush_buf.size());
        sync_fd(log_fd);
        flush_buf.clear();
        sync_wal = false;
    }
}

void logger::replay()
{
    struct stat st;
    fstat(log_fd, &st);
    if (st.st_size == 0) return;
    recovery = true;
    auto xid_set = db->trmgr.get_xid_set();
    void *start = mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, log_fd, 0);
    char *ptr = reinterpret_cast<char*>(start);
    char *end = ptr + st.st_size;
    std::string key, value;
    while (ptr < end) {
        char type = *ptr++;
        trx_id_t xid = decode64(&ptr);
        uint8_t keylen = decode8(&ptr);
        key.assign(ptr, keylen);
        ptr += keylen;
        if (type == Insert || type == Update) {
            uint32_t valuelen = decode32(&ptr);
            value.assign(ptr, valuelen);
            ptr += valuelen;
            if (!xid_set.count(xid)) continue;
            if (type == Insert) db->insert(key, value);
            else db->update(key, value);
        } else if (type == Delete) {
            if (!xid_set.count(xid)) continue;
            db->erase(key);
        }
    }
    munmap(start, st.st_size);
    recovery = false;
}

void logger::check_point()
{
    db->Checkpoint = true;
    // 我们必须保证wal先于数据落盘
    flush_wal(true);
    check_point_cv.notify_one();
}

void logger::quit_check_point()
{
    quit_sync_logger = true;
    quit_cleaner = true;
    check_point();
    if (sync_logger.joinable())
        sync_logger.join();
    if (cleaner.joinable())
        cleaner.join();
}

void logger::clean_handler()
{
    while (!quit_cleaner) {
        std::unique_lock<std::mutex> ulock(check_point_mtx);
        check_point_cv.wait_for(ulock, std::chrono::seconds(db->ops.check_point_interval));
        if (!quit_cleaner && db->trmgr.have_active_transaction()) {
            // 阻塞生成新事务，并等待所有活跃事务提交
            db->trmgr.set_blocking(true);
            continue;
        }
        if (!db->Checkpoint) check_point();
        if (db->Rebuild) {
            db->Checkpoint = false;
            continue;
        }
        db->wait_sync_point(false);
        db->translation_table.flush();
        unlink(log_file.c_str());
        close(log_fd);
        if (!quit_cleaner) {
            open_log_file();
            db->trmgr.clear_xid_file();
            db->trmgr.set_blocking(false);
        }
        db->Checkpoint = false;
    }
}

} // namespace bpdb
