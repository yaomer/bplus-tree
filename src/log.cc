#include <sys/uio.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "db.h"
#include "codec.h"

using namespace bpdb;

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

void logger::append(char type, const std::string *key, const std::string *value)
{
    int cur_buf_size;
    if (recovery) return;
    {
        lock_t lk(log_mtx);
        write_buf.append(1, type);
        encode8(write_buf, key->size());
        write_buf.append(*key);
        if (type == LOG_TYPE_INSERT) {
            encode32(write_buf, value->size());
            write_buf.append(*value);
        }
        cur_buf_size = write_buf.size();
    }
    if (db->ops.wal_sync == 0) {
        log_cv.notify_one();
    } else if (db->ops.wal_sync == 1) {
        if (cur_buf_size >= db->ops.wal_sync_buffer_size) {
            log_cv.notify_one();
        }
    }
}

void logger::sync_log_handler()
{
    while (!quit_sync_logger) {
        {
            std::unique_lock<std::mutex> ulock(log_mtx);
            log_cv.wait_for(ulock, std::chrono::seconds(db->ops.wal_wake_interval));
            if (write_buf.empty()) continue;
            write_buf.swap(flush_buf);
        }
        write(log_fd, flush_buf.data(), flush_buf.size());
        fsync(log_fd);
        flush_buf.clear();
    }
}

void logger::replay()
{
    recovery = true;
    struct stat st;
    fstat(log_fd, &st);
    if (st.st_size == 0) return;
    void *start = mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, log_fd, 0);
    char *ptr = reinterpret_cast<char*>(start);
    char *end = ptr + st.st_size;
    std::string key, value;
    while (ptr < end) {
        char type = *ptr++;
        uint8_t keylen = decode8(&ptr);
        key.assign(ptr, keylen);
        ptr += keylen;
        if (type == LOG_TYPE_INSERT) {
            uint32_t valuelen = decode32(&ptr);
            value.assign(ptr, valuelen);
            ptr += valuelen;
            db->insert(key, value);
        } else if (type == LOG_TYPE_ERASE) {
            db->erase(key);
        } else {
            panic("replay: unknown type");
        }
    }
    munmap(start, st.st_size);
    recovery = false;
}

void logger::check_point()
{
    log_cv.notify_one();
    check_point_cv.notify_one();
    db->is_check_point = true;
}

void logger::quit_check_point()
{
    quit_sync_logger = true;
    log_cv.notify_one();
    if (sync_logger.joinable())
        sync_logger.join();
    quit_cleaner = true;
    check_point();
    if (cleaner.joinable())
        cleaner.join();
}

void logger::clean_handler()
{
    while (!quit_cleaner) {
        std::unique_lock<std::mutex> ulock(check_point_mtx);
        check_point_cv.wait_for(ulock, std::chrono::seconds(db->ops.check_point_interval));
        db->is_check_point = true;
        if (db->is_rebuild) {
            db->is_check_point = false;
            continue;
        }
        db->wait_sync_point(false);
        db->translation_table.flush();
        unlink(log_file.c_str());
        close(log_fd);
        if (!quit_cleaner) open_log_file();
        db->is_check_point = false;
    }
}
