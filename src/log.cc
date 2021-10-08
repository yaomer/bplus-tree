#include <sys/uio.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "db.h"
#include "codec.h"

using namespace bpdb;

void redo_log::init()
{
    log_file = db->dbname + "redo.log";
    check_point_file = db->dbname + "checkpoint";
    if ((log_fd = open(log_file.c_str(), O_RDWR | O_APPEND)) < 0) {
        if (errno != ENOENT) {
            panic("redo_log: open(%s): %s", log_file.c_str(), strerror(errno));
        }
    } else {
        try_recovery();
    }
    log_fd = open(log_file.c_str(), O_RDWR | O_APPEND | O_CREAT, 0666);
    check_point_fd = open(check_point_file.c_str(), O_RDWR | O_TRUNC | O_CREAT, 0666);
}

void redo_log::try_recovery()
{
    off_t checkpoint;
    int cp_fd = open(check_point_file.c_str(), O_RDONLY);
    if (cp_fd < 0) checkpoint = 0;
    else read(cp_fd, &checkpoint, sizeof(checkpoint));
    lsn = checkpoint;
    replay(checkpoint);
    check_point();
    unlink(log_file.c_str());
    unlink(check_point_file.c_str());
    close(log_fd);
    close(check_point_fd);
    lsn = 0;
}

void redo_log::append(char type, const std::string *key, const std::string *value)
{
    lock_t lk(log_mtx);
    uint8_t keylen;
    uint32_t valuelen;
    struct iovec iov[5];
    iov[0].iov_base = &type;
    iov[0].iov_len = sizeof(type);
    switch (type) {
    case LOG_TYPE_INSERT:
        keylen = key->size();
        iov[1].iov_base = &keylen;
        iov[1].iov_len = sizeof(keylen);
        iov[2].iov_base = const_cast<char*>(key->data());
        iov[2].iov_len = keylen;
        valuelen = value->size();
        iov[3].iov_base = &valuelen;
        iov[3].iov_len = sizeof(valuelen);
        iov[4].iov_base = const_cast<char*>(value->data());
        iov[4].iov_len = valuelen;
        if (!recovery) {
            writev(log_fd, iov, 5);
            fsync(log_fd);
        }
        lsn += sizeof(type) + sizeof(keylen) + keylen + sizeof(valuelen) + valuelen;
        break;
    case LOG_TYPE_ERASE:
        keylen = key->size();
        iov[1].iov_base = &keylen;
        iov[1].iov_len = sizeof(keylen);
        iov[2].iov_base = const_cast<char*>(key->data());
        iov[2].iov_len = keylen;
        if (!recovery) {
            writev(log_fd, iov, 3);
            fsync(log_fd);
        }
        lsn += sizeof(type) + sizeof(keylen) + keylen;
        break;
    }
}

void redo_log::replay(off_t checkpoint)
{
    recovery = true;
    int pagesize = getpagesize();
    int n = checkpoint / pagesize;
    int off = checkpoint % pagesize;
    struct stat st;
    fstat(log_fd, &st);
    if (st.st_size == 0) return;
    off_t map_start = n * pagesize;
    size_t map_size = st.st_size - map_start;
    void *start = mmap(nullptr, map_size, PROT_READ, MAP_SHARED, log_fd, map_start);
    char *ptr = reinterpret_cast<char*>(start) + off;
    char *end = ptr + map_size - off;
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
    munmap(start, map_size);
    recovery = false;
}

void redo_log::check_point()
{
    cv.notify_one();
}

void redo_log::quit_check_point()
{
    quit_cleaner = true;
    check_point();
    if (cleaner.joinable())
        cleaner.join();
}

void redo_log::clean_handler()
{
    while (!quit_cleaner) {
        std::unique_lock<std::mutex> ulock(check_point_mtx);
        cv.wait_for(ulock, std::chrono::seconds(10));
        db->is_check_point = true;
        while (db->sync_check_point > 0) ;
        db->translation_table.flush();
        write_check_point(lsn);
        db->is_check_point = false;
    }
}

void redo_log::write_check_point(off_t checkpoint)
{
    lseek(check_point_fd, 0, SEEK_SET);
    write(check_point_fd, &checkpoint, sizeof(checkpoint));
    fsync(check_point_fd);
}
