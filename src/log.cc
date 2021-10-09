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
        sync_log(iov, 5);
        break;
    case LOG_TYPE_ERASE:
        keylen = key->size();
        iov[1].iov_base = &keylen;
        iov[1].iov_len = sizeof(keylen);
        iov[2].iov_base = const_cast<char*>(key->data());
        iov[2].iov_len = keylen;
        sync_log(iov, 3);
        break;
    }
}

void logger::sync_log(struct iovec *iov, int len)
{
    lock_t lk(log_mtx);
    if (!recovery) {
        writev(log_fd, iov, len);
        fsync(log_fd);
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
    cv.notify_one();
}

void logger::quit_check_point()
{
    quit_cleaner = true;
    check_point();
    if (cleaner.joinable())
        cleaner.join();
}

void logger::clean_handler()
{
    while (!quit_cleaner) {
        std::unique_lock<std::mutex> ulock(check_point_mtx);
        cv.wait_for(ulock, std::chrono::seconds(10));
        db->is_check_point = true;
        while (db->sync_check_point > 0) ;
        db->translation_table.flush();
        unlink(log_file.c_str());
        close(log_fd);
        if (!quit_cleaner) open_log_file();
        db->is_check_point = false;
    }
}
