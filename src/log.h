#ifndef _BPLUS_TREE_LOG_H
#define _BPLUS_TREE_LOG_H

#include <string>
#include <map>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <thread>

#include <sys/uio.h>

#include "common.h"

namespace bpdb {

class DB;

enum { LOG_TYPE_INSERT, LOG_TYPE_ERASE };

class logger {
public:
    logger(DB *db) : db(db), quit_cleaner(false), cleaner([this]{ this->clean_handler(); }) {  }
    void init();
    void append(char type, const std::string *key, const std::string *value = nullptr);
    void quit_check_point();
private:
    void open_log_file();
    void sync_log(struct iovec *iov, int len);
    void clean_handler();
    void replay();
    void check_point();
    DB *db;
    int log_fd;
    std::string log_file;
    bool recovery = false;
    std::mutex log_mtx;
    std::mutex check_point_mtx;
    std::condition_variable cv;
    std::atomic_bool quit_cleaner;
    std::thread cleaner;
    // LSN(Log Sequence Number)
    // off_t lsn = 0; // We don't need for now
};
}

#endif // _BPLUS_TREE_LOG_H
