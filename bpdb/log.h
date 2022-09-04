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

class logger {
public:
    logger(DB *db) : db(db),
        quit_sync_logger(false), sync_logger([this]{ this->sync_log_handler(); }), sync_wal(false),
        quit_cleaner(false), cleaner([this]{ this->clean_handler(); }) {  }
    logger(const logger&) = delete;
    logger& operator=(const logger&) = delete;
    void init();
    void append_wal(char type, const std::string& key, value_t *value = nullptr,
                    std::string *realval = nullptr);
    void flush_wal(bool wait = false);
    void check_point();
    void quit_check_point();
private:
    void open_log_file();
    void sync_log_handler();
    void clean_handler();
    void replay();

    void format_wal(char type, const std::string& key, value_t *value, std::string *realval);

    DB *db;
    int log_fd;
    std::string log_file;
    bool recovery = false;
    std::mutex log_mtx;
    std::condition_variable log_cv;
    std::atomic_bool quit_sync_logger;
    std::thread sync_logger;
    std::atomic_bool sync_wal;
    std::string write_buf, flush_buf;
    std::mutex check_point_mtx;
    std::condition_variable check_point_cv;
    std::atomic_bool quit_cleaner;
    std::thread cleaner;
    // LSN(Log Sequence Number)
    // off_t lsn = 0; // We don't need for now
};
}

#endif // _BPLUS_TREE_LOG_H
