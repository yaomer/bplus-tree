#ifndef _BPLUS_TREE_LOG_H
#define _BPLUS_TREE_LOG_H

#include <string>
#include <map>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <thread>

#include "common.h"

namespace bpdb {

class DB;

enum { LOG_TYPE_INSERT, LOG_TYPE_ERASE };

class redo_log {
public:
    redo_log(DB *db) : db(db), quit_cleaner(false), cleaner([this]{ this->clean_handler(); }) {  }
    void init();
    void append(char type, const std::string *key, const std::string *value = nullptr);
    void quit_check_point();
private:
    void try_recovery();
    void clean_handler();
    void replay(off_t checkpoint);
    void check_point();
    void write_check_point(off_t checkpoint);
    DB *db;
    int log_fd;
    int check_point_fd;
    std::string log_file;
    std::string check_point_file;
    bool recovery = false;
    std::mutex log_mtx;
    std::mutex check_point_mtx;
    std::condition_variable cv;
    std::atomic_bool quit_cleaner;
    std::thread cleaner;
    // LSN(Log Sequence Number)
    off_t lsn = 0;
};
}

#endif // _BPLUS_TREE_LOG_H
