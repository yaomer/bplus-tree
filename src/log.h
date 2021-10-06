#ifndef _BPLUS_TREE_LOG_H
#define _BPLUS_TREE_LOG_H

#include <string>
#include <map>
#include <unordered_map>

#include "common.h"

namespace bpdb {

class DB;

enum { LOG_TYPE_INSERT, LOG_TYPE_ERASE };
enum { FUZZY_CHECK_POINT, SHARP_CHECK_POINT };

enum { FUZZY_CHECK_POINT_PAGES = 100 };

enum { CHECK_POINT_INTERVAL = 1 };

class redo_log {
public:
    redo_log(DB *db) : db(db) {  }
    void init();
    void append(char type, const std::string *key, const std::string *value = nullptr);
    void check_point(char type);
    void put_change_node(node *node);
    void put_complete();
    void remove_node(node *node);
private:
    void try_recovery();
    void replay(off_t checkpoint);
    void write_check_point(off_t checkpoint);
    void fuzzy_check_point();
    void sharp_check_point();
    void flush_node(node *node);
    DB *db;
    int log_fd;
    int check_point_fd;
    std::string log_file;
    std::string check_point_file;
    bool recovery = false;
    // LSN(Log Sequence Number)
    off_t cur_lsn = 0;
    off_t next_lsn = 0;
    // off_t log_file_size = 1024 * 1024 * 1024;
    struct modify_info { // 对应一次修改操作
        std::vector<node*> nodelist;
        header_t header;
    };
    // 所有的dirty node按lsn从小到大排列，以配合checkpoint刷盘
    std::map<off_t, modify_info> flush_list;
};
}

#endif // _BPLUS_TREE_LOG_H
