#ifndef _BPLUS_TREE_DB_H
#define _BPLUS_TREE_DB_H

#include <string>
#include <vector>

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>

#include "disk.h"
#include "page.h"
#include "log.h"

namespace bpdb {

void panic(const char *fmt, ...);

typedef std::function<bool(const key_t&, const key_t&)> Comparator;

struct options {
    int page_size = 1024 * 16;
    int page_cache_slots = 1024;
    // 0: sync every log
    // 1: sync every `wal_sync_buffer_size`
    int wal_sync = 1;
    int wal_sync_buffer_size = 4096;
    // 每隔多久(s)唤醒后台sync-logger线程
    int wal_wake_interval = 1;
    // 默认每10(s)做一次check-point
    int check_point_interval = 10;
    Comparator keycomp;
};

class DB {
public:
    DB(const options& ops, const std::string& dbname)
        : ops(ops), dbname(dbname), translation_table(this), page_manager(this), logger(this)
    {
        check_options();
        init();
    }
    ~DB() { logger.quit_check_point(); }
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    class iterator {
    public:
        iterator(DB *db) : db(db), page_id(0), i(0) {  }
        bool valid();
        const std::string& key();
        const std::string& value();
        iterator& seek(const std::string& key);
        iterator& seek_to_first();
        iterator& seek_to_last();
        iterator& next();
        iterator& prev();
    private:
        DB *db;
        page_id_t page_id;
        int i;
        std::string saved_key;
        std::string saved_value;
    };

    iterator new_iterator() { return iterator(this); }
    bool find(const std::string& key, std::string *value);
    void insert(const std::string& key, const std::string& value);
    void erase(const std::string& key);
    void rebuild();
private:
    void init();
    void check_options();
    int open_db_file();

    bool is_main_thread() { return std::this_thread::get_id() == cur_tid; }
    int get_db_fd() { return is_main_thread() ? fd : open_db_file(); }
    void put_db_fd(int fd) { if (!is_main_thread()) close(fd); }

    void lock_header() { header_latch.lock(); }
    void unlock_header() { header_latch.unlock(); }

    // 简单轮询以等待后台check_point()结束
    void wait_if_check_point() { if (is_check_point) while(is_check_point) ; }

    node *to_node(page_id_t page_id) { return translation_table.to_node(page_id); }
    page_id_t to_page_id(node *node) { return translation_table.to_page_id(node); }

    int search(node *x, const key_t& key);
    bool check_limit(const std::string& key, const std::string& value);
    value_t *build_new_value(const std::string& value);

    std::pair<node*, int> find(node *x, const key_t& key);
    void insert(node *x, const key_t& key, value_t *value);
    void erase(node *x, const key_t& key, node *precursor);

    bool isfull(node *x, const key_t& key, value_t *value);
    void split(node *x, int i, const key_t& key);
    node *split(node *y, int type);
    enum { RIGHT_INSERT_SPLIT, LEFT_INSERT_SPLIT, MID_SPLIT };
    int get_split_type(node *x, const key_t& key);
    void link_leaf(node *z, node *y, int type);
    void update_header_in_insert(node *x, const key_t& key);

    node *get_precursor(node *x);
    void borrow_from_right(node *r, node *x, node *z, int i);
    void borrow_from_left(node *r, node *x, node *y, int i);
    void merge(node *y, node *x);

    bool less(const key_t& l, const key_t& r)
    {
        return comparator(l, r);
    }
    bool equal(const key_t& l, const key_t& r)
    {
        return !comparator(l, r) && !comparator(r, l);
    }

    int fd;
    options ops;
    std::string dbname;
    std::string dbfile;
    std::thread::id cur_tid;
    // 每个线程执行修改操作时先递增sync_check_point，修改完成后再递减
    // 我们在做check_point()之前要保证sync_check_point=0，以保证刷脏页时数据库状态的一致性
    std::atomic_int sync_check_point = 0;
    // 正在进行check_point()，写操作将暂时阻塞
    std::atomic_bool is_check_point = false;
    header_t header;
    // 对header.page_size的并发访问是没有问题的，因为它不能在运行时更改
    std::recursive_mutex header_latch;
    std::unique_ptr<node> root; // 根节点常驻内存
    // 保护根节点，因为root本身可能会被修改，所以我们不能直接使用root->lock()
    // 那样是不安全的
    std::shared_mutex root_latch;
    translation_table translation_table;
    page_manager page_manager;
    logger logger;
    Comparator comparator;
    friend class translation_table;
    friend class page_manager;
    friend class logger;
};
}

#endif // _BPLUS_TREE_DB_H
