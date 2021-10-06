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

class DB {
public:
    DB() : dbname("testdb"), translation_table(this), page_manager(this), redo_log(this)
    {
        init();
    }
    DB(const std::string& dbname)
        : dbname(dbname), translation_table(this), page_manager(this), redo_log(this)
    {
        init();
    }
    ~DB() { redo_log.check_point(SHARP_CHECK_POINT); }
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    typedef std::function<bool(const key_t&, const key_t&)> Comparator;

    class iterator {
    public:
        iterator(DB *db) : db(db), off(0), i(0) {  }
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
        off_t off;
        int i;
        std::string saved_value;
    };

    void set_key_comparator(Comparator comp);
    void set_page_size(int page_size);
    void set_page_cache_slots(int slots);

    iterator new_iterator() { return iterator(this); }
    bool find(const std::string& key, std::string *value);
    void insert(const std::string& key, const std::string& value);
    void erase(const std::string& key);
    void rebuild();
private:
    void init();
    void flush_if_needed();

    node *to_node(off_t off) { return translation_table.to_node(off); }
    off_t to_off(node *node) { return translation_table.to_off(node); }

    int search(node *x, const key_t& key);
    bool check_limit(const std::string& key, const std::string& value);
    value_t *build_new_value(const std::string& value);

    std::pair<node*, int> find(node *x, const key_t& key);
    void insert(node *x, const key_t& key, value_t *value);
    void erase(node *x, const key_t& key, node *precursor);

    bool isfull(node *x, const key_t& key, value_t *value);
    void split(node *x, int i, const key_t& key);
    node *split(node *x, int type);
    enum { RIGHT_INSERT_SPLIT, LEFT_INSERT_SPLIT, MID_SPLIT };
    int get_split_type(node *x, const key_t& key);
    void link_leaf(node *z, node *y, int type);

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
    std::string dbname;
    std::string dbfile;
    header_t header;
    std::unique_ptr<node> root; // 根节点常驻内存
    translation_table translation_table;
    page_manager page_manager;
    redo_log redo_log;
    Comparator comparator;
    friend class translation_table;
    friend class page_manager;
    friend class redo_log;
};
}

#endif // _BPLUS_TREE_DB_H
