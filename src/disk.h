#ifndef _BPLUS_TREE_DISK_H
#define _BPLUS_TREE_DISK_H

#include <unordered_map>
#include <vector>
#include <list>
#include <string>

#include <sys/uio.h>

#include "page.h"
#include "common.h"

namespace bplus_tree_db {

class DB;

// 转换表中并不保存根节点
class translation_table {
public:
    translation_table(DB *db) : db(db), lru_cap(1024) {  }
    translation_table(const translation_table&) = delete;
    translation_table& operator=(const translation_table&) = delete;

    void init();
    void set_cache_cap(int cap) { lru_cap = std::max(128, cap); }
    void save_header(header_t *header);
    void save_node(off_t off, node *node);
    node *load_node(off_t off);
    void load_real_value(value_t *value, std::string *saved_val);
    void free_node(node *node);
    void free_value(value_t *value);
    void release_root(node *root);
    node *to_node(off_t off);
    off_t to_off(node *node);
    // 向转换表中加入一个新的表项
    void put(off_t off, node *node) { lru_put(off, node); }
private:
    struct cache_node {
        std::unique_ptr<node> x;
        std::list<off_t>::iterator pos;
        cache_node() : x(nullptr), pos() {  }
        cache_node(node *x, std::list<off_t>::iterator pos) : x(std::unique_ptr<node>(x)), pos(pos) { }
    };

    node *lru_get(off_t off);
    void lru_put(off_t off, node *node);

    void fill_header(header_t *header, struct iovec *iov);
    void load_header();
    void save_value(std::string& buf, value_t *value);
    value_t *load_value(char **ptr);

    void clear();

    DB *db;
    // 双向转换表 && LRU cache
    std::unordered_map<off_t, cache_node> translation_to_node;
    std::unordered_map<node*, off_t> translation_to_off;
    std::list<off_t> cache_list;
    int lru_cap;
};
}

#endif // _BPLUS_TREE_DISK_H
