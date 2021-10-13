#ifndef _BPLUS_TREE_DISK_H
#define _BPLUS_TREE_DISK_H

#include <unordered_map>
#include <vector>
#include <list>
#include <string>

#include <sys/uio.h>

#include "page.h"
#include "common.h"

namespace bpdb {

class DB;

// 转换表中并不保存根节点
class translation_table {
public:
    translation_table(DB *db) : db(db), lru_cap(1024) {  }
    translation_table(const translation_table&) = delete;
    translation_table& operator=(const translation_table&) = delete;

    void init();
    void set_cache_cap(int cap) { lru_cap = std::max(128, cap); }
    node *load_node(page_id_t page_id);
    void load_real_value(value_t *value, std::string *saved_val);
    void free_value(value_t *value);
    void release_root(node *root);
    node *to_node(page_id_t page_id);
    page_id_t to_page_id(node *node);
    // 向转换表中加入一个新的表项
    void put(page_id_t page_id, node *node) { lru_put(page_id, node); }
    void flush();
private:
    struct cache_node {
        std::unique_ptr<node> x;
        std::list<page_id_t>::iterator pos;
        cache_node() : x(nullptr), pos() {  }
        cache_node(node *x, std::list<page_id_t>::iterator pos) : x(std::unique_ptr<node>(x)), pos(pos) { }
    };

    node *lru_get(page_id_t page_id);
    void lru_put(page_id_t page_id, node *node);

    void fill_header(header_t *header, struct iovec *iov);
    void load_header();
    void save_header(header_t *header);
    void save_node(page_id_t page_id, node *node);
    void save_value(std::string& buf, value_t *value);
    value_t *load_value(char **ptr);
    void free_node(page_id_t page_id, node *node);

    void clear();

    DB *db;
    // 双向转换表 && LRU cache
    std::unordered_map<page_id_t, cache_node> translation_to_node;
    std::unordered_map<node*, page_id_t> translation_to_page;
    std::list<page_id_t> cache_list;
    std::shared_mutex table_latch;
    int lru_cap;
};
}

#endif // _BPLUS_TREE_DISK_H
