#ifndef _BPLUS_TREE_DISK_H
#define _BPLUS_TREE_DISK_H

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <list>
#include <string>

#include <sys/uio.h>

#include "page.h"

namespace bplus_tree_db {

class DB;
struct node;

typedef std::string key_t;

struct value_t {
    ~value_t() { delete val; }
    off_t over_page_off = 0;
    uint16_t remain_off = 0;
    uint32_t reallen;
    std::string *val;
};

// 转换表中并不保存根节点
class translation_table {
public:
    translation_table(DB *db) : db(db), lru_cap(1024) {  }
    ~translation_table() { lru_flush(); }
    translation_table(const translation_table&) = delete;
    translation_table& operator=(const translation_table&) = delete;

    void init();
    void set_cache_cap(int cap) { lru_cap = std::max(128, cap); }
    node *load_node(off_t off);
    void load_real_value(value_t *value);
    void free_node(node *node);
    void free_value(value_t *value);
    void release_root(node *root);
    node *to_node(off_t off);
    off_t to_off(node *node);
    // 向转换表中加入一个新的表项
    void put(off_t off, node *node) { lru_put(off, node); }
    // 放入每轮修改中涉及到的节点，flush()之后，change_list会被清空
    void put_change_node(node *node) { change_list.emplace(node); }
    // 将change_list中的dirty node刷到磁盘
    void flush();
private:
    struct cache_node {
        std::unique_ptr<node> x;
        std::list<off_t>::iterator pos;
        cache_node() : x(nullptr), pos() {  }
        cache_node(node *x, std::list<off_t>::iterator pos) : x(std::unique_ptr<node>(x)), pos(pos) { }
    };

    node *lru_get(off_t off);
    void lru_put(off_t off, node *node);
    void lru_flush();

    void fill_header(struct iovec *iov);
    void save_header();
    void load_header();
    void save_node(off_t off, node *node);
    void save_value(std::string& buf, value_t *value);
    value_t *load_value(char **ptr);

    void clear();

    DB *db;
    // 双向转换表
    std::unordered_map<off_t, cache_node> translation_to_node;
    std::unordered_map<node*, off_t> translation_to_off;
    std::list<off_t> cache_list;
    // 为了避免每次遍历cache_list
    std::unordered_set<node*> change_list;
    int lru_cap;
};
}

#endif // _BPLUS_TREE_DISK_H
