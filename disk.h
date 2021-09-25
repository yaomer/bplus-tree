#ifndef _BPLUS_TREE_DISK_H
#define _BPLUS_TREE_DISK_H

#include <unordered_map>
#include <vector>
#include <list>

#include <sys/uio.h>

namespace bplus_tree_db {

class DB;
struct node;

// 转换表中并不保存根节点
class translation_table {
public:
    translation_table(const std::string& filename, DB *db);
    ~translation_table() { lru_flush(); }
    void set_cache_cap(int cap) { lru_cap = std::max(lru_min_cap, cap); }
    off_t alloc_page();
    void free_page(off_t off);
    node *load_node(off_t off);
    node *to_node(off_t off);
    off_t to_off(node *node);
    // 向转换表中加入一个新的表项
    void put(off_t off, node *node) { lru_put(off, node); }
    // 放入每轮修改中涉及到的节点，flush()之后，change_list会被清空
    void put_change_node(node *node) { change_list.emplace_back(node); }
    // 将change_list中的dirty node刷到磁盘
    void flush();
private:
    struct cache_node {
        std::unique_ptr<node> x;
        std::list<off_t>::iterator pos;
        cache_node() : x(nullptr), pos() {  }
        cache_node(node *x, std::list<off_t>::iterator pos) : x(std::unique_ptr<node>(x)), pos(pos) { }
    };

    static const int lru_min_cap = 128;

    node *lru_get(off_t off);
    void lru_put(off_t off, node *node);
    void lru_flush();

    void fill_header(struct iovec *iov);
    void save_header();
    void load_header();
    void save_node(off_t off, node *node);
    void panic(const char *fmt, ...);

    int fd;
    std::string filename;
    DB *db; // we need `header` and `root`
    // 双向转换表
    std::unordered_map<off_t, cache_node> translation_to_node;
    std::unordered_map<node*, off_t> translation_to_off;
    std::list<off_t> cache_list;
    // 为了避免每次遍历cache_list
    std::vector<node*> change_list;
    int lru_cap;
};
}

#endif // _BPLUS_TREE_DISK_H
