#ifndef _BPLUS_TREE_DISK_H
#define _BPLUS_TREE_DISK_H

#include <unordered_map>

#include <sys/uio.h>

namespace bplus_tree_db {

struct header_t;
struct node;

// 转换表中并不保存根节点
class translation_table {
public:
    translation_table(const std::string& filename, header_t *header);
    off_t alloc_page();
    void free_page(off_t off);
    node *load_node(off_t off);
    void put(off_t off, node *node)
    {
        translation_to_node.emplace(off, node);
        translation_to_off.emplace(node, off);
    }
    node *to_node(off_t off);
    off_t to_off(node *node);
    // 因为根节点会不断调整，所以需要每次传进来
    void flush(node *root);
private:
    void fill_header(struct iovec *iov);
    void save_header();
    void load_header();
    void save_node(off_t off, node *node);
    void panic(const char *fmt, ...);

    int fd;
    std::string filename;
    // 双向转换表
    std::unordered_map<off_t, node*> translation_to_node;
    std::unordered_map<node*, off_t> translation_to_off;
    header_t *header;
};
}

#endif // _BPLUS_TREE_DISK_H
