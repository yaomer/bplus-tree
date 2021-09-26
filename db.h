#ifndef _BPLUS_TREE_DB_H
#define _BPLUS_TREE_DB_H

#include <string>
#include <vector>

#include <limits.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "disk.h"
#include "page.h"

namespace bplus_tree_db {

struct header_t {
    int8_t magic = 0x1a;
    size_t page_size = 1024 * 16;
    size_t key_nums = 0;
    off_t root_off = 0;
    off_t leaf_off = 0;
    off_t free_list_head = page_size;
    size_t free_pages = 0;
    off_t over_page_list_head = 0;
    size_t over_pages = 0;
};

struct limits {
    // key允许的最大长度，较小的key有助于最大化索引节点的分支因子
    const size_t max_key = UINT8_MAX;
    const size_t max_value = UINT32_MAX;
    const size_t type_field = 1;
    const size_t key_nums_field = 2;
    const size_t key_len_field = 1;
    const size_t value_len_field = 4;
    const size_t off_field = sizeof(off_t);
    // 如果一个value的长度超过了over_value，那么超出的部分将被存放到溢出页
    // 由header.page_size决定
    size_t over_value;
};

void panic(const char *fmt, ...);

extern struct limits limit;

struct node {
    node(bool leaf) : leaf(leaf)
    {
        page_used = limit.type_field + limit.key_nums_field;
        if (leaf) page_used += limit.off_field * 2; // left and right
    }
    ~node()
    {
        if (!leaf) return;
        for (auto it = values.begin(); it != values.end(); ) {
            auto e = it++;
            delete *e;
        }
    }

    void resize(int n)
    {
        keys.resize(n);
        if (leaf) values.resize(n);
        else childs.resize(n);
    }
    void remove_from(int from)
    {
        keys.erase(keys.begin() + from, keys.end());
        if (leaf) values.erase(values.begin() + from, values.end());
        else childs.erase(childs.begin() + from, childs.end());
        update();
    }
    void remove(int i)
    {
        int n = keys.size();
        for (int j = i + 1; j < n; j++) {
            copy(j - 1, j);
            if (!leaf) childs[j - 1] = childs[j];
        }
        resize(n - 1);
        update();
    }
    void copy(int i, node *x, int j)
    {
        keys[i] = x->keys[j];
        if (leaf) values[i] = x->values[j];
    }
    void copy(int i, int j)
    {
        copy(i, this, j);
    }
    void update(bool dirty = true);

    bool leaf;
    bool dirty = false;
    std::vector<key_t> keys;
    std::vector<off_t> childs;
    std::vector<value_t*> values;
    size_t page_used;
    off_t left = 0, right = 0;
};

class DB {
public:
    DB() : filename("dump.bpt"), translation_table(this), page_manager(this)
    {
        init();
    }
    DB(const std::string& filename)
        : filename(filename), translation_table(this), page_manager(this)
    {
        init();
    }
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    typedef std::function<bool(const key_t&, const key_t&)> Comparator;

    class iterator {
    public:
        iterator(DB *db) : db(db), off(0), i(0) {  }
        iterator(DB *db, off_t off, int i) : db(db), off(off), i(i) {  }
        bool valid() { return off > 0; }
        key_t *key() { return &db->to_node(off)->keys[i]; }
        value_t *value() { return db->to_node(off)->values[i]; }
        iterator& next()
        {
            if (off > 0) {
                node *x = db->to_node(off);
                if (i + 1 < x->keys.size()) i++;
                else {
                    off = x->right;
                    i = 0;
                }
            }
            return *this;
        }
        iterator& prev()
        {
            if (off > 0) {
                node *x = db->to_node(off);
                if (i - 1 >= 0) i--;
                else {
                    off = x->left;
                    if (off > 0) i = db->to_node(off)->keys.size() - 1;
                }
            }
            return *this;
        }
    private:
        DB *db;
        off_t off;
        int i;
    };

    void set_key_comparator(Comparator comp);
    void set_page_size(int page_size);
    void set_page_cache_slots(int slots);

    iterator first() { return iterator(this, header.leaf_off, 0); }
    iterator find(const key_t& key) { return find(root.get(), key); }
    void insert(const key_t& key, const value_t& value);
    void erase(const key_t& key);
private:
    void init();

    node *to_node(off_t off) { return translation_table.to_node(off); }
    off_t to_off(node *node) { return translation_table.to_off(node); }

    iterator find(node *x, const key_t& key);
    void insert(node *x, const key_t& key, const value_t& value);
    void erase(node *x, const key_t& key, node *precursor);

    bool isfull(node *x, const key_t& key, const value_t& value);
    void split(node *x, int i);
    node *split(node *x);

    node *get_precursor(node *x);
    void borrow_from_right(node *r, node *x, node *z, int i);
    void borrow_from_left(node *r, node *x, node *y, int i);
    void merge(node *y, node *x);

    int search(node *x, const key_t& key);

    bool check_limit(const key_t& key, const value_t& value);

    bool less(const key_t& l, const key_t& r)
    {
        return comparator(l, r);
    }
    bool equal(const key_t& l, const key_t& r)
    {
        return !comparator(l, r) && !comparator(r, l);
    }

    int fd;
    std::string filename;
    header_t header;
    std::unique_ptr<node> root; // 根节点常驻内存
    translation_table translation_table;
    page_manager page_manager;
    Comparator comparator;
    friend class translation_table;
    friend class page_manager;
};
}

#endif // _BPLUS_TREE_DB_H
