#ifndef _BPLUS_TREE_DB_H
#define _BPLUS_TREE_DB_H

#include <string>
#include <vector>

#include "disk.h"

namespace bplus_tree_db {

struct header_t {
    int8_t magic = 0x1a;
    size_t page_size = 1024 * 4;
    size_t nodes = 0;
    off_t root_off = 0;
    off_t leaf_off = 0;
    off_t free_list_head = page_size;
    size_t free_pages = 0;
};

typedef std::string key_t;
typedef std::string value_t;

struct node {
    node(bool leaf) : leaf(leaf) {  }

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
    void update(bool dirty = true)
    {
        used_bytes = 1 + 2;
        for (auto& key : keys) used_bytes += 1 + key.size();
        if (leaf) {
            for (auto& value : values) used_bytes += 2 + value->size();
            used_bytes += sizeof(off_t) * 2;
        } else {
            used_bytes += sizeof(off_t) * childs.size();
        }
        this->dirty = dirty;
    }
    bool leaf;
    bool dirty = false;
    std::vector<key_t> keys;
    std::vector<off_t> childs;
    std::vector<value_t*> values;
    size_t used_bytes = 3;
    off_t left = 0, right = 0;
};

class db {
public:
    db() : translation_table("dump.bpt", &header)
    {
        init();
    }

    db(const std::string& filename)
        : translation_table(filename, &header)
    {
        init();
    }

    struct Comparator {
        bool operator()(const key_t& l, const key_t& r) const
        {
            return std::less<key_t>()(l, r);
        }
    };

    class iterator {
    public:
        iterator(db *b) : b(b), off(0), i(0) {  }
        iterator(db *b, off_t off, int i) : b(b), off(off), i(i) {  }
        bool valid() { return off > 0; }
        key_t *key() { return &b->to_node(off)->keys[i]; }
        value_t *value() { return b->to_node(off)->values[i]; }
        iterator& next()
        {
            if (off > 0) {
                node *x = b->to_node(off);
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
                node *x = b->to_node(off);
                if (i - 1 >= 0) i--;
                else {
                    off = x->left;
                    if (off > 0) i = b->to_node(off)->keys.size() - 1;
                }
            }
            return *this;
        }
    private:
        db *b;
        off_t off;
        int i;
    };

    iterator first() { return iterator(this, header.leaf_off, 0); }
    iterator find(const key_t& key)
    {
        return find(root, key);
    }
    void insert(const key_t& key, const value_t& value)
    {
        node *r = root;
        if (!check_limit(key, value)) return;
        if (isfull(r, key, value)) {
            root = new node(false);
            root->resize(1);
            root->childs[0] = header.root_off;
            translation_table.put(header.root_off, r);
            header.root_off = translation_table.alloc_page();
            split(root, 0);
        }
        insert(root, key, value);
        translation_table.flush(root);
    }
private:
    void init();

    node *to_node(off_t off) { return translation_table.to_node(off); }
    off_t to_off(node *node) { return translation_table.to_off(node); }

    iterator find(node *x, const key_t& key);
    void insert(node *x, const key_t& key, const value_t& value);

    bool check_limit(const key_t& key, const value_t& value);

    void split(node *x, int i);
    node *split(node *x);

    // 查找x->keys[]中大于等于key的关键字的索引位置
    int search(node *x, const key_t& key)
    {
        auto comp = comparator;
        auto p = std::lower_bound(x->keys.begin(), x->keys.end(), key, comp);
        if (p == x->keys.end()) return x->keys.size();
        return std::distance(x->keys.begin(), p);
    }

    bool isfull(node *x, const key_t& key, const value_t& value);

    bool less(const key_t& l, const key_t& r)
    {
        return comparator(l, r);
    }
    bool equal(const key_t& l, const key_t& r)
    {
        return !comparator(l, r) && !comparator(r, l);
    }

    header_t header;
    node *root; // 根节点常驻内存
    translation_table translation_table;
    Comparator comparator;
};
}

#endif // _BPLUS_TREE_DB_H
