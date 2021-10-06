#ifndef _BPLUS_TREE_COMMON_H
#define _BPLUS_TREE_COMMON_H

#include <string>
#include <vector>

#include <limits.h>

namespace bplus_tree_db {

typedef std::string key_t;

struct value_t {
    ~value_t() { delete val; }
    off_t over_page_off = 0;
    uint16_t remain_off = 0;
    uint32_t reallen;
    std::string *val;
};

struct header_t {
    int8_t magic = 0x1a;
    size_t page_size = 1024 * 16;
    size_t key_nums = 0;
    off_t root_off = 0;
    off_t leaf_off = 0;
    off_t last_off = 0;
    off_t free_list_head = page_size;
    size_t free_pages = 0;
    off_t over_page_list_head = 0;
    size_t over_pages = 0;
};

struct limit_t {
    // key允许的最大长度，较小的key有助于最大化索引节点的分支因子
    const size_t max_key = UINT8_MAX;
    const size_t max_value = UINT32_MAX;
    const size_t type_field = 1;
    const size_t key_nums_field = 2;
    const size_t key_len_field = 1;
    const size_t value_len_field = 4;
    // 如果一个value的长度超过了over_value，那么超出的部分将被存放到溢出页
    // 由header.page_size决定
    size_t over_value;
};

extern struct limit_t limit;

struct node {
    node(bool leaf) : leaf(leaf)
    {
        page_used = limit.type_field + limit.key_nums_field;
        if (leaf) page_used += sizeof(off_t) * 2; // left and right
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
    off_t lsn = 0;
};

}

#endif // _BPLUS_TREE_COMMON_H
