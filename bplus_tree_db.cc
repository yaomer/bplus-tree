#include "bplus_tree_db.h"

#define KEY_LIMIT ((1u << 8) - 1)
#define VALUE_LIMIT (1024)

using namespace bplus_tree_db;

void db::init()
{
    if (header.nodes == 0) {
        header.root_off = translation_table.alloc_page();
        header.leaf_off = header.root_off;
        root = new node(true);
    } else {
        root = translation_table.load_node(header.root_off);
    }
}

db::iterator db::find(node *x, const key_t& key)
{
    int i = search(x, key);
    if (i == x->keys.size()) return iterator(this);
    if (x->leaf) {
        if (equal(x->keys[i], key)) return iterator(this, to_off(x), i);
        else return iterator(this);
    }
    return find(to_node(x->childs[i]), key);
}

void db::insert(node *x, const key_t& key, const value_t& value)
{
    int i = search(x, key);
    int n = x->keys.size();
    if (x->leaf) {
        if (i < n && equal(x->keys[i], key)) {
            delete x->values[i];
            x->values[i] = new value_t(value);
        } else {
            x->resize(++n);
            for (int j = n - 2; j >= i; j--) {
                x->copy(j + 1, j);
            }
            node *leaf = header.leaf_off == header.root_off ? root : to_node(header.leaf_off);
            if (less(key, leaf->keys[0])) header.leaf_off = to_off(x);
            x->keys[i] = key;
            x->values[i] = new value_t(value);
            header.nodes++;
        }
        x->update();
    } else {
        if (i == n) {
            x->keys[--i] = key;
            x->update();
        }
        if (isfull(to_node(x->childs[i]), key, value)) {
            split(x, i);
            if (less(x->keys[i], key)) i++;
        }
        insert(to_node(x->childs[i]), key, value);
    }
}

void db::split(node *x, int i)
{
    node *y = to_node(x->childs[i]);
    node *z = split(y);
    int n = x->keys.size();
    x->resize(++n);
    for (int j = n - 1; j > i; j--) {
        x->copy(j, j - 1);
        if (j > i + 1) x->childs[j] = x->childs[j - 1];
    }
    x->keys[i] = y->keys.back();
    if (n == 2) {
        x->keys[n - 1] = z->keys.back();
    }
    x->childs[i + 1] = to_off(z);
    // 如果z是叶节点，那么就需要串到叶节点链表中
    if (z->leaf) {
        z->left = to_off(y);
        z->right = y->right;
        if (y->right > 0) {
            node *r = to_node(y->right);
            r->left = to_off(z);
            r->dirty = true;
        }
        y->right = to_off(z);
    }
    x->update();
}

node *db::split(node *x)
{
    int n = x->keys.size();
    int t = ceil(n / 2.0);
    node *y = new node(x->leaf);
    translation_table.put(translation_table.alloc_page(), y);
    y->resize(n - t);
    for (int i = t; i < n; i++) {
        y->copy(i - t, x, i);
        if (!x->leaf) y->childs[i - t] = x->childs[i];
    }
    x->remove_from(t);
    x->update();
    y->update();
    return y;
}

bool db::isfull(node *x, const key_t& key, const value_t& value)
{
    size_t used = x->used_bytes;
    if (x->leaf) {
        used += (1 + key.size()) + (2 + value.size());
    } else {
        used += (1 + KEY_LIMIT) + sizeof(off_t);
    }
    return used > header.page_size;
}

bool db::check_limit(const key_t& key, const value_t& value)
{
    if (key.size() > KEY_LIMIT) {
        printf("key(%s) over the max-limit(%d)\n", key.c_str(), KEY_LIMIT);
        return false;
    }
    if (value.size() > VALUE_LIMIT) {
        printf("value(%s) over the max-limit(%d)\n", value.c_str(), VALUE_LIMIT);
        return false;
    }
    return true;
}
