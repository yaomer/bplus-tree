#include <unordered_set>

#include "db.h"

using namespace bplus_tree_db;

namespace bplus_tree_db {

struct limits limit;

void panic(const char *fmt, ...)
{
    va_list ap;
    char buf[1024];
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    fprintf(stderr, "error: %s\n", buf);
    va_end(ap);
    // TODO: do sth
    exit(1);
}
}

void DB::init()
{
    comparator = [](const key_t& l, const key_t& r) {
        return std::less<key_t>()(l, r);
    };
    if (header.nodes == 0) {
        header.root_off = translation_table.alloc_page();
        header.leaf_off = header.root_off;
        root = new node(true);
    } else {
        root = translation_table.load_node(header.root_off);
    }
}

void DB::set_key_comparator(Comparator comp)
{
    comparator = comp;
}

void DB::set_page_size(int page_size)
{
    static std::unordered_set<int> valid_pages = {
        1024 * 4,
        1024 * 8,
        1024 * 16,
        1024 * 32,
        1024 * 64,
    };
    if (!valid_pages.count(page_size)) {
        panic("The optional value of `page_size` is (4K, 8K, 16K, 32K or 64K)");
    }
    header.page_size = page_size;
}

void DB::set_page_cache_slots(int slots)
{
    translation_table.set_cache_cap(slots);
}

DB::iterator DB::find(node *x, const key_t& key)
{
    int i = search(x, key);
    if (i == x->keys.size()) return iterator(this);
    if (x->leaf) {
        if (equal(x->keys[i], key)) return iterator(this, to_off(x), i);
        else return iterator(this);
    }
    return find(to_node(x->childs[i]), key);
}

void DB::insert(const key_t& key, const value_t& value)
{
    printf("key = %s\n", key.c_str());
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
    translation_table.flush();
}

void DB::insert(node *x, const key_t& key, const value_t& value)
{
    int i = search(x, key);
    int n = x->keys.size();
    translation_table.put_change_node(x);
    if (x->leaf) {
        if (i < n && equal(x->keys[i], key)) {
            if (x->values[i]->compare(value) == 0) return;
            translation_table.free_value(x->values[i]);
            x->values[i] = new value_t(value);
        } else {
            x->resize(++n);
            for (int j = n - 2; j >= i; j--) {
                x->copy(j + 1, j);
            }
            if (less(key, to_node(header.leaf_off)->keys[0])) header.leaf_off = to_off(x);
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

void DB::split(node *x, int i)
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

node *DB::split(node *x)
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

// 查找x->keys[]中大于等于key的关键字的索引位置
int DB::search(node *x, const key_t& key)
{
    auto comp = comparator;
    auto p = std::lower_bound(x->keys.begin(), x->keys.end(), key, comp);
    if (p == x->keys.end()) return x->keys.size();
    return std::distance(x->keys.begin(), p);
}

bool DB::isfull(node *x, const key_t& key, const value_t& value)
{
    size_t page_used = x->page_used;
    if (x->leaf) {
        page_used += (limit.key_len_field + key.size()) + (limit.value_len_field + std::min(limit.over_value, value.size()));
    } else {
        page_used += (limit.key_len_field + limit.max_key) + sizeof(off_t);
    }
    return page_used > header.page_size;
}

bool DB::check_limit(const key_t& key, const value_t& value)
{
    if (key.size() == 0 || key.size() > limit.max_key) {
        printf("The range for key is (0, %zu]\n", limit.max_key);
        return false;
    }
    if (value.size() > limit.max_value) {
        printf("The range for value is [0, %zu]\n", limit.max_value);
        return false;
    }
    return true;
}
