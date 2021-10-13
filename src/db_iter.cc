#include "db.h"

using namespace bpdb;

bool DB::iterator::valid()
{
    return page_id > 0;
}

const std::string& DB::iterator::key()
{
    node *x = db->to_node(page_id);
    if (x) {
        rlock_t lk(x->latch);
        if (!x->deleted) {
            if (i == -1) i = x->keys.size() - 1;
            saved_key = x->keys[i];
            return saved_key;
        }
    }
    page_id = 0;
    saved_key.clear();
    return saved_key;
}

const std::string& DB::iterator::value()
{
    node *x = db->to_node(page_id);
    if (x) {
        rlock_t lk(x->latch);
        if (!x->deleted) {
            if (i == -1) i = x->keys.size() - 1;
            db->translation_table.load_real_value(x->values[i], &saved_value);
            return saved_value;
        }
    }
    page_id = 0;
    saved_value.clear();
    return saved_value;
}

DB::iterator& DB::iterator::seek(const std::string& key)
{
    {
        rlock_t rlk(db->root_latch);
        db->root->lock_shared();
    }
    auto [node, pos] = db->find(db->root.get(), key);
    if (node) {
        page_id = db->to_page_id(node);
        i = pos;
        node->unlock_shared();
    }
    return *this;
}

DB::iterator& DB::iterator::seek_to_first()
{
    if (db->header.key_nums > 0) {
        recursive_lock_t lk(db->header_latch);
        page_id = db->header.leaf_id;
        i = 0;
    }
    return *this;
}

DB::iterator& DB::iterator::seek_to_last()
{
    {
        rlock_t rlk(db->root_latch);
        db->root->lock_shared();
    }
    saved_key = db->root->keys.back();
    db->root->unlock_shared();
    return seek(saved_key);
}

DB::iterator& DB::iterator::next()
{
    if (page_id == 0) return *this;
    node *x = db->to_node(page_id);
    if (x) {
        rlock_t lk(x->latch);
        if (!x->deleted) {
            if (i == -1) i = x->keys.size() - 1;
            if (i + 1 < x->keys.size()) i++;
            else {
                page_id = x->right;
                i = 0;
            }
            return *this;
        }
    }
    page_id = 0;
    return *this;
}

DB::iterator& DB::iterator::prev()
{
    if (page_id == 0) return *this;
    node *x = db->to_node(page_id);
    if (x) {
        rlock_t lk(x->latch);
        if (!x->deleted) {
            if (i == -1) i = x->keys.size() - 1;
            if (i - 1 >= 0) i--;
            else {
                page_id = x->left;
                i = -1;
            }
            return *this;
        }
    }
    page_id = 0;
    return *this;
}
