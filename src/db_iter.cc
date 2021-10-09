#include "db.h"

using namespace bpdb;

bool DB::iterator::valid()
{
    return page_id > 0;
}

const std::string& DB::iterator::key()
{
    return db->to_node(page_id)->keys[i];
}

const std::string& DB::iterator::value()
{
    value_t *v = db->to_node(page_id)->values[i];
    if (v->reallen <= limit.over_value) return *v->val;
    db->translation_table.load_real_value(v, &saved_value);
    return saved_value;
}

DB::iterator& DB::iterator::seek(const std::string& key)
{
    auto [node, pos] = db->find(db->root.get(), key);
    if (node) {
        page_id = db->to_page_id(node);
        i = pos;
    }
    return *this;
}

DB::iterator& DB::iterator::seek_to_first()
{
    if (db->header.key_nums > 0) {
        page_id = db->header.leaf_id;
        i = 0;
    }
    return *this;
}

DB::iterator& DB::iterator::seek_to_last()
{
    key_t key;
    db->root_shmtx.lock_shared();
    key = db->root->keys.back();
    db->root_shmtx.unlock_shared();
    return seek(key);
}

DB::iterator& DB::iterator::next()
{
    if (page_id > 0) {
        node *x = db->to_node(page_id);
        if (i + 1 < x->keys.size()) i++;
        else {
            page_id = x->right;
            i = 0;
        }
    }
    return *this;
}

DB::iterator& DB::iterator::prev()
{
    if (page_id > 0) {
        node *x = db->to_node(page_id);
        if (i - 1 >= 0) i--;
        else {
            page_id = x->left;
            if (page_id > 0) i = db->to_node(page_id)->keys.size() - 1;
        }
    }
    return *this;
}
