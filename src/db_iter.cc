#include "db.h"

using namespace bplus_tree_db;

bool DB::iterator::valid()
{
    return off > 0;
}

const std::string& DB::iterator::key()
{
    return db->to_node(off)->keys[i];
}

const std::string& DB::iterator::value()
{
    value_t *v = db->to_node(off)->values[i];
    if (v->reallen <= limit.over_value) return *v->val;
    db->translation_table.load_real_value(v, &saved_value);
    return saved_value;
}

DB::iterator& DB::iterator::seek(const std::string& key)
{
    auto [node, pos] = db->find(db->root.get(), key);
    if (node) {
        off = db->to_off(node);
        i = pos;
    }
    return *this;
}

DB::iterator& DB::iterator::seek_to_first()
{
    if (db->header.key_nums > 0) {
        off = db->header.leaf_off;
        i = 0;
    }
    return *this;
}

DB::iterator& DB::iterator::seek_to_last()
{
    if (db->header.key_nums > 0) {
        off = db->header.last_off;
        i = db->to_node(off)->keys.size() - 1;
    }
    return *this;
}

DB::iterator& DB::iterator::next()
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

DB::iterator& DB::iterator::prev()
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
