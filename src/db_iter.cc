#include "db.h"

namespace bpdb {

bool DB::iterator::valid()
{
    return page_id > 0;
}

const std::string& DB::iterator::key()
{
    node *x = db->to_node(page_id);
    if (i == -1) i = x->keys.size() - 1;
    return x->keys[i];
}

const std::string& DB::iterator::value()
{
    node *x = db->to_node(page_id);
    if (i == -1) i = x->keys.size() - 1;
    value_t *v = x->values[i];
    if (v->reallen <= limit.over_value) return *v->val;
    db->translation_table.load_real_value(v, &saved_value);
    return saved_value;
}

DB::iterator& DB::iterator::seek(const std::string& key)
{
    db->root->lock_shared();
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
        page_id = db->header.leaf_id;
        i = 0;
    }
    return *this;
}

DB::iterator& DB::iterator::seek_to_last()
{
    return seek(db->root->keys.back());
}

DB::iterator& DB::iterator::next()
{
    node *x = db->to_node(page_id);
    if (i == -1) i = x->keys.size() - 1;
    if (i + 1 < x->keys.size()) i++;
    else {
        page_id = x->right;
        i = 0;
    }
    return *this;
}

DB::iterator& DB::iterator::prev()
{
    node *x = db->to_node(page_id);
    if (i == -1) i = x->keys.size() - 1;
    if (i - 1 >= 0) i--;
    else {
        page_id = x->left;
        i = -1;
    }
    return *this;
}

} // namespace bpdb
