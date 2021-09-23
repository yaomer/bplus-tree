#ifndef _BTREE_H
#define _BTREE_H

#include <string>
#include <vector>

//
// 参照算法导论的伪代码描述
//
template <typename Key,
          typename T,
          typename Comparator = std::less<Key>>
class btree {
public:
    typedef Key key_type;
    typedef T value_type;

    struct node {
        node(int t, bool isleaf) : t(t), isleaf(isleaf)
        {
            keys.resize(max_keys(t));
            values.resize(max_keys(t));
            if (!isleaf) childs.resize(max_childs(t));
        }
        value_type *find(const key_type& key)
        {
            int i = search(key);
            if (isequal(i, key)) return values[i];
            if (isleaf) return nullptr;
            return childs[i]->find(key);
        }
        // 沿树单程递归向下插入，不需要回溯
        void insert(const key_type& key, const value_type& value)
        {
            int i = search(key);
            if (isequal(i, key)) {
                // 已存在我们就更新value
                delete values[i];
                values[i] = new value_type(value);
                return;
            }
            if (isleaf) {
                // 腾出一个位置来存放新的(key, value)
                for (int j = n - 1; j >= i; j--) {
                    copy(j + 1, j);
                }
                keys[i] = key;
                values[i] = new value_type(value);
                n++;
            } else {
                if (childs[i]->isfull()) {
                    split(i); // split()保证不会降临到一个满节点上
                    if (equal(keys[i], key)) return;
                    // keys[i]为分裂后两个子节点的父节点
                    if (less(keys[i], key)) i++;
                }
                childs[i]->insert(key, value);
            }
        }
        // 分裂满节点childs[i]
        void split(int i)
        {
            node *y = childs[i];
            node *z = y->split();
            // 空出childs[i + 1]指向新分裂出的节点z
            for (int j = n; j > i; j--) {
                childs[j + 1] = childs[j];
            }
            childs[i + 1] = z;
            // 空出keys[i]，存放被提升的新关键字y->keys[t - 1]
            for (int j = n - 1; j >= i; j--) {
                copy(j + 1, j);
            }
            copy(i, y, t - 1);
            n++;
        }
        node *split()
        {
            node *x = new node(t, isleaf);
            // 将自己的后t - 1个关键字复制到z中
            for (int j = 0; j < t - 1; j++) {
                x->copy(j, this, j + t);
                if (!isleaf) x->childs[j] = childs[j + t];
            }
            if (!isleaf) x->childs[t - 1] = childs[2 * t - 1];
            x->n = t - 1;
            n = t - 1;
            return x;
        }
        void erase(const key_type& key)
        {
            int i = search(key);
            if (isleaf) {
                if (isequal(i, key)) {
                    remove_key(i, true);
                }
            } else {
                if (isequal(i, key)) {
                    erase_in_inner(i, key);
                } else {
                    erase_not_in_inner(i, key);
                }
            }
        }
        void erase_in_inner(int i, const key_type& key)
        {
            node *y = childs[i]; // 小于key的子树
            node *z = childs[i + 1]; // 大于key的子树
            if (y->n >= t) {
                // 用子树y中的前驱节点的关键字key'来代替key，然后递归删除key'
                node *precursor = y->get_precursor();
                value_type *remval = values[i];
                copy(i, precursor, precursor->n - 1);
                precursor->values[precursor->n - 1] = remval;
                y->erase(keys[i]);
            } else if (z->n >= t) {
                // 用子树z中的后继节点的关键字key'来代替key，然后递归删除key'
                node *successor = z->get_successor();
                value_type *remval = values[i];
                copy(i, successor, 0);
                successor->values[0] = remval;
                z->erase(keys[i]);
            } else {
                // 此时y和z都只有t - 1个关键字，我们将key和z合并到y中
                // 然后从y中递归删除key
                y->copy(y->n++, this, i);
                remove_key(i, false, i + 1);
                y->merge(z);
                y->erase(key);
            }
        }
        void erase_not_in_inner(int i, const key_type& key)
        {
            node *x = childs[i];
            node *y = i >= 1 ? childs[i - 1] : nullptr; // x的左兄弟
            node *z = i + 1 <= n ? childs[i + 1] : nullptr; // x的右兄弟
            // 如果x只有t - 1个关键字，那么我们必须通过以下步骤来保证
            // 降至一个至少包含t个关键字的节点
            if (x->n == t - 1) {
                // 如果x有一个至少包含t个关键字的相邻的兄弟
                // 那么我们就从它那里借一个关键字，以让x->n == t
                // 从而继续递归向下删除
                if (y && y->n >= t) {
                    borrow_from_left(x, y, i - 1);
                    x->erase(key);
                } else if (z && z->n >= t) {
                    borrow_from_right(x, z, i);
                    x->erase(key);
                } else {
                    // 与一个相邻兄弟合并，将this中的一个关键字移到新节点中，
                    // 使之成为该节点的中间关键字，
                    if (y) {
                        y->copy(y->n++, this, i - 1);
                        remove_key(i - 1, false, i);
                        y->merge(x);
                        y->erase(key);
                    } else if (z) {
                        x->copy(x->n++, this, i);
                        remove_key(i, false, i + 1);
                        x->merge(z);
                        x->erase(key);
                    }
                }
            } else {
                x->erase(key);
            }
        }
        void borrow_from_right(node *x, node *z, int i)
        {
            x->copy(x->n++, this, i);
            if (!x->isleaf) x->childs[x->n] = z->childs[0];
            copy(i, z, 0);
            z->remove_key(0, false, 0);
        }
        void borrow_from_left(node *x, node *y, int i)
        {
            if (!x->isleaf)
                x->childs[x->n + 1] = x->childs[x->n];
            for (int j = x->n - 1; j >= 0; j--) {
                x->copy(j + 1, j);
                if (!x->isleaf) x->childs[j + 1] = x->childs[j];
            }
            x->copy(0, this, i);
            x->n++;
            if (!x->isleaf) x->childs[0] = y->childs[y->n];
            copy(i, y, y->n - 1);
            y->remove_key(y->n - 1, false);
        }
        void merge(node *x)
        {
            for (int j = 0; j < x->n; j++) {
                copy(j + n, x, j);
                if (!isleaf) childs[j + n] = x->childs[j];
            }
            n += x->n;
            if (!isleaf) childs[n] = x->childs[x->n];
            delete x;
        }

        // 查找this->keys[]中大于等于key的关键字的索引位置
        int search(const key_type& key)
        {
            auto end = keys.begin() + n;
            auto p = std::lower_bound(keys.begin(), end, key, Comparator());
            if (p == end) return n;
            return std::distance(keys.begin(), p);
        }
        // 该节点是否是满的
        bool isfull() { return n == keys.size(); }

        bool isequal(int i, const key_type& key)
        {
            return i < n && equal(keys[i], key);
        }
        void copy(int i, node *x, int j)
        {
            keys[i] = x->keys[j];
            values[i] = x->values[j];
        }
        void copy(int i, int j)
        {
            copy(i, this, j);
        }
        void remove_key(int i, bool deleted, int child = -1)
        {
            if (deleted) delete values[i];
            for (int j = i + 1; j < n; j++) {
                copy(j - 1, j);
            }
            if (!isleaf && child >= 0) {
                for (int j = child + 1; j <= n; j++) {
                    childs[j - 1] = childs[j];
                }
            }
            n--;
        }

        node *get_precursor()
        {
            if (isleaf) return this;
            return childs[n]->get_precursor();
        }
        node *get_successor()
        {
            if (isleaf) return this;
            return childs[0]->get_successor();
        }

        bool less(const key_type& l, const key_type& r)
        {
            return comparator(l, r);
        }
        bool equal(const key_type& l, const key_type& r)
        {
            return !comparator(l, r) && !comparator(r, l);
        }

        bool isleaf;
        const int t;
        int n = 0; // 节点中存放的关键字数目
        std::vector<key_type> keys;
        std::vector<value_type*> values;
        std::vector<node*> childs;
        Comparator comparator;
    };

    btree() : root(new node(t, true))
    {
    }
    btree(int t) : t(t), root(new node(t, true))
    {
    }
    value_type *find(const key_type& key)
    {
        return root->find(key);
    }
    void insert(const key_type& key, const value_type& value)
    {
        node *r = root;
        if (r->isfull()) {
            node *s = new node(t, false);
            root = s;
            s->childs[0] = r;
            s->split(0);
            s->insert(key, value);
        } else {
            r->insert(key, value);
        }
    }
    void erase(const key_type& key)
    {
        root->erase(key);
        if (!root->isleaf && root->n == 0) {
            node *r = root->childs[0];
            delete root;
            root = r;
        }
    }
private:
    btree(const btree&) = delete;
    btree& operator=(const btree&) = delete;

    static int max_keys(int t) { return 2 * t - 1; }
    static int max_childs(int t) { return 2 * t; }

    // t为B树的最小度数
    // (除根节点以外)每个节点至少包含t-1个关键字，至少有t个孩子
    // 至多包含2t-1个关键字，至多有2t个孩子
    // 如果一个节点恰好有2t-1个关键字，就称它是满的
    const int t = 3; // t >= 2
    node *root = nullptr;
};

#endif // _BTREE_H
