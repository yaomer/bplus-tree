#ifndef _BPLUS_TREE_H
#define _BPLUS_TREE_H

#include <string>
#include <vector>

#include <math.h>

template <typename Key,
          typename T,
          typename Comparator = std::less<Key>>
class bplus_tree {
public:
    typedef Key key_type;
    typedef T value_type;
    struct node;
    typedef std::pair<node*, int> pos_type;

    struct node {
        node(int M, bool isleaf) : isleaf(isleaf)
        {
            keys.resize(M);
            if (isleaf) values.resize(M);
            else childs.resize(M);
        }
        pos_type find(const key_type& key)
        {
            int i = search(key);
            if (i == n) return { nullptr, 0 };
            if (isleaf) return { this, i };
            return childs[i]->find(key);
        }
        pos_type insert(const key_type& key, const value_type& value)
        {
            int i = search(key);
            if (isleaf) {
                if (isequal(i, key)) {
                    delete values[i];
                    values[i] = new value_type(value);
                } else {
                    for (int j = n - 1; j >= i; j--) {
                        copy(j + 1, j);
                    }
                    keys[i] = key;
                    values[i] = new value_type(value);
                    n++;
                }
                return { this, i };
            } else {
                if (i == n) {
                    // 如果key比索引节点中最大的关键字还要大，那么就需要更新索引节点的右边界
                    keys[--i] = key;
                }
                // 分裂沿途的满节点
                if (childs[i]->isfull()) {
                    split(i);
                    if (less(keys[i], key)) i++;
                }
                return childs[i]->insert(key, value);
            }
        }
        void split(int i)
        {
            node *y = childs[i];
            node *z = y->split();
            n++;
            for (int j = n - 1; j > i; j--) {
                copy(j, j - 1);
                if (j > i + 1) childs[j] = childs[j - 1];
            }
            keys[i] = y->keys[y->n - 1];
            childs[i + 1] = z;
            // 如果当前节点为空，那么我们不仅需要从y中提升一个关键字，
            // 还需要从z中也提升一个关键字
            if (n == 1) {
                keys[n++] = z->keys[z->n - 1];
            }
            // 如果z是叶节点，那么就需要串到叶节点链表中
            if (z->isleaf) {
                z->left = y;
                z->right = y->right;
                if (y->right) y->right->left = z;
                y->right = z;
            }
        }
        node *split()
        {
            int M = keys.size();
            int t = ceil(M / 2.0);
            node *x = new node(M, isleaf);
            for (int i = t; i < M; i++) {
                x->copy(i - t, this, i);
                if (!isleaf) x->childs[i - t] = childs[i];
            }
            x->n = M - t;
            n = t;
            return x;
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
            if (isleaf) values[i] = x->values[j];
        }
        void copy(int i, int j)
        {
            copy(i, this, j);
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
        int n = 0; // 节点中存放的关键字数目
        node *left = nullptr, *right = nullptr;
        std::vector<key_type> keys;
        std::vector<value_type*> values;
        std::vector<node*> childs;
        Comparator comparator;
    };

    class iterator {
    public:
        iterator(node *x, int i) : pos(x, i) {  }
        bool valid() { return pos.first; }
        key_type *key() { return &pos.first->keys[pos.second]; }
        value_type *value() { return pos.first->values[pos.second]; }
        iterator& next()
        {
            auto& [x, i] = pos;
            if (x) {
                if (i + 1 < x->n) i++;
                else {
                    x = x->right;
                    i = 0;
                }
            }
            return *this;
        }
        iterator& prev()
        {
            auto& [x, i] = pos;
            if (x) {
                if (i - 1 >= 0) i--;
                else {
                    x = x->left;
                    if (x) i = x->n - 1;
                }
            }
            return *this;
        }
    private:
        pos_type pos;
    };

    bplus_tree() : root(new node(M, true))
    {
    }
    bplus_tree(int M) : M(M), root(new node(M, true))
    {
    }
    iterator first() { return iterator(begin, 0); }
    iterator last() { return iterator(end, end ? end->n - 1 : 0); }
    iterator find(const key_type& key)
    {
        auto [x, i] = root->find(key);
        if (x && x->isequal(i, key)) return iterator(x, i);
        return iterator(nullptr, 0);
    }
    iterator insert(const key_type& key, const value_type& value)
    {
        node *r = root;
        if (r->isfull()) {
            root = new node(M, false);
            root->childs[0] = r;
            root->split(0);
        }
        auto [ins, i] = root->insert(key, value);
        if (!begin || less(ins->keys[i], begin->keys[0])) {
            begin = ins;
        }
        if (!end || less(end->keys[end->n - 1], ins->keys[i])) {
            end = ins;
        }
        return iterator(ins, i);
    }
private:
    bplus_tree(const bplus_tree&) = delete;
    bplus_tree& operator=(const bplus_tree&) = delete;

    bool less(const key_type& l, const key_type& r)
    {
        return comparator(l, r);
    }

    // M为B+树的阶，B+树中关键字和子节点范围都是[ceil(M / 2), M]
    // 并且所有关键字必须都出现在叶子节点中
    const int M = 4; // M >= 3
    node *root = nullptr;
    node *begin = nullptr, *end = nullptr;
    Comparator comparator;
};

#endif // _BPLUS_TREE_H
