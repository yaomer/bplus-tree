#ifndef _BPLUS_TREE_H
#define _BPLUS_TREE_H

#include <string>
#include <vector>

template <typename Key,
          typename T,
          typename Comparator = std::less<Key>>
class bplus_tree {
public:
    typedef Key key_type;
    typedef T value_type;
    struct leaf_node;
    typedef std::pair<leaf_node*, int> pos_type;
    typedef std::pair<key_type*, value_type*> res_type;

    struct node {
        node(int t)
        {
            keys.resize(max_keys(t));
            childs.resize(max_childs(t));
        }
        virtual ~node() {  }
        virtual pos_type find(const key_type& key) = 0;
        virtual void insert(const key_type& key, const value_type& value) = 0;

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

        bool less(const key_type& l, const key_type& r)
        {
            return comparator(l, r);
        }
        bool equal(const key_type& l, const key_type& r)
        {
            return !comparator(l, r) && !comparator(r, l);
        }

        // node *parent = nullptr;
        int n = 0; // 节点中存放的关键字数目
        std::vector<key_type> keys;
        std::vector<node*> childs; // 孩子节点数目等于n+1
        Comparator comparator;
    };

    struct inner_node : public node {
        inner_node(int t) : node(t), t(t) {  }
        ~inner_node() {  }
        pos_type find(const key_type& key) override
        {
            int i = this->search(key);
            // 最终会递归到叶子节点，从而结束查找
            return this->childs[i]->find(key);
        }
        void insert(const key_type& key, const value_type& value) override
        {
            int i = this->search(key);
            if (this->childs[i]->isfull()) {
                split(i);
                if (this->less(this->keys[i], key)) i++;
            }
            this->childs[i]->insert(key, value);
        }
        // 此时该内部节点(记为x)是非满的
        // 而i为一个使x->childs[i]为x的满子节点的下标(即x->childs[i]是满的)
        // B+树的所有key都需要出现在叶子节点，这点不同于B树，所以分裂时要注意
        void split(int i)
        {
            inner_node *x = this;
            node *y = x->childs[i];
            node *z;
            if (typeid(*y) == typeid(leaf_node)) {
                z = new leaf_node(t);
            } else {
                z = new inner_node(t);
            }
            z->n = t - 1;
            // 将节点y的后t - 1个关键字复制到z中
            for (int j = 0; j < t - 1; j++) {
                z->keys[j] = y->keys[j + t];
            }
            // 如果y不是叶子节点，则还需要修改孩子节点指针
            if (typeid(*y) == typeid(inner_node)) {
                for (int j = 0; j < t; j++) {
                    z->childs[j] = y->childs[j + t];
                }
            } else {
                leaf_node *z_leaf = dynamic_cast<leaf_node*>(z);
                leaf_node *y_leaf = dynamic_cast<leaf_node*>(y);
                // 如果y是叶子节点，则需要复制值指针，并将新节点z接到叶子节点的链表中
                for (int j = 0; j < t - 1; j++) {
                    z_leaf->values[j] = y_leaf->values[j + t];
                }
                z_leaf->left = y_leaf;
                z_leaf->right = y_leaf->right;
                y_leaf->right = z_leaf;
            }
            // 分裂后y拥有前t个关键字(仍包含需要提升的关键字)
            y->n = t;
            // 将i之后的孩子节点指针向后移一位，空出childs[i + 1]作为新节点z的位置
            for (int j = x->n; j > i; j--) {
                x->childs[j + 1] = x->childs[j];
            }
            x->childs[i + 1] = z;
            // 空出keys[i]，存放被提升的新关键字
            for (int j = x->n - 1; j >= i; j--) {
                x->keys[j + 1] = x->keys[j];
            }
            // y中的最后一个关键字keys[t - 1]将被提升到父节点x中
            x->keys[i] = y->keys[t - 1];
            x->n++;
        }
        const int t;
    };

    struct leaf_node : public node {
        leaf_node(int t) : node(t)
        {
            values.resize(max_keys(t));
        }
        ~leaf_node() {  }
        pos_type find(const key_type& key) override
        {
            int i = this->search(key);
            if (i >= this->n || !this->equal(this->keys[i], key)) return {nullptr, -1};
            return {this, i};
        }
        void insert(const key_type& key, const value_type& value) override
        {
            int i = this->search(key);
            if (i < this->n) {
                // 已存在我们就更新value
                if (this->equal(key, this->keys[i])) {
                    delete this->values[i];
                    this->values[i] = new value_type(value);
                    return;
                }
            }
            // 腾出一个位置来存放新的(key, value)
            for (int j = this->n - 1; j >= i; j--) {
                this->keys[j + 1] = this->keys[j];
                this->values[j + 1] = this->values[j];
            }
            this->keys[i] = key;
            this->values[i] = new value_type(value);
            this->n++;
        }

        std::vector<value_type*> values;
        leaf_node *left = nullptr;
        leaf_node *right = nullptr;
    };

    bplus_tree() : root(new leaf_node(t))
    {
    }
    bplus_tree(int t) : t(t), root(new leaf_node(t))
    {
    }
    void insert(const key_type& key, const value_type& value)
    {
        node *r = root;
        if (r->isfull()) {
            inner_node *s = new inner_node(t);
            root = s;
            s->childs[0] = r;
            s->split(0);
            s->insert(key, value);
        } else {
            r->insert(key, value);
        }
    }
    res_type find(const key_type& key)
    {
        auto [leaf, i] = root->find(key);
        if (leaf) {
            return { &leaf->keys[i], leaf->values[i] };
        } else {
            return { nullptr, nullptr };
        }
    }
    std::vector<res_type> range(const key_type& first, const key_type& end)
    {
        auto [leaf, start] = root->find(first);
        std::vector<res_type> res;
        while (leaf) {
            for (int i = start; i < leaf->n; i++) {
                if (less(leaf->keys[i], end)) {
                    // TODO: firstly reserve
                    res.emplace_back(&leaf->keys[i], leaf->values[i]);
                } else {
                    return res;
                }
            }
            leaf = leaf->right;
            start = 0;
        }
        return res;
    }
private:
    bplus_tree(const bplus_tree&) = delete;
    bplus_tree& operator=(const bplus_tree&) = delete;

    bool less(const key_type& l, const key_type& r)
    {
        return comparator(l, r);
    }

    static int max_keys(int t) { return 2 * t - 1; }
    static int max_childs(int t) { return 2 * t; }

    // t为B+树的最小度数
    // (除根节点以外)每个节点至少包含t-1个关键字，至少有t个孩子
    // 至多包含2t-1个关键字，至多有2t个孩子
    // 如果一个节点恰好有2t-1个关键字，就称它是满的
    const int t = 3; // t >= 2
    node *root = nullptr;
    Comparator comparator;
};

#endif // _BPLUS_TREE_H
