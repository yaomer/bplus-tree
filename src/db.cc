#include <unordered_set>

#include <sys/stat.h>

#include "db.h"

using namespace bpdb;

namespace bpdb {

struct limit_t limit;

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
    cur_tid = std::this_thread::get_id();
    comparator = [](const key_t& l, const key_t& r) {
        return std::less<key_t>()(l, r);
    };
    if (dbname.empty()) panic("dbname is empty");
    if (dbname.back() != '/') dbname.push_back('/');
    mkdir(dbname.c_str(), 0777);
    dbfile = dbname + "dump.db";
    fd = open_db_file();
    limit.over_value = header.page_size / 16;
    translation_table.init();
    page_manager.init();
    if (header.root_off == 0) {
        header.root_off = page_manager.alloc_page();
        header.leaf_off = header.last_off = header.root_off;
        root.reset(new node(true));
    } else {
        root.reset(translation_table.load_node(header.root_off));
    }
    redo_log.init();
}

int DB::open_db_file()
{
    int fd = open(dbfile.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        panic("open(%s): %s", dbfile.c_str(), strerror(errno));
    }
    return fd;
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

namespace bpdb {
    thread_local bool hold_root = false; // 是否持有root_shmtx
}

#define __lock_shared_root() \
    root_shmtx.lock_shared(); \
    hold_root = true

#define __lock_root() \
    root_shmtx.lock(); \
    hold_root = true

#define __unlock_root() \
    root_shmtx.unlock(); \
    hold_root = false

#define __unlock_shared(x) \
    if (hold_root) { root_shmtx.unlock_shared(); hold_root = false; } \
    else (x)->unlock_shared()

#define __unlock(x) \
    if (hold_root) { root_shmtx.unlock(); hold_root = false; } \
    else (x)->unlock()

bool DB::find(const std::string& key, std::string *value)
{
    __lock_shared_root();
    auto [x, i] = find(root.get(), key);
    if (x) {
        translation_table.load_real_value(x->values[i], value);
        __unlock_shared(x);
        return true;
    }
    return false;
}

std::pair<node*, int> DB::find(node *x, const key_t& key)
{
    node *child;
    int i = search(x, key);
    if (i == x->keys.size()) goto not_found;
    if (x->leaf) {
        if (equal(x->keys[i], key)) return { x, i };
        else goto not_found;
    }
    child = to_node(x->childs[i]);
    child->lock_shared();
    __unlock_shared(x);
    return find(child, key);
not_found:
    __unlock_shared(x);
    return { nullptr, 0 };
}

value_t *DB::build_new_value(const std::string& value)
{
    value_t *v = new value_t();
    v->reallen = value.size();
    if (value.size() <= limit.over_value) {
        v->val = new std::string(value);
    } else {
        v->val = const_cast<std::string*>(&value);
    }
    return v;
}

void DB::insert(const std::string& key, const std::string& value)
{
    if (!check_limit(key, value)) return;
    wait_if_check_point();
    sync_check_point++;
    redo_log.append(LOG_TYPE_INSERT, &key, &value);
    value_t *v = build_new_value(value);
    __lock_root();
    node *r = root.get();
    if (isfull(r, key, v)) {
        root.release();
        root.reset(new node(false));
        root->resize(1);
        lock_header();
        root->childs[0] = header.root_off;
        translation_table.put(header.root_off, r);
        header.root_off = page_manager.alloc_page();
        unlock_header();
        split(root.get(), 0, key);
    }
    insert(root.get(), key, v);
    sync_check_point--;
}

void DB::insert(node *x, const key_t& key, value_t *value)
{
    int i = search(x, key);
    int n = x->keys.size();
    if (x->leaf) {
        if (i < n && equal(x->keys[i], key)) {
            translation_table.free_value(x->values[i]);
            x->values[i] = value;
        } else {
            x->resize(++n);
            for (int j = n - 2; j >= i; j--) {
                x->copy(j + 1, j);
            }
            x->keys[i] = key;
            x->values[i] = value;
            lock_header();
            node *leaf = to_node(header.leaf_off);
            node *last = to_node(header.last_off);
            if (x != leaf) {
                leaf->lock_shared();
                if (less(key, leaf->keys[0])) header.leaf_off = to_off(x);
                leaf->unlock_shared();
            }
            if (x != last) {
                last->lock_shared();
                if (less(last->keys.back(), key)) header.last_off = to_off(x);
                last->unlock_shared();
            }
            header.key_nums++;
            unlock_header();
        }
        x->update();
        __unlock(x);
    } else {
        if (i == n) {
            x->keys[--i] = key;
            x->update();
        }
        // 我们先尝试获取子节点的写锁
        //
        // 1) 如果子节点是满的，需要进行分裂，那么我们就同时持有父子节点的写锁，
        // 直至分裂操作完成，然后进入2)
        //
        // 2) 如果子节点是安全的，那么我们就释放父节点的写锁，进入下一层
        node *child = to_node(x->childs[i]);
        child->lock();
        if (isfull(child, key, value)) {
            split(x, i, key);
            // for mid-split, `last_off` should point to the split right child node
            lock_header();
            if (header.last_off == x->childs[i]) header.last_off = x->childs[i + 1];
            unlock_header();
            if (less(x->keys[i], key)) {
                child->unlock();
                child = to_node(x->childs[++i]);
                child->lock();
            }
        }
        __unlock(x);
        insert(child, key, value);
    }
}

void DB::split(node *x, int i, const key_t& key)
{
    node *y = to_node(x->childs[i]);
    int type = get_split_type(y, key);
    node *z = split(y, type);
    int n = x->keys.size();
    x->resize(++n);
    for (int j = n - 1; j > i; j--) {
        x->copy(j, j - 1);
        if (j > i + 1) x->childs[j] = x->childs[j - 1];
    }
    x->keys[i] = type == LEFT_INSERT_SPLIT ? key : y->keys.back();
    if (n == 2) {
        if (type == MID_SPLIT) x->keys[n - 1] = z->keys.back();
        else if (type == RIGHT_INSERT_SPLIT) x->keys[n - 1] = key;
        else x->keys[n - 1] = y->keys.back();
    }
    x->childs[i + 1] = to_off(z);
    if (type == LEFT_INSERT_SPLIT)
        std::swap(x->childs[i], x->childs[i + 1]);
    if (z->leaf)
        link_leaf(z, y, type);
    x->update();
}

node *DB::split(node *x, int type)
{
    node *y = new node(x->leaf);
    translation_table.put(page_manager.alloc_page(), y);
    if (type == MID_SPLIT) {
        int n = x->keys.size();
        int point = ceil(n / 2.0);
        y->resize(n - point);
        for (int i = point; i < n; i++) {
            y->copy(i - point, x, i);
            if (!x->leaf) y->childs[i - point] = x->childs[i];
        }
        x->remove_from(point);
        y->update();
    }
    return y;
}

// 节点分裂默认是从中间分裂
// 这意味着顺序插入操作可能会导致x中大约一半的空间后面无法被利用
// 针对这种情况，我们可以使用一种简单直接的办法来进行优化：
// 当在叶节点的最左端或最右端插入时，我们就进行插入点分裂而非中间分裂
// 1) right-insert-point-split
// [1 2 3] (insert 4) -> [3 4]
//                      /     \
//                   [1 2 3]->[4]
// 2) left-insert-point-split
// [2 3 4] (insert 1) -> [1 4]
//                      /     \
//                     [1]->[2 3 4]
int DB::get_split_type(node *x, const key_t& key)
{
    int type = MID_SPLIT;
    if (x->leaf) {
        lock_header();
        node *leaf = to_node(header.leaf_off);
        node *last = to_node(header.last_off);
        if (leaf != x) leaf->lock_shared();
        if (last != x) last->lock_shared();
        unlock_header();
        if (x == last && less(last->keys.back(), key)) {
            type = RIGHT_INSERT_SPLIT;
        } else if (x == leaf && less(key, leaf->keys[0])) {
            type = LEFT_INSERT_SPLIT;
        }
        if (last != x) last->unlock_shared();
        if (leaf != x) leaf->unlock_shared();
    }
    return type;
}

void DB::link_leaf(node *z, node *y, int type)
{
    if (type == LEFT_INSERT_SPLIT) { // [z y]
        z->right = to_off(y);
        z->left = y->left;
        if (y->left > 0) {
            node *r = to_node(y->left);
            r->right = to_off(z);
            r->dirty = true;
        }
        y->left = to_off(z);
    } else { // [y z]
        z->left = to_off(y);
        z->right = y->right;
        if (y->right > 0) {
            node *r = to_node(y->right);
            r->left = to_off(z);
            r->dirty = true;
        }
        y->right = to_off(z);
    }
    z->dirty = true;
    y->dirty = true;
}

void DB::erase(const key_t& key)
{
    wait_if_check_point();
    sync_check_point++;
    redo_log.append(LOG_TYPE_ERASE, &key);
    __lock_root();
    erase(root.get(), key, nullptr);
    __lock_root();
    if (!root->leaf && root->keys.size() == 1) {
        off_t off = root->childs[0];
        node *r = to_node(off);
        translation_table.release_root(r);
        root.reset(r);
        __unlock_root();
        lock_header();
        page_manager.free_page(header.root_off);
        header.root_off = off;
        unlock_header();
    } else {
        __unlock_root();
    }
    sync_check_point--;
}

void DB::erase(node *r, const key_t& key, node *precursor)
{
    int i = search(r, key);
    int n = r->keys.size();
    if (i == n) return;
    if (r->leaf) {
        if (i < n && equal(r->keys[i], key)) {
            translation_table.free_value(r->values[i]);
            r->remove(i);
            lock_header();
            header.key_nums--;
            unlock_header();
        }
        __unlock(r);
        return;
    }
    node *x = to_node(r->childs[i]);
    if (x != precursor) x->lock();
    if (!precursor && (i < n && equal(r->keys[i], key))) {
        // 这种情况下，我们就需要一直持有当前precursor的写锁，直至整个删除操作完成
        precursor = get_precursor(x);
    }
    if (precursor) {
        r->keys[i] = precursor->keys[precursor->keys.size() - 2];
        r->update();
    }
    size_t t = header.page_size / 2;
    if (x->page_used >= t) {
        __unlock(r);
        erase(x, key, precursor);
        return;
    }
    node *y = i - 1 >= 0 ? to_node(r->childs[i - 1]) : nullptr;
    node *z = i + 1 < r->keys.size() ? to_node(r->childs[i + 1]) : nullptr;
    if (y && y != precursor) y->lock();
    if (z && z != precursor) z->lock();
    if (y && y->page_used >= t) {
        borrow_from_left(r, x, y, i - 1);
        __unlock(r);
        y->unlock();
        erase(x, key, precursor);
    } else if (z && z->page_used >= t) {
        borrow_from_right(r, x, z, i);
        __unlock(r);
        z->unlock();
        erase(x, key, precursor);
    } else {
        if (y) {
            off_t off = r->childs[i - 1];
            r->remove(i - 1);
            r->childs[i - 1] = off;
            merge(y, x);
            __unlock(r);
            erase(y, key, precursor);
        } else {
            off_t off = r->childs[i];
            r->remove(i);
            r->childs[i] = off;
            merge(x, z);
            __unlock(r);
            erase(x, key, precursor);
        }
    }
}

node *DB::get_precursor(node *x)
{
    node *r = x;
    while (!x->leaf) {
        node *y = to_node(x->childs[x->keys.size() - 1]);
        y->lock();
        if (x != r) x->unlock();
        x = y;
    }
    return x;
}

void DB::borrow_from_right(node *r, node *x, node *z, int i)
{
    r->copy(i, z, 0);
    int n = x->keys.size();
    x->resize(++n);
    x->copy(n - 1, z, 0);
    if (!x->leaf) x->childs[n - 1] = z->childs[0];
    z->remove(0);
    r->update();
    x->update();
}

void DB::borrow_from_left(node *r, node *x, node *y, int i)
{
    int n = x->keys.size();
    x->resize(n + 1);
    for (int j = n - 1; j >= 0; j--) {
        x->copy(j + 1, j);
        if (!x->leaf) x->childs[j + 1] = x->childs[j];
    }
    n = y->keys.size();
    x->copy(0, y, n - 1);
    if (!x->leaf) x->childs[0] = y->childs[n - 1];
    y->remove(--n);
    r->copy(i, y, n - 1);
    r->update();
    x->update();
}

void DB::merge(node *y, node *x)
{
    lock_header();
    if (header.leaf_off == to_off(x)) header.leaf_off = to_off(y);
    if (header.last_off == to_off(x)) header.last_off = to_off(y);
    unlock_header();
    int xn = x->keys.size();
    int yn = y->keys.size();
    y->resize(yn + xn);
    for (int j = 0; j < xn; j++) {
        y->copy(j + yn, x, j);
        if (!y->leaf) y->childs[j + yn] = x->childs[j];
    }
    if (y->leaf) {
        y->right = x->right;
        if (x->right > 0) {
            node *r = to_node(x->right);
            r->left = to_off(y);
            r->dirty = true;
        }
    }
    x->resize(0);
    translation_table.free_node(x);
    y->update();
}

// 查找x->keys[]中大于等于key的关键字的索引位置
int DB::search(node *x, const key_t& key)
{
    auto comp = comparator;
    auto p = std::lower_bound(x->keys.begin(), x->keys.end(), key, comp);
    if (p == x->keys.end()) return x->keys.size();
    return std::distance(x->keys.begin(), p);
}

bool DB::isfull(node *x, const key_t& key, value_t *value)
{
    size_t page_used = x->page_used;
    if (x->leaf) {
        page_used += (limit.key_len_field + key.size()) + (limit.value_len_field + std::min(limit.over_value, (size_t)value->reallen));
    } else {
        page_used += (limit.key_len_field + limit.max_key) + sizeof(off_t);
    }
    return page_used > header.page_size;
}

bool DB::check_limit(const std::string& key, const std::string& value)
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

void DB::rebuild()
{
    char tmpfile[] = "tmp.XXXXXX";
    mktemp(tmpfile);
    DB *tmpdb = new DB(tmpfile);
    auto it = new_iterator();
    for (it.seek_to_first(); it.valid(); it.next()) {
        tmpdb->insert(it.key(), it.value());
    }
    delete tmpdb;
    rename(tmpfile, dbfile.c_str());
    init();
}
