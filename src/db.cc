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

void DB::check_options()
{
    static std::unordered_set<int> valid_pages = {
        1024 * 4,
        1024 * 8,
        1024 * 16,
        1024 * 32,
        1024 * 64,
    };
    if (!valid_pages.count(ops.page_size)) {
        panic("The optional value of `page_size` is (4K, 8K, 16K, 32K or 64K)");
    }
    header.page_size = ops.page_size;
    translation_table.set_cache_cap(ops.page_cache_slots);
    if (ops.keycomp) {
        comparator = ops.keycomp;
    } else {
        comparator = [](const key_t& l, const key_t& r) {
            return std::less<key_t>()(l, r);
        };
    }
    if (ops.wal_sync != 0 && ops.wal_sync != 1) {
        panic("The optional value of `wal_sync` is (0 or 1)");
    }
}

void DB::init()
{
    cur_tid = std::this_thread::get_id();
    if (dbname.empty()) panic("dbname is empty");
    if (dbname.back() != '/') dbname.push_back('/');
    mkdir(dbname.c_str(), 0777);
    dbfile = dbname + "dump.db";
    fd = open_db_file();
    limit.over_value = header.page_size / 16;
    translation_table.init();
    page_manager.init();
    if (header.root_id == 0) {
        header.root_id = page_manager.alloc_page();
        header.leaf_id = header.root_id;
        root.reset(new node(true));
    } else {
        root.reset(translation_table.load_node(header.root_id));
    }
    logger.init();
}

int DB::open_db_file()
{
    int fd = open(dbfile.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        panic("open(%s): %s", dbfile.c_str(), strerror(errno));
    }
    return fd;
}

// 数据库中一般将保护对内存数据结构的并发访问的锁成为latch(闩锁)
// 而将事务隔离相关的锁称为lock，保护的主要是数据库逻辑内容，通常锁定时间很长
//
// 并发条件下，一种可能会出现死锁的情况是父子节点之间的遍历，不过由于我们插入/删除操作
// 采用的都是自上而下的分裂/合并操作，所以这种情况的死锁不会发生。
//
// 另一种就是同层相邻节点遍历的情景了。
// 一个线程正向遍历，一个线程反向遍历就可能会造成死锁。
// 为了避免出现这种情况，我们在正向或反向遍历B-tree同层节点时，只要遇到latch获取失败，
// 就立即释放掉自己占有的latch，从而让冲突的对方能继续执行下去，而自己则进行一次从root到leaf的重试。
// 考虑到两个冲突的线程可能会同时重试的情况，我们规定冲突时反向的线程进行重试，
// 这样可以保证冲突时只有一个线程会重试，另一个线程会继续执行。

namespace bpdb {
    thread_local bool hold_root_latch = false; // 是否持有root_latch
    thread_local bool lock_conflict_retry = false;
}

void DB::lock_shared_root()
{
    root_latch.lock_shared();
    hold_root_latch = true;
}

void DB::lock_root()
{
    root_latch.lock();
    hold_root_latch = true;
}

void DB::unlock_shared_root()
{
    root_latch.unlock_shared();
    hold_root_latch = false;
}

void DB::unlock_root()
{
    root_latch.unlock();
    hold_root_latch = false;
}

void DB::unlock_shared(node *node)
{
    if (hold_root_latch) {
        unlock_shared_root();
    } else {
        node->unlock_shared();
    }
}

void DB::unlock(node *node)
{
    if (hold_root_latch) {
        unlock_root();
    } else {
        node->unlock();
    }
}

bool DB::find(const std::string& key, std::string *value)
{
    lock_shared_root();
    auto [x, i] = find(root.get(), key);
    if (x) {
        translation_table.load_real_value(x->values[i], value);
        unlock_shared(x);
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
    unlock_shared(x);
    return find(child, key);
not_found:
    unlock_shared(x);
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
    logger.append(LOG_TYPE_INSERT, &key, &value);
    value_t *v = build_new_value(value);
    lock_root();
    node *r = root.get();
    if (isfull(r, key, v)) {
        root.release();
        root.reset(new node(false));
        root->resize(1);
        lock_header();
        root->childs[0] = header.root_id;
        translation_table.put(header.root_id, r);
        header.root_id = page_manager.alloc_page();
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
            update_header_in_insert(x, key);
        }
        x->update();
        unlock(x);
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
            if (less(x->keys[i], key)) {
                child->unlock();
                child = to_node(x->childs[++i]);
                child->lock();
            }
        }
        unlock(x);
        insert(child, key, value);
    }
}

void DB::update_header_in_insert(node *x, const key_t& key)
{
    lock_header();
    page_id_t leaf_id = header.leaf_id;
    unlock_header();
    // 我们不能在lock_header()的情况下去lock(leaf)，这很可能会造成死锁
    // T1: hold(header), require(leaf)
    // T2: hold(leaf), require(header)
    node *leaf = to_node(leaf_id);
    // 因为我们并没有持有leaf的父节点，所以它可能会被其他线程删除掉
    if (leaf && leaf != x) leaf->lock_shared();
    lock_header();
    if (leaf && !leaf->deleted && less(key, leaf->keys[0])) header.leaf_id = to_page_id(x);
    if (leaf && leaf != x) leaf->unlock_shared();
    header.key_nums++;
    unlock_header();
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
    x->childs[i + 1] = to_page_id(z);
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
        if (x->right == 0 && less(x->keys.back(), key)) {
            type = RIGHT_INSERT_SPLIT;
        } else if (x->left == 0 && less(key, x->keys[0])) {
            type = LEFT_INSERT_SPLIT;
        }
    }
    return type;
}

void DB::link_leaf(node *z, node *y, int type)
{
    if (type == LEFT_INSERT_SPLIT) { // [z y]
        z->right = to_page_id(y);
        z->left = y->left;
        if (y->left > 0) {
            node *r = to_node(y->left);
            r->lock();
            r->right = to_page_id(z);
            r->dirty = true;
            r->unlock();
        }
        y->left = to_page_id(z);
    } else { // [y z]
        z->left = to_page_id(y);
        z->right = y->right;
        if (y->right > 0) {
            node *r = to_node(y->right);
            r->lock();
            r->left = to_page_id(z);
            r->dirty = true;
            r->unlock();
        }
        y->right = to_page_id(z);
    }
    z->dirty = true;
    y->dirty = true;
}

void DB::erase(const key_t& key)
{
    wait_if_check_point();
    sync_check_point++;
    logger.append(LOG_TYPE_ERASE, &key);
    lock_root();
    erase(root.get(), key, nullptr);
    lock_root();
    if (!root->leaf && root->keys.size() == 1) {
        page_id_t page_id = root->childs[0];
        node *r = to_node(page_id);
        r->lock_shared();
        translation_table.release_root(r);
        r->unlock_shared();
        root.reset(r);
        unlock_root();
        lock_header();
        page_manager.free_page(header.root_id);
        header.root_id = page_id;
        unlock_header();
    } else {
        unlock_root();
    }
    sync_check_point--;
}

void DB::erase(node *r, const key_t& key, node *precursor)
{
    int i = search(r, key);
    int n = r->keys.size();
    if (i == n) { unlock(r); return; }
    if (r->leaf) {
        if (i < n && equal(r->keys[i], key)) {
            translation_table.free_value(r->values[i]);
            r->remove(i);
            lock_header();
            header.key_nums--;
            unlock_header();
        }
        if (precursor && precursor != r) precursor->unlock();
        unlock(r);
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
        unlock(r);
        erase(x, key, precursor);
        return;
    }
    node *y = i - 1 >= 0 ? to_node(r->childs[i - 1]) : nullptr;
    node *z = i + 1 < r->keys.size() ? to_node(r->childs[i + 1]) : nullptr;
    if (y && y != precursor) y->lock();
    if (z && z != precursor) z->lock();
    if (y && y->page_used >= t) {
        if (z && z != precursor) z->unlock();
        borrow_from_left(r, x, y, i - 1);
        unlock(r);
        if (y != precursor) y->unlock();
        erase(x, key, precursor);
    } else if (z && z->page_used >= t) {
        if (y && y != precursor) y->unlock();
        borrow_from_right(r, x, z, i);
        unlock(r);
        if (z != precursor) z->unlock();
        erase(x, key, precursor);
    } else {
        if (y) {
            if (z && z != precursor) z->unlock();
            page_id_t page_id = r->childs[i - 1];
            r->remove(i - 1);
            r->childs[i - 1] = page_id;
            merge(y, x);
            unlock(r);
            erase(y, key, precursor);
        } else {
            page_id_t page_id = r->childs[i];
            r->remove(i);
            r->childs[i] = page_id;
            merge(x, z);
            unlock(r);
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
    if (header.leaf_id == to_page_id(x)) header.leaf_id = to_page_id(y);
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
            r->lock();
            r->left = to_page_id(y);
            r->dirty = true;
            r->unlock();
        }
    }
    x->free();
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
        page_used += (limit.key_len_field + limit.max_key) + sizeof(page_id_t);
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
    DB *tmpdb = new DB(options(), tmpfile);
    auto it = new_iterator();
    for (it.seek_to_first(); it.valid(); it.next()) {
        tmpdb->insert(it.key(), it.value());
    }
    delete tmpdb;
    rename(tmpfile, dbfile.c_str());
    init();
}
