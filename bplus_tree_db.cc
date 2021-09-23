#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdarg.h>
#include <errno.h>

#include "bplus_tree_db.h"

#define KEY_LIMIT ((1u << 8) - 1)
#define VALUE_LIMIT (1024)

void bplus_tree_db::init()
{
    fd = open(filename.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        panic("open(%s) error: %s", filename.c_str(), strerror(errno));
    }
    load_header();
    if (header.nodes == 0) {
        header.root_off = alloc_page();
        header.leaf_off = header.root_off;
        root = new node(true);
    } else {
        root = load_node(header.root_off);
    }
}

bplus_tree_db::iterator bplus_tree_db::find(node *x, const key_t& key)
{
    int i = search(x, key);
    if (i == x->keys.size()) return iterator(this);
    if (x->leaf) {
        if (equal(x->keys[i], key)) return iterator(this, to_off(x), i);
        else return iterator(this);
    }
    return find(to_node(x->childs[i]), key);
}

void bplus_tree_db::insert(node *x, const key_t& key, const value_t& value)
{
    int i = search(x, key);
    int n = x->keys.size();
    if (x->leaf) {
        if (i < n && equal(x->keys[i], key)) {
            delete x->values[i];
            x->values[i] = new value_t(value);
        } else {
            x->resize(++n);
            for (int j = n - 2; j >= i; j--) {
                x->copy(j + 1, j);
            }
            if (less(key, to_node(header.leaf_off)->keys[0]))
                header.leaf_off = to_off(x);
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

void bplus_tree_db::split(node *x, int i)
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

bplus_tree_db::node *bplus_tree_db::split(node *x)
{
    int n = x->keys.size();
    int t = ceil(n / 2.0);
    node *y = new node(x->leaf);
    translation_put(alloc_page(), y);
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

bool bplus_tree_db::isfull(node *x, const key_t& key, const value_t& value)
{
    size_t used = x->used_bytes;
    if (x->leaf) {
        used += (1 + key.size()) + (2 + value.size());
    } else {
        used += (1 + KEY_LIMIT) + sizeof(off_t);
    }
    return used > header.page_size;
}

bool bplus_tree_db::check_limit(const key_t& key, const value_t& value)
{
    if (key.size() > KEY_LIMIT) {
        printf("key(%s) over the max-limit(%d)\n", key.c_str(), KEY_LIMIT);
        return false;
    }
    if (value.size() > VALUE_LIMIT) {
        printf("value(%s) over the max-limit(%d)\n", value.c_str(), VALUE_LIMIT);
        return false;
    }
    return true;
}

// ########################### file-header ###########################
// [magic][page-size][nodes][root-off][leaf-off][free-list-head][free-blocks]
void bplus_tree_db::fill_header(struct iovec *iov)
{
    iov[0].iov_base = &header.magic;
    iov[0].iov_len = sizeof(header.magic);
    iov[1].iov_base = &header.page_size;
    iov[1].iov_len = sizeof(header.page_size);
    iov[2].iov_base = &header.nodes;
    iov[2].iov_len = sizeof(header.nodes);
    iov[3].iov_base = &header.root_off;
    iov[3].iov_len = sizeof(header.root_off);
    iov[4].iov_base = &header.leaf_off;
    iov[4].iov_len = sizeof(header.leaf_off);
    iov[5].iov_base = &header.free_list_head;
    iov[5].iov_len = sizeof(header.free_list_head);
    iov[6].iov_base = &header.free_pages;
    iov[6].iov_len = sizeof(header.free_pages);
}

void bplus_tree_db::save_header()
{
    struct iovec iov[7];
    fill_header(iov);
    lseek(fd, 0, SEEK_SET);
    writev(fd, iov, 7);
}

void bplus_tree_db::load_header()
{
    int8_t magic;
    if (get_file_size() == 0) return;
    read(fd, &magic, sizeof(magic));
    if (magic != header.magic) {
        panic("unknown data file <%s>", filename.c_str());
    }
    struct iovec iov[7];
    fill_header(iov);
    lseek(fd, 0, SEEK_SET);
    readv(fd, iov, 7);
}

off_t bplus_tree_db::alloc_page()
{
    off_t off = header.free_list_head;
    if (header.free_pages > 0) {
        header.free_pages--;
        lseek(fd, off, SEEK_SET);
        read(fd, &header.free_list_head, sizeof(header.free_list_head));
    } else {
        header.free_list_head += header.page_size;
    }
    return off;
}

void bplus_tree_db::free_page(off_t off)
{
    lseek(fd, off, SEEK_SET);
    write(fd, &header.free_list_head, sizeof(header.free_list_head));
    header.free_list_head = off;
    header.free_pages++;
}

void bplus_tree_db::save_node(off_t off, node *node)
{
    std::string buf;
    buf.reserve(header.page_size);
    buf.append(1, node->leaf);
    uint16_t keynums = node->keys.size();
    buf.append((char*)&keynums, sizeof(keynums));
    for (auto& key : node->keys) {
        uint8_t keylen = key.size();
        buf.append((char*)&keylen, sizeof(keylen));
        buf.append(key);
    }
    if (node->leaf) {
        for (auto& value : node->values) {
            uint16_t valuelen = value->size();
            buf.append((char*)&valuelen, sizeof(valuelen));
            buf.append(*value);
        }
        buf.append((char*)&node->left, sizeof(node->left));
        buf.append((char*)&node->right, sizeof(node->right));
    } else {
        for (auto& child_off : node->childs) {
            buf.append((char*)&child_off, sizeof(child_off));
        }
    }
    lseek(fd, off, SEEK_SET);
    write(fd, buf.data(), header.page_size);
}

bplus_tree_db::node *bplus_tree_db::load_node(off_t off)
{
    void *start = mmap(nullptr, header.page_size, PROT_READ, MAP_SHARED, fd, off);
    if (start == MAP_FAILED) {
        panic("load node failed from off=%lld: %s", off, strerror(errno));
    }
    char *buf = reinterpret_cast<char*>(start);
    node *node = new class node(*buf);
    buf += 1;
    uint16_t keynums = *(uint16_t*)buf;
    buf += sizeof(uint16_t);
    node->keys.reserve(keynums);
    for (int i = 0; i < keynums; i++) {
        uint8_t keylen = *(uint8_t*)buf;
        buf += sizeof(uint8_t);
        node->keys.emplace_back(buf, keylen);
        buf += keylen;
    }
    if (node->leaf) {
        node->values.reserve(keynums);
        for (int i = 0; i < keynums; i++) {
            uint16_t valuelen = *(uint16_t*)buf;
            buf += sizeof(uint16_t);
            node->values.emplace_back(new value_t(buf, valuelen));
            buf += valuelen;
        }
        node->left = *(off_t*)buf;
        buf += sizeof(off_t);
        node->right = *(off_t*)buf;
        buf += sizeof(off_t);
    } else {
        node->childs.reserve(keynums);
        for (int i = 0; i < keynums; i++) {
            off_t child_off = *(off_t*)buf;
            node->childs.emplace_back(child_off);
            buf += sizeof(off_t);
        }
    }
    node->update(false);
    munmap(start, header.page_size);
    return node;
}

off_t bplus_tree_db::get_file_size()
{
    struct stat st;
    fstat(fd, &st);
    return st.st_size;
}

void bplus_tree_db::panic(const char *fmt, ...)
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
