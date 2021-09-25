#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <errno.h>

#include "db.h"

using namespace bplus_tree_db;

translation_table::translation_table(const std::string& filename, DB *db)
    : filename(filename), db(db), lru_cap(1024)
{
    fd = open(filename.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        panic("open(%s) error: %s", filename.c_str(), strerror(errno));
    }
    load_header();
}

node *translation_table::lru_get(off_t off)
{
    auto it = translation_to_node.find(off);
    if (it == translation_to_node.end()) return nullptr;
    if (it->second.pos != cache_list.begin()) {
        cache_list.erase(it->second.pos);
        cache_list.push_front(it->first);
        it->second.pos = cache_list.begin();
    }
    return it->second.x.get();
}

// 放入的<off, node>一定是原先不存在的
void translation_table::lru_put(off_t off, node *node)
{
    if (translation_to_node.count(off)) {
        panic("lru_put: off=%lld exists", off);
    }
    if (cache_list.size() == lru_cap) {
        off_t evict_off = cache_list.back();
        auto *evict_node = translation_to_node[evict_off].x.get();
        if (evict_node->dirty) {
            save_node(evict_off, evict_node);
        }
        translation_to_off.erase(evict_node);
        translation_to_node.erase(evict_off);
        cache_list.pop_back();
    }
    cache_list.push_front(off);
    translation_to_node.emplace(off, cache_node(node, cache_list.begin()));
    translation_to_off.emplace(node, off);
}

// 退出时flush所有的dirty node
void translation_table::lru_flush()
{
    for (auto& [node, off] : translation_to_off) {
        if (node->dirty) {
            save_node(off, node);
        }
    }
    translation_to_node.clear();
    if (db->root->dirty) {
        save_node(db->header.root_off, db->root);
    }
    save_header();
    fsync(fd);
}

node *translation_table::to_node(off_t off)
{
    if (off == db->header.root_off) return db->root;
    node *node = lru_get(off);
    if (node == nullptr) {
        node = load_node(off);
        lru_put(off, node);
    }
    return node;
}

off_t translation_table::to_off(node *node)
{
    if (node == db->root) return db->header.root_off;
    return translation_to_off.find(node)->second;
}

// 将每次修改操作涉及到的所有dirty node写回磁盘
// 但节点本身在被淘汰前仍驻留在内存中
void translation_table::flush()
{
    for (auto *node : change_list) {
        if (node->dirty) {
            save_node(to_off(node), node);
            node->dirty = false;
        }
    }
    change_list.clear();
    save_header();
    fsync(fd);
}

// ########################### file-header ###########################
// [magic][page-size][nodes][root-off][leaf-off][free-list-head][free-pages]
void translation_table::fill_header(struct iovec *iov)
{
    iov[0].iov_base = &db->header.magic;
    iov[0].iov_len = sizeof(db->header.magic);
    iov[1].iov_base = &db->header.page_size;
    iov[1].iov_len = sizeof(db->header.page_size);
    iov[2].iov_base = &db->header.nodes;
    iov[2].iov_len = sizeof(db->header.nodes);
    iov[3].iov_base = &db->header.root_off;
    iov[3].iov_len = sizeof(db->header.root_off);
    iov[4].iov_base = &db->header.leaf_off;
    iov[4].iov_len = sizeof(db->header.leaf_off);
    iov[5].iov_base = &db->header.free_list_head;
    iov[5].iov_len = sizeof(db->header.free_list_head);
    iov[6].iov_base = &db->header.free_pages;
    iov[6].iov_len = sizeof(db->header.free_pages);
}

void translation_table::save_header()
{
    struct iovec iov[7];
    fill_header(iov);
    lseek(fd, 0, SEEK_SET);
    writev(fd, iov, 7);
}

void translation_table::load_header()
{
    int8_t magic;
    struct stat st;
    fstat(fd, &st);
    if (st.st_size == 0) return;
    read(fd, &magic, sizeof(magic));
    if (magic != db->header.magic) {
        panic("unknown data file <%s>", filename.c_str());
    }
    struct iovec iov[7];
    fill_header(iov);
    lseek(fd, 0, SEEK_SET);
    readv(fd, iov, 7);
}

// ############################# page #############################

off_t translation_table::alloc_page()
{
    off_t off = db->header.free_list_head;
    if (db->header.free_pages > 0) {
        db->header.free_pages--;
        lseek(fd, off, SEEK_SET);
        read(fd, &db->header.free_list_head, sizeof(db->header.free_list_head));
    } else {
        db->header.free_list_head += db->header.page_size;
    }
    return off;
}

void translation_table::free_page(off_t off)
{
    lseek(fd, off, SEEK_SET);
    write(fd, &db->header.free_list_head, sizeof(db->header.free_list_head));
    db->header.free_list_head = off;
    db->header.free_pages++;
}

// ############################# encoding #############################

static void encode8(std::string& buf, uint8_t n)
{
    buf.append(reinterpret_cast<char*>(&n), 1);
}

static void encode16(std::string& buf, uint16_t n)
{
    buf.append(reinterpret_cast<char*>(&n), 2);
}

static void encodeoff(std::string& buf, off_t off)
{
    buf.append(reinterpret_cast<char*>(&off), sizeof(off));
}

static uint8_t decode8(char **ptr)
{
    uint8_t n = *reinterpret_cast<uint8_t*>(*ptr);
    *ptr += 1;
    return n;
}

static uint16_t decode16(char **ptr)
{
    uint16_t n = *reinterpret_cast<uint16_t*>(*ptr);
    *ptr += 2;
    return n;
}

static off_t decodeoff(char **ptr)
{
    off_t n = *reinterpret_cast<off_t*>(*ptr);
    *ptr += sizeof(n);
    return n;
}

// ############################# node #############################
// 1 bytes [leaf]
// 2 bytes [key-nums]
// all-keys [key -> 1 bytes [key-len] [key]]
// ########################### inner-node ###########################
// if leaf = 0:
// all-child-offs [child-off -> sizeof(off_t)]
// ########################### leaf-node ###########################
// if leaf = 1:
// all-values [value -> 2 bytes [value-len] [value]]
// left-off right-off -> sizeof(off_t)
//
void node::update(bool dirty)
{
    page_used = 1 + 2; // leaf and key-nums
    for (auto& key : keys) page_used += 1 + key.size();
    if (leaf) {
        for (auto& value : values) page_used += 2 + value->size();
        page_used += sizeof(off_t) * 2;
    } else {
        page_used += sizeof(off_t) * childs.size();
    }
    this->dirty = dirty;
}

void translation_table::save_node(off_t off, node *node)
{
    std::string buf;
    buf.reserve(db->header.page_size);
    encode8(buf, node->leaf);
    encode16(buf, node->keys.size());
    for (auto& key : node->keys) {
        encode8(buf, key.size());
        buf.append(key);
    }
    if (node->leaf) {
        for (auto& value : node->values) {
            encode16(buf, value->size());
            buf.append(*value);
        }
        encodeoff(buf, node->left);
        encodeoff(buf, node->right);
    } else {
        for (auto& child_off : node->childs) {
            encodeoff(buf, child_off);
        }
    }
    lseek(fd, off, SEEK_SET);
    write(fd, buf.data(), db->header.page_size);
}

node *translation_table::load_node(off_t off)
{
    void *start = mmap(nullptr, db->header.page_size, PROT_READ, MAP_SHARED, fd, off);
    if (start == MAP_FAILED) {
        panic("load node failed from off=%lld: %s", off, strerror(errno));
    }
    char *buf = reinterpret_cast<char*>(start);
    uint8_t leaf = decode8(&buf);
    node *node = new struct node(leaf);
    uint16_t keynums = decode16(&buf);
    node->keys.reserve(keynums);
    for (int i = 0; i < keynums; i++) {
        uint8_t keylen = decode8(&buf);
        node->keys.emplace_back(buf, keylen);
        buf += keylen;
    }
    if (node->leaf) {
        node->values.reserve(keynums);
        for (int i = 0; i < keynums; i++) {
            uint16_t valuelen = decode16(&buf);
            node->values.emplace_back(new value_t(buf, valuelen));
            buf += valuelen;
        }
        node->left = decodeoff(&buf);
        node->right = decodeoff(&buf);
    } else {
        node->childs.reserve(keynums);
        for (int i = 0; i < keynums; i++) {
            node->childs.emplace_back(decodeoff(&buf));
        }
    }
    node->update(false);
    munmap(start, db->header.page_size);
    return node;
}

void translation_table::panic(const char *fmt, ...)
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
