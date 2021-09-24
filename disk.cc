#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <errno.h>

#include "bplus_tree_db.h"

using namespace bplus_tree_db;

translation_table::translation_table(const std::string& filename, header_t *header)
    : filename(filename), header(header)
{
    fd = open(filename.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        panic("open(%s) error: %s", filename.c_str(), strerror(errno));
    }
    load_header();
}

node *translation_table::to_node(off_t off)
{
    auto it = translation_to_node.find(off);
    if (it != translation_to_node.end()) {
        return it->second;
    } else {
        node *node = load_node(off);
        put(off, node);
        return node;
    }
}

off_t translation_table::to_off(node *node)
{
    return translation_to_off.find(node)->second;
}

void translation_table::flush(node *root)
{
    if (root == nullptr) {
        panic("flush(root=nullptr)");
    }
    if (root->dirty) {
        save_node(header->root_off, root);
        root->dirty = false;
    }
    for (auto& [off, node] : translation_to_node) {
        if (node->dirty) {
            save_node(off, node);
            node->dirty = false;
        }
        if (off == header->root_off) continue;
        for (auto it = node->values.begin(); it != node->values.end(); ) {
            auto e = it++;
            delete *e;
        }
    }
    save_header();
    // 更好的做法是维护一个LRU缓存，缓存一些经常访问的节点
    // 不用每次清空
    for (auto it = translation_to_node.begin(); it != translation_to_node.end(); ) {
        auto e = it++;
        delete e->second;
    }
    translation_to_node.clear();
    translation_to_off.clear();
}

// ########################### file-header ###########################
// [magic][page-size][nodes][root-off][leaf-off][free-list-head][free-blocks]
void translation_table::fill_header(struct iovec *iov)
{
    iov[0].iov_base = &header->magic;
    iov[0].iov_len = sizeof(header->magic);
    iov[1].iov_base = &header->page_size;
    iov[1].iov_len = sizeof(header->page_size);
    iov[2].iov_base = &header->nodes;
    iov[2].iov_len = sizeof(header->nodes);
    iov[3].iov_base = &header->root_off;
    iov[3].iov_len = sizeof(header->root_off);
    iov[4].iov_base = &header->leaf_off;
    iov[4].iov_len = sizeof(header->leaf_off);
    iov[5].iov_base = &header->free_list_head;
    iov[5].iov_len = sizeof(header->free_list_head);
    iov[6].iov_base = &header->free_pages;
    iov[6].iov_len = sizeof(header->free_pages);
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
    if (magic != header->magic) {
        panic("unknown data file <%s>", filename.c_str());
    }
    struct iovec iov[7];
    fill_header(iov);
    lseek(fd, 0, SEEK_SET);
    readv(fd, iov, 7);
}

off_t translation_table::alloc_page()
{
    off_t off = header->free_list_head;
    if (header->free_pages > 0) {
        header->free_pages--;
        lseek(fd, off, SEEK_SET);
        read(fd, &header->free_list_head, sizeof(header->free_list_head));
    } else {
        header->free_list_head += header->page_size;
    }
    return off;
}

void translation_table::free_page(off_t off)
{
    lseek(fd, off, SEEK_SET);
    write(fd, &header->free_list_head, sizeof(header->free_list_head));
    header->free_list_head = off;
    header->free_pages++;
}

void translation_table::save_node(off_t off, node *node)
{
    std::string buf;
    buf.reserve(header->page_size);
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
    write(fd, buf.data(), header->page_size);
}

node *translation_table::load_node(off_t off)
{
    void *start = mmap(nullptr, header->page_size, PROT_READ, MAP_SHARED, fd, off);
    if (start == MAP_FAILED) {
        panic("load node failed from off=%lld: %s", off, strerror(errno));
    }
    char *buf = reinterpret_cast<char*>(start);
    node *node = new struct node(*buf);
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
    munmap(start, header->page_size);
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
