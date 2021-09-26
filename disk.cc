#include <sys/stat.h>
#include <sys/mman.h>

#include "db.h"

using namespace bplus_tree_db;

translation_table::translation_table(DB *db)
    : db(db), lru_cap(1024)
{
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
        // node被淘汰时，相应的value就应该从over_page_off中移除，不然就会导致野指针
        for (auto value : evict_node->values) {
            if (over_page_off.count(value)) {
                over_page_off.erase(value);
            }
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
        save_node(db->header.root_off, db->root.get());
    }
    save_header();
    fsync(db->fd);
}

node *translation_table::to_node(off_t off)
{
    if (off == db->header.root_off) return db->root.get();
    node *node = lru_get(off);
    if (node == nullptr) {
        node = load_node(off);
        lru_put(off, node);
    }
    return node;
}

off_t translation_table::to_off(node *node)
{
    if (node == db->root.get()) return db->header.root_off;
    return translation_to_off.find(node)->second;
}

// 将每次修改操作涉及到的所有dirty node写回磁盘
// 但节点本身在被淘汰前仍驻留在内存中
void translation_table::flush()
{
    for (auto node : change_list) {
        if (node->dirty) {
            save_node(to_off(node), node);
            node->dirty = false;
        }
    }
    change_list.clear();
    save_header();
    fsync(db->fd);
}

// ########################### file-header ###########################
// [magic][page-size][key-nums][root-off][leaf-off][free-list-head][free-pages]
// [over-page-list-head][over-pages]
void translation_table::fill_header(struct iovec *iov)
{
    iov[0].iov_base = &db->header.magic;
    iov[0].iov_len = sizeof(db->header.magic);
    iov[1].iov_base = &db->header.page_size;
    iov[1].iov_len = sizeof(db->header.page_size);
    iov[2].iov_base = &db->header.key_nums;
    iov[2].iov_len = sizeof(db->header.key_nums);
    iov[3].iov_base = &db->header.root_off;
    iov[3].iov_len = sizeof(db->header.root_off);
    iov[4].iov_base = &db->header.leaf_off;
    iov[4].iov_len = sizeof(db->header.leaf_off);
    iov[5].iov_base = &db->header.free_list_head;
    iov[5].iov_len = sizeof(db->header.free_list_head);
    iov[6].iov_base = &db->header.free_pages;
    iov[6].iov_len = sizeof(db->header.free_pages);
    iov[7].iov_base = &db->header.over_page_list_head;
    iov[7].iov_len = sizeof(db->header.over_page_list_head);
    iov[8].iov_base = &db->header.over_pages;
    iov[8].iov_len = sizeof(db->header.over_pages);
}

void translation_table::save_header()
{
    struct iovec iov[9];
    fill_header(iov);
    lseek(db->fd, 0, SEEK_SET);
    writev(db->fd, iov, 9);
}

void translation_table::load_header()
{
    int8_t magic;
    struct stat st;
    fstat(db->fd, &st);
    if (st.st_size == 0) return;
    read(db->fd, &magic, sizeof(magic));
    if (magic != db->header.magic) {
        panic("unknown data file <%s>", db->filename.c_str());
    }
    struct iovec iov[9];
    fill_header(iov);
    lseek(db->fd, 0, SEEK_SET);
    readv(db->fd, iov, 9);
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

static void encode32(std::string& buf, uint32_t n)
{
    buf.append(reinterpret_cast<char*>(&n), 4);
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

static uint32_t decode32(char **ptr)
{
    uint32_t n = *reinterpret_cast<uint32_t*>(*ptr);
    *ptr += 4;
    return n;
}

static off_t decodeoff(char **ptr)
{
    off_t n = *reinterpret_cast<off_t*>(*ptr);
    *ptr += sizeof(n);
    return n;
}

// ############################# node #############################
// [leaf]
// [key-nums]
// all-keys [key -> [key-len] [key]]
// ########################### inner-node ###########################
// all-child-offs
// ########################### leaf-node ###########################
// all-values [value -> [value-len] [value] (over-pages)]
// left-off right-off
//
void node::update(bool dirty)
{
    page_used = limit.type_field + limit.key_nums_field;
    for (auto& key : keys) page_used += limit.key_len_field + key.size();
    if (leaf) {
        for (auto& value : values)  {
            page_used += limit.value_len_field + std::min(limit.over_value, value->size());
        }
        page_used += limit.off_field * 2;
    } else {
        page_used += limit.off_field * childs.size();
    }
    this->dirty = dirty;
}

void translation_table::save_node(off_t off, node *node)
{
    std::string buf;
    buf.reserve(node->page_used);
    encode8(buf, node->leaf);
    encode16(buf, node->keys.size());
    for (auto& key : node->keys) {
        encode8(buf, key.size());
        buf.append(key);
    }
    if (node->leaf) {
        for (auto& value : node->values) {
            save_value(buf, value);
        }
        encodeoff(buf, node->left);
        encodeoff(buf, node->right);
    } else {
        for (auto& child_off : node->childs) {
            encodeoff(buf, child_off);
        }
    }
    lseek(db->fd, off, SEEK_SET);
    // 如果没有写满一页的话，也不会有什么问题，文件空洞是允许的
    write(db->fd, buf.data(), buf.size());
}

void translation_table::save_value(std::string& buf, value_t *value)
{
    uint32_t len = value->size();
    encode32(buf, len);
    if (len <= limit.over_value) {
        buf.append(*value);
        return;
    }
    size_t pos = limit.over_value - limit.off_field;
    auto it = over_page_off.find(value);
    // 我们只需将存储到叶节点本身的部分数据写入磁盘即可
    if (it != over_page_off.end()) {
        encodeoff(buf, it->second);
        buf.append(value->data(), pos);
        return;
    }
    // 如果是新插入的值，那么我们就需要为所有数据分配新页，并全部写入磁盘
    // [value-len][over-page-off][value]
    off_t off = db->page_manager.alloc_page();
    encodeoff(buf, off);
    buf.append(value->data(), pos);
    over_page_off.emplace(value, off);
    // 剩余的数据将被以分页的形式写入多个溢出页中，多个溢出页之间链式相连
    // over-page -> [next-over-page-off][value]
    size_t remain_bytes = len - pos;
    size_t cap_of_page = db->header.page_size - limit.off_field;
    size_t n = remain_bytes / cap_of_page;
    size_t r = remain_bytes % cap_of_page;
    std::vector<size_t> pages(n, cap_of_page);
    if (r > 0) pages.push_back(r);

    for (int i = 0; i < pages.size(); i++) {
        struct iovec iov[2];
        off_t next_off = i == pages.size() - 1 ? 0 : db->page_manager.alloc_page();
        iov[0].iov_base = &next_off;
        iov[0].iov_len = sizeof(next_off);
        iov[1].iov_base = value->data() + pos;
        iov[1].iov_len = pages[i];
        pos += pages[i];
        lseek(db->fd, off, SEEK_SET);
        writev(db->fd, iov, 2);
        off = next_off;
    }
}

node *translation_table::load_node(off_t off)
{
    void *start = mmap(nullptr, db->header.page_size, PROT_READ, MAP_SHARED, db->fd, off);
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
            node->values.emplace_back(load_value(&buf));
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

value_t *translation_table::load_value(char **ptr)
{
    uint32_t len = decode32(ptr);
    if (len <= limit.over_value) {
        value_t *value = new value_t(*ptr, len);
        *ptr += len;
        return value;
    }
    value_t *value = new value_t();
    off_t off = decodeoff(ptr);
    over_page_off.emplace(value, off);
    size_t n = limit.over_value - limit.off_field;
    value->append(*ptr, n);
    *ptr += n;

    size_t remain_bytes = len - n;
    size_t cap_of_page = db->header.page_size - limit.off_field;

    while (true) {
        void *start = mmap(nullptr, db->header.page_size, PROT_READ, MAP_SHARED, db->fd, off);
        if (start == MAP_FAILED) {
            panic("load page failed from off=%lld: %s", off, strerror(errno));
        }
        char *buf = reinterpret_cast<char*>(start);
        off = decodeoff(&buf);
        if (remain_bytes >= cap_of_page) {
            value->append(buf, cap_of_page);
            remain_bytes -= cap_of_page;
        } else {
            value->append(buf, remain_bytes);
        }
        munmap(start, db->header.page_size);
        if (off == 0) break;
    }
    return value;
}

void translation_table::free_value(value_t *value)
{
    if (value->size() > limit.over_value) {
        off_t off = over_page_off[value];
        over_page_off.erase(value);
        while (true) {
            lseek(db->fd, off, SEEK_SET);
            off_t next_off;
            read(db->fd, &next_off, sizeof(off_t));
            if (off > 0) db->page_manager.free_page(off);
            off = next_off;
            if (off == 0) break;
        }
    }
    delete value;
}

void translation_table::free_node(node *node)
{
    off_t off = to_off(node);
    change_list.erase(node);
    cache_list.erase(translation_to_node[off].pos);
    translation_to_off.erase(node);
    translation_to_node.erase(off);
    db->page_manager.free_page(off);
}

void translation_table::release_root(node *root)
{
    off_t off = to_off(root);
    cache_list.erase(translation_to_node[off].pos);
    translation_to_node[off].x.release();
    translation_to_node.erase(off);
    translation_to_off.erase(root);
}
