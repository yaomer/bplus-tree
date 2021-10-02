#include <sys/stat.h>
#include <sys/mman.h>

#include "db.h"
#include "codec.h"

using namespace bplus_tree_db;

void translation_table::init()
{
    clear();
    load_header();
}

void translation_table::clear()
{
    translation_to_node.clear();
    translation_to_off.clear();
    cache_list.clear();
    change_list.clear();
    over_page_off.clear();
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
    // 就算什么也没做，我们也强制flush一次根节点
    // 以便重启后可以成功load根节点
    save_node(db->header.root_off, db->root.get());
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
    auto it = translation_to_off.find(node);
    if (it == translation_to_off.end()) {
        panic("to_off(%p)", node);
    }
    return it->second;
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
// [magic][page-size][key-nums][root-off][leaf-off][last-off]
// [free-list-head][free-pages][over-page-list-head][over-pages]
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
    iov[5].iov_base = &db->header.last_off;
    iov[5].iov_len = sizeof(db->header.last_off);
    iov[6].iov_base = &db->header.free_list_head;
    iov[6].iov_len = sizeof(db->header.free_list_head);
    iov[7].iov_base = &db->header.free_pages;
    iov[7].iov_len = sizeof(db->header.free_pages);
    iov[8].iov_base = &db->header.over_page_list_head;
    iov[8].iov_len = sizeof(db->header.over_page_list_head);
    iov[9].iov_base = &db->header.over_pages;
    iov[9].iov_len = sizeof(db->header.over_pages);
}

#define HEADER_IOV_LEN 10

void translation_table::save_header()
{
    struct iovec iov[HEADER_IOV_LEN];
    fill_header(iov);
    lseek(db->fd, 0, SEEK_SET);
    writev(db->fd, iov, HEADER_IOV_LEN);
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
    struct iovec iov[HEADER_IOV_LEN];
    fill_header(iov);
    lseek(db->fd, 0, SEEK_SET);
    readv(db->fd, iov, HEADER_IOV_LEN);
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
        page_used += sizeof(off_t) * 2;
    } else {
        page_used += sizeof(off_t) * childs.size();
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

#define CAP_OF_OVER_PAGE (db->header.page_size - sizeof(off_t))
#define CAP_OF_SHARED_OVER_PAGE (CAP_OF_OVER_PAGE - 8)

#define OVER_VALUE_LEN (limit.over_value - sizeof(off_t) - 2)

void translation_table::save_value(std::string& buf, value_t *value)
{
    uint32_t len = value->size();
    encode32(buf, len);
    if (len <= limit.over_value) {
        buf.append(*value);
        return;
    }
    // over-value:
    // -----------------------------------------------------------------
    // |  4 bytes  |   8 bytes     |  2 bytes   | limit.over_value - 10 |
    // | value-len | over-page-off | remain-off |        value          |
    // -----------------------------------------------------------------
    // over-page: [next-over-page-off][data]
    size_t pos = OVER_VALUE_LEN;
    auto it = over_page_off.find(value);
    // 我们只需将存储到叶节点本身的部分数据写入磁盘即可
    if (it != over_page_off.end()) {
        auto [page_off, remain_off] = it->second;
        encodeoff(buf, page_off);
        encode16(buf, remain_off);
        buf.append(value->data(), pos);
        return;
    }
    // 对于剩下的存储到溢出页的数据，我们将根据页大小进行分块
    len -= pos;
    size_t n = len / CAP_OF_OVER_PAGE;
    uint16_t r = len % CAP_OF_OVER_PAGE;
    std::vector<size_t> pages(n, CAP_OF_OVER_PAGE);
    // 剩下的r恰好无法放到shared-over-page中
    if (r > CAP_OF_SHARED_OVER_PAGE) {
        pages.push_back(r);
        r = 0;
    }

    over_page_off_t over_page;
    if (r > 0) {
        size_t roff = pos + n * CAP_OF_OVER_PAGE;
        over_page = db->page_manager.write_over_page(value->data() + roff, r);
    }

    n = pages.size();
    off_t page_off = n > 0 ? db->page_manager.alloc_page() : over_page.first;

    off_t off = page_off;
    for (int i = 0; i < n; i++) {
        struct iovec iov[2];
        off_t next_off;
        if (i == n - 1) {
            next_off = r > 0 ? over_page.first : 0;
        } else {
            next_off = db->page_manager.alloc_page();
        }
        iov[0].iov_base = &next_off;
        iov[0].iov_len = sizeof(next_off);
        iov[1].iov_base = value->data() + pos;
        iov[1].iov_len = pages[i];
        pos += pages[i];
        lseek(db->fd, off, SEEK_SET);
        writev(db->fd, iov, 2);
        off = next_off;
    }
    encodeoff(buf, page_off);
    encode16(buf, over_page.second);
    buf.append(value->data(), OVER_VALUE_LEN);
    over_page_off[value] = { page_off, over_page.second };
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
    uint16_t remain_off = decode16(ptr);
    over_page_off[value] = { off, remain_off };
    size_t n = OVER_VALUE_LEN;
    value->append(*ptr, n);
    *ptr += n;
    len -= n;

    while (true) {
        void *start = mmap(nullptr, db->header.page_size, PROT_READ, MAP_SHARED, db->fd, off);
        if (start == MAP_FAILED) {
            panic("load page failed from off=%lld: %s", off, strerror(errno));
        }
        char *buf = reinterpret_cast<char*>(start);
        off = decodeoff(&buf);
        if (len >= CAP_OF_OVER_PAGE) {
            value->append(buf, CAP_OF_OVER_PAGE);
            len -= CAP_OF_OVER_PAGE;
        } else {
            if (len <= CAP_OF_SHARED_OVER_PAGE) {
                value->append(buf - sizeof(off) + remain_off, len);
            } else {
                value->append(buf, len);
            }
            off = 0;
        }
        munmap(start, db->header.page_size);
        if (off == 0) break;
    }
    return value;
}

void translation_table::free_value(value_t *value)
{
    size_t len = value->size();
    if (len > limit.over_value) {
        auto [off, remain_off] = over_page_off[value];
        len -= OVER_VALUE_LEN;
        over_page_off.erase(value);
        while (true) {
            off_t next_off;
            lseek(db->fd, off, SEEK_SET);
            read(db->fd, &next_off, sizeof(off_t));
            if (len >= CAP_OF_OVER_PAGE) {
                db->page_manager.free_page(off);
                len -= CAP_OF_OVER_PAGE;
                if (next_off == 0) break;
                off = next_off;
            } else {
                if (len <= CAP_OF_SHARED_OVER_PAGE)
                    db->page_manager.free_over_page(off, remain_off, len);
                else
                    db->page_manager.free_page(off);
                break;
            }
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
    change_list.erase(db->root.get());
    cache_list.erase(translation_to_node[off].pos);
    translation_to_node[off].x.release();
    translation_to_node.erase(off);
    translation_to_off.erase(root);
}
