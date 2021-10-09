#include <sys/stat.h>
#include <sys/mman.h>

#include "db.h"
#include "codec.h"

using namespace bpdb;

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
    if (cache_list.size() >= lru_cap) {
        off_t evict_off = cache_list.back();
        auto *evict_node = translation_to_node[evict_off].x.get();
        if (evict_node->shmtx.try_lock()) {
            if (!evict_node->dirty && !evict_node->maybe_using) {
                translation_to_off.erase(evict_node);
                translation_to_node.erase(evict_off);
                cache_list.pop_back();
            } else {
                evict_node->unlock();
            }
        }
    }
    cache_list.push_front(off);
    translation_to_node.emplace(off, cache_node(node, cache_list.begin()));
    translation_to_off.emplace(node, off);
}

node *translation_table::to_node(off_t off)
{
    if (off == db->header.root_off) return db->root.get();
    wlock_t wlk(shmtx);
    node *node = lru_get(off);
    if (node == nullptr) {
        node = load_node(off);
        lru_put(off, node);
    }
    node->maybe_using = true;
    return node;
}

off_t translation_table::to_off(node *node)
{
    if (node == db->root.get()) return db->header.root_off;
    rlock_t rlock(shmtx);
    auto it = translation_to_off.find(node);
    if (it == translation_to_off.end()) {
        panic("to_off(%p)", node);
    }
    return it->second;
}

void translation_table::put(off_t off, node *node)
{
    wlock_t wlk(shmtx);
    lru_put(off, node);
}

void translation_table::flush()
{
    wlock_t wlk(shmtx);
    for (auto& [node, off] : translation_to_off) {
        if (node->dirty) {
            save_node(off, node);
            node->dirty = false;
        }
        node->maybe_using = false;
    }
    // 就算什么也没做，我们也强制flush一次根节点
    // 以便重启后可以成功load根节点
    save_node(db->header.root_off, db->root.get());
    save_header(&db->header);
    fsync(db->fd);
}

// ########################### file-header ###########################
// [magic][page-size][key-nums][root-off][leaf-off]
// [free-list-head][free-pages][over-page-list-head][over-pages]
void translation_table::fill_header(header_t *header, struct iovec *iov)
{
    iov[0].iov_base = &header->magic;
    iov[0].iov_len = sizeof(header->magic);
    iov[1].iov_base = &header->page_size;
    iov[1].iov_len = sizeof(header->page_size);
    iov[2].iov_base = &header->key_nums;
    iov[2].iov_len = sizeof(header->key_nums);
    iov[3].iov_base = &header->root_off;
    iov[3].iov_len = sizeof(header->root_off);
    iov[4].iov_base = &header->leaf_off;
    iov[4].iov_len = sizeof(header->leaf_off);
    iov[5].iov_base = &header->free_list_head;
    iov[5].iov_len = sizeof(header->free_list_head);
    iov[6].iov_base = &header->free_pages;
    iov[6].iov_len = sizeof(header->free_pages);
    iov[7].iov_base = &header->over_page_list_head;
    iov[7].iov_len = sizeof(header->over_page_list_head);
    iov[8].iov_base = &header->over_pages;
    iov[8].iov_len = sizeof(header->over_pages);
}

#define HEADER_IOV_LEN 9

void translation_table::save_header(header_t *header)
{
    struct iovec iov[HEADER_IOV_LEN];
    fill_header(header, iov);
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
        panic("unknown data file <%s>", db->dbfile.c_str());
    }
    struct iovec iov[HEADER_IOV_LEN];
    fill_header(&db->header, iov);
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
            page_used += limit.value_len_field + std::min(limit.over_value, (size_t)value->reallen);
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
    uint32_t len = value->reallen;
    encode32(buf, len);
    if (len <= limit.over_value) {
        buf.append(*value->val);
        return;
    }
    // over-value:
    // -----------------------------------------------------------------
    // |  4 bytes  |   8 bytes     |  2 bytes   | limit.over_value - 10 |
    // | value-len | over-page-off | remain-off |        value          |
    // -----------------------------------------------------------------
    // over-page: [next-over-page-off][data]
    size_t pos = OVER_VALUE_LEN;
    // 我们只需将存储到叶节点本身的部分数据写入磁盘即可
    if (value->over_page_off > 0) {
        encodeoff(buf, value->over_page_off);
        encode16(buf, value->remain_off);
        buf.append(value->val->data(), pos);
        return;
    }
    // ##初次插入新value的情况
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
        over_page = db->page_manager.write_over_page(value->val->data() + roff, r);
        value->remain_off = over_page.second;
    }

    n = pages.size();
    value->over_page_off = n > 0 ? db->page_manager.alloc_page() : over_page.first;

    off_t off = value->over_page_off;
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
        iov[1].iov_base = value->val->data() + pos;
        iov[1].iov_len = pages[i];
        pos += pages[i];
        lseek(db->fd, off, SEEK_SET);
        writev(db->fd, iov, 2);
        off = next_off;
    }
    encodeoff(buf, value->over_page_off);
    encode16(buf, value->remain_off);
    // 这里的value->val只是指向用户传入进来的value的指针，并不持有它
    // 插入后我们只需保留存储在叶节点本身的前面部分值即可
    value->val = new std::string(value->val->data(), OVER_VALUE_LEN);
    buf.append(*value->val);
}

node *translation_table::load_node(off_t off)
{
    void *start = mmap(nullptr, db->header.page_size, PROT_READ, MAP_SHARED, db->fd, off);
    if (start == MAP_FAILED) {
        panic("load_node: load node failed from off=%lld: %s", off, strerror(errno));
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
    value_t *value = new value_t();
    value->reallen = decode32(ptr);
    if (value->reallen <= limit.over_value) {
        value->val = new std::string(*ptr, value->reallen);
        *ptr += value->reallen;
        return value;
    }
    // 就算value的长度超过了limit.over_value，我们也只加载存储在叶节点
    // 本身的数据，这并不影响修改操作，当真正需要完整的值时，可以通过
    // 调用load_real_value()来获得
    value->over_page_off = decodeoff(ptr);
    value->remain_off = decode16(ptr);
    value->val = new std::string(*ptr, OVER_VALUE_LEN);
    *ptr += OVER_VALUE_LEN;
    return value;
}

// 查找溢出页，取出完整的value
void translation_table::load_real_value(value_t *value, std::string *saved_val)
{
    off_t off = value->over_page_off;
    if (off == 0) {
        assert(value->reallen <= limit.over_value);
        saved_val->assign(*value->val);
        return;
    }
    uint32_t len = value->reallen - OVER_VALUE_LEN;
    assert(value->val->size() == OVER_VALUE_LEN);
    saved_val->assign(*value->val);
    while (true) {
        void *start = mmap(nullptr, db->header.page_size, PROT_READ, MAP_SHARED, db->fd, off);
        if (start == MAP_FAILED) {
            panic("load_real_value: load page failed from off=%lld: %s", off, strerror(errno));
        }
        char *buf = reinterpret_cast<char*>(start);
        off = decodeoff(&buf);
        if (len >= CAP_OF_OVER_PAGE) {
            saved_val->append(buf, CAP_OF_OVER_PAGE);
            len -= CAP_OF_OVER_PAGE;
        } else {
            if (len <= CAP_OF_SHARED_OVER_PAGE) {
                saved_val->append(buf - sizeof(off) + value->remain_off, len);
            } else {
                saved_val->append(buf, len);
            }
            off = 0;
        }
        munmap(start, db->header.page_size);
        if (off == 0) break;
    }
}

void translation_table::free_value(value_t *value)
{
    int fd = db->get_db_fd();
    uint32_t len = value->reallen;
    if (len > limit.over_value) {
        off_t off = value->over_page_off;
        len -= OVER_VALUE_LEN;
        while (true) {
            off_t next_off;
            lseek(fd, off, SEEK_SET);
            read(fd, &next_off, sizeof(off_t));
            if (len >= CAP_OF_OVER_PAGE) {
                db->page_manager.free_page(off);
                len -= CAP_OF_OVER_PAGE;
                if (next_off == 0) break;
                off = next_off;
            } else {
                if (len <= CAP_OF_SHARED_OVER_PAGE)
                    db->page_manager.free_over_page(off, value->remain_off, len);
                else
                    db->page_manager.free_page(off);
                break;
            }
        }
    }
    db->put_db_fd(fd);
    delete value;
}

void translation_table::free_node(node *node)
{
    off_t off = to_off(node);
    wlock_t wlk(shmtx);
    cache_list.erase(translation_to_node[off].pos);
    translation_to_off.erase(node);
    translation_to_node.erase(off);
    db->page_manager.free_page(off);
}

void translation_table::release_root(node *root)
{
    off_t off = to_off(root);
    wlock_t wlk(shmtx);
    cache_list.erase(translation_to_node[off].pos);
    translation_to_node[off].x.release();
    translation_to_node.erase(off);
    translation_to_off.erase(root);
}
