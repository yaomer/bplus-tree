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
    translation_to_page.clear();
    cache_list.clear();
}

node *translation_table::lru_get(page_id_t page_id)
{
    wlock_t wlk(table_latch);
    auto it = translation_to_node.find(page_id);
    if (it == translation_to_node.end()) return nullptr;
    if (it->second.pos != cache_list.begin()) {
        cache_list.erase(it->second.pos);
        cache_list.push_front(it->first);
        it->second.pos = cache_list.begin();
    }
    return it->second.x.get();
}

void translation_table::lru_put(page_id_t page_id, node *node)
{
    wlock_t wlk(table_latch);
    if (translation_to_node.count(page_id)) return;
    if (cache_list.size() >= lru_cap) {
        page_id_t evict_page_id = cache_list.back();
        auto *evict_node = translation_to_node[evict_page_id].x.get();
        if (evict_node->latch.try_lock()) {
            if (!evict_node->deleted && !evict_node->dirty && !evict_node->maybe_using) {
                translation_to_page.erase(evict_node);
                translation_to_node.erase(evict_page_id);
                cache_list.pop_back();
            } else {
                evict_node->unlock();
            }
        }
    }
    cache_list.push_front(page_id);
    translation_to_node.emplace(page_id, cache_node(node, cache_list.begin()));
    translation_to_page.emplace(node, page_id);
}

node *translation_table::to_node(page_id_t page_id)
{
    if (page_id == db->header.root_id) return db->root.get();
    node *node = lru_get(page_id);
    if (node == nullptr) {
        node = load_node(page_id);
        lru_put(page_id, node);
    }
    node->maybe_using = true;
    return node;
}

page_id_t translation_table::to_page_id(node *node)
{
    if (node == db->root.get()) return db->header.root_id;
    rlock_t rlock(table_latch);
    auto it = translation_to_page.find(node);
    if (it == translation_to_page.end()) {
        panic("to_page_id(%p)", node);
    }
    return it->second;
}

void translation_table::flush()
{
    std::vector<node*> del_nodes;
    {
        rlock_t rlk(table_latch);
        for (auto& [node, page_id] : translation_to_page) {
            if (node->deleted) {
                del_nodes.push_back(node);
                continue;
            }
            if (node->dirty) {
                save_node(page_id, node);
                node->dirty = false;
            }
            node->maybe_using = false;
        }
    }
    for (auto node : del_nodes) {
        free_node(translation_to_page[node], node);
    }
    // 就算什么也没做，我们也强制flush一次根节点
    // 以便重启后可以成功load根节点
    save_node(db->header.root_id, db->root.get());
    save_header(&db->header);
    fsync(db->fd);
}

// ########################### file-header ###########################
// [magic][page-size][key-nums][root-id][leaf-id]
// [free-list-head][free-pages][over-page-list-head][over-pages]
void translation_table::fill_header(header_t *header, struct iovec *iov)
{
    iov[0].iov_base = &header->magic;
    iov[0].iov_len = sizeof(header->magic);
    iov[1].iov_base = &header->page_size;
    iov[1].iov_len = sizeof(header->page_size);
    iov[2].iov_base = &header->key_nums;
    iov[2].iov_len = sizeof(header->key_nums);
    iov[3].iov_base = &header->root_id;
    iov[3].iov_len = sizeof(header->root_id);
    iov[4].iov_base = &header->leaf_id;
    iov[4].iov_len = sizeof(header->leaf_id);
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

void node::update(bool dirty)
{
    page_used = limit.type_field + limit.key_nums_field;
    for (auto& key : keys) page_used += limit.key_len_field + key.size();
    if (leaf) {
        for (auto& value : values)  {
            page_used += limit.value_len_field + sizeof(trx_id_t) + std::min(limit.over_value, (size_t)value->reallen);
        }
        page_used += sizeof(page_id_t) * 2;
    } else {
        page_used += sizeof(page_id_t) * childs.size();
    }
    this->dirty = dirty;
}

void translation_table::save_node(page_id_t page_id, node *node)
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
        encode_page_id(buf, node->left);
        encode_page_id(buf, node->right);
    } else {
        for (auto& child_page_id : node->childs) {
            encode_page_id(buf, child_page_id);
        }
    }
    lseek(db->fd, page_id, SEEK_SET);
    // 如果没有写满一页的话，也不会有什么问题，文件空洞是允许的
    write(db->fd, buf.data(), buf.size());
}

#define CAP_OF_OVER_PAGE (db->header.page_size - sizeof(page_id_t))
#define CAP_OF_SHARED_OVER_PAGE (CAP_OF_OVER_PAGE - 8)

#define OVER_VALUE_LEN (limit.over_value - sizeof(page_id_t) - 2)

// if value->reallen <= limit.over_value
// +----------------------------------------+
// |  4 bytes  | 8 bytes | limit.over_value |
// | value-len | trx-id  |       value      |
// +----------------------------------------+
// else:
// +-----------------------------------------------------------------------+
// |  4 bytes  | 8 bytes |   8 bytes    | 2 bytes  | limit.over_value - 10 |
// | value-len | trx-id  | over-page-id | page-off |        value          |
// +-----------------------------------------------------------------------+
// over-page: [next-over-page-id][data]
void translation_table::save_value(std::string& buf, value_t *value)
{
    uint32_t len = value->reallen;
    encode32(buf, len);
    encode64(buf, value->trx_id);
    if (len <= limit.over_value) {
        buf.append(*value->val);
        return;
    }
    // 我们只需将存储到叶节点本身的部分数据写入磁盘即可
    if (value->over_page_id > 0) {
        encode_page_id(buf, value->over_page_id);
        encode16(buf, value->page_off);
        assert(value->val->size() == OVER_VALUE_LEN);
        buf.append(*value->val);
        return;
    }
    size_t pos = OVER_VALUE_LEN;
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

    over_page_id_t over_page;
    if (r > 0) {
        size_t roff = pos + n * CAP_OF_OVER_PAGE;
        over_page = db->page_manager.write_over_page(value->val->data() + roff, r);
        value->page_off = over_page.second;
    }

    n = pages.size();
    value->over_page_id = n > 0 ? db->page_manager.alloc_page() : over_page.first;

    page_id_t page_id = value->over_page_id;
    for (int i = 0; i < n; i++) {
        struct iovec iov[2];
        page_id_t next_page_id;
        if (i == n - 1) {
            next_page_id = r > 0 ? over_page.first : 0;
        } else {
            next_page_id = db->page_manager.alloc_page();
        }
        iov[0].iov_base = &next_page_id;
        iov[0].iov_len = sizeof(next_page_id);
        iov[1].iov_base = value->val->data() + pos;
        iov[1].iov_len = pages[i];
        pos += pages[i];
        lseek(db->fd, page_id, SEEK_SET);
        writev(db->fd, iov, 2);
        page_id = next_page_id;
    }
    encode_page_id(buf, value->over_page_id);
    encode16(buf, value->page_off);
    // 移除保存在溢出页中的数据
    value->val->erase(OVER_VALUE_LEN);
    buf.append(*value->val);
}

node *translation_table::load_node(page_id_t page_id)
{
    void *start = mmap(nullptr, db->header.page_size, PROT_READ, MAP_SHARED, db->fd, page_id);
    if (start == MAP_FAILED) {
        panic("load_node: load node failed from page_id=%lld: %s", page_id, strerror(errno));
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
        node->left = decode_page_id(&buf);
        node->right = decode_page_id(&buf);
    } else {
        node->childs.reserve(keynums);
        for (int i = 0; i < keynums; i++) {
            node->childs.emplace_back(decode_page_id(&buf));
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
    value->trx_id = decode64(ptr);
    if (value->reallen <= limit.over_value) {
        value->val = new std::string(*ptr, value->reallen);
        *ptr += value->reallen;
        return value;
    }
    // 就算value的长度超过了limit.over_value，我们也只加载存储在叶节点
    // 本身的数据，这并不影响修改操作，当真正需要完整的值时，可以通过
    // 调用load_real_value()来获得
    value->over_page_id = decode_page_id(ptr);
    value->page_off = decode16(ptr);
    value->val = new std::string(*ptr, OVER_VALUE_LEN);
    *ptr += OVER_VALUE_LEN;
    return value;
}

// 查找溢出页，取出完整的value
void translation_table::load_real_value(value_t *value, std::string *saved_val)
{
    page_id_t page_id = value->over_page_id;
    if (page_id == 0) {
        // 对于还未落盘的数据，value->reallen可能大于limit.over_value
        saved_val->assign(*value->val);
        return;
    }
    uint32_t len = value->reallen - OVER_VALUE_LEN;
    assert(value->val->size() == OVER_VALUE_LEN);
    saved_val->assign(*value->val);
    while (true) {
        void *start = mmap(nullptr, db->header.page_size, PROT_READ, MAP_SHARED, db->fd, page_id);
        if (start == MAP_FAILED) {
            panic("load_real_value: load page failed from page_id=%lld: %s", page_id, strerror(errno));
        }
        char *buf = reinterpret_cast<char*>(start);
        page_id = decode_page_id(&buf);
        if (len >= CAP_OF_OVER_PAGE) {
            saved_val->append(buf, CAP_OF_OVER_PAGE);
            len -= CAP_OF_OVER_PAGE;
        } else {
            if (len <= CAP_OF_SHARED_OVER_PAGE) {
                saved_val->append(buf - sizeof(page_id) + value->page_off, len);
            } else {
                saved_val->append(buf, len);
            }
            page_id = 0;
        }
        munmap(start, db->header.page_size);
        if (page_id == 0) break;
    }
}

void translation_table::free_value(value_t *value)
{
    int fd = db->get_db_fd();
    uint32_t len = value->reallen;
    // 必须是已落盘的数据
    if (value->over_page_id > 0 && len > limit.over_value) {
        page_id_t page_id = value->over_page_id;
        len -= OVER_VALUE_LEN;
        while (true) {
            page_id_t next_page_id;
            lseek(fd, page_id, SEEK_SET);
            read(fd, &next_page_id, sizeof(page_id_t));
            if (len >= CAP_OF_OVER_PAGE) {
                db->page_manager.free_page(page_id);
                len -= CAP_OF_OVER_PAGE;
                if (next_page_id == 0) break;
                page_id = next_page_id;
            } else {
                if (len <= CAP_OF_SHARED_OVER_PAGE)
                    db->page_manager.free_over_page(page_id, value->page_off, len);
                else
                    db->page_manager.free_page(page_id);
                break;
            }
        }
    }
    db->put_db_fd(fd);
    delete value;
}

void translation_table::free_node(page_id_t page_id, node *node)
{
    cache_list.erase(translation_to_node[page_id].pos);
    translation_to_page.erase(node);
    translation_to_node.erase(page_id);
    db->page_manager.free_page(page_id);
}

void translation_table::release_root(node *root)
{
    page_id_t page_id = to_page_id(root);
    wlock_t wlk(table_latch);
    cache_list.erase(translation_to_node[page_id].pos);
    translation_to_node[page_id].x.release();
    translation_to_node.erase(page_id);
    translation_to_page.erase(root);
}
