#include <sys/uio.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "db.h"
#include "codec.h"

using namespace bplus_tree_db;

void redo_log::init()
{
    log_file = db->dbname + "redo.log";
    check_point_file = db->dbname + "checkpoint";
    if ((log_fd = open(log_file.c_str(), O_RDWR | O_APPEND)) < 0) {
        if (errno != ENOENT) {
            panic("redo_log: open(%s): %s", log_file.c_str(), strerror(errno));
        }
    } else {
        try_recovery();
    }
    log_fd = open(log_file.c_str(), O_RDWR | O_APPEND | O_CREAT, 0666);
    check_point_fd = open(check_point_file.c_str(), O_RDWR | O_TRUNC | O_CREAT, 0666);
}

void redo_log::try_recovery()
{
    off_t checkpoint;
    int cp_fd = open(check_point_file.c_str(), O_RDONLY);
    if (cp_fd < 0) checkpoint = 0;
    else read(cp_fd, &checkpoint, sizeof(checkpoint));
    cur_lsn = next_lsn = checkpoint;
    replay(checkpoint);
    check_point(SHARP_CHECK_POINT);
    unlink(log_file.c_str());
    unlink(check_point_file.c_str());
    close(log_fd);
    close(check_point_fd);
    cur_lsn = next_lsn = 0;
}

void redo_log::append(char type, const std::string *key, const std::string *value)
{
    uint8_t keylen;
    uint32_t valuelen;
    struct iovec iov[5];
    iov[0].iov_base = &type;
    iov[0].iov_len = sizeof(type);
    switch (type) {
    case LOG_TYPE_INSERT:
        keylen = key->size();
        iov[1].iov_base = &keylen;
        iov[1].iov_len = sizeof(keylen);
        iov[2].iov_base = const_cast<char*>(key->data());
        iov[2].iov_len = keylen;
        valuelen = value->size();
        iov[3].iov_base = &valuelen;
        iov[3].iov_len = sizeof(valuelen);
        iov[4].iov_base = const_cast<char*>(value->data());
        iov[4].iov_len = valuelen;
        if (!recovery) {
            writev(log_fd, iov, 5);
            fsync(log_fd);
        }
        cur_lsn = next_lsn;
        next_lsn += sizeof(type) + sizeof(keylen) + keylen + sizeof(valuelen) + valuelen;
        break;
    case LOG_TYPE_ERASE:
        keylen = key->size();
        iov[1].iov_base = &keylen;
        iov[1].iov_len = sizeof(keylen);
        iov[2].iov_base = const_cast<char*>(key->data());
        iov[2].iov_len = keylen;
        if (!recovery) {
            writev(log_fd, iov, 3);
            fsync(log_fd);
        }
        cur_lsn = next_lsn;
        next_lsn += sizeof(type) + sizeof(keylen) + keylen;
        break;
    }
}

void redo_log::replay(off_t checkpoint)
{
    recovery = true;
    int pagesize = getpagesize();
    int n = checkpoint / pagesize;
    int off = checkpoint % pagesize;
    struct stat st;
    fstat(log_fd, &st);
    if (st.st_size == 0) return;
    off_t map_start = n * pagesize;
    size_t map_size = st.st_size - map_start;
    void *start = mmap(nullptr, map_size, PROT_READ, MAP_SHARED, log_fd, map_start);
    char *ptr = reinterpret_cast<char*>(start) + off;
    char *end = ptr + map_size - off;
    std::string key, value;
    while (ptr < end) {
        char type = *ptr++;
        uint8_t keylen = decode8(&ptr);
        key.assign(ptr, keylen);
        ptr += keylen;
        if (type == LOG_TYPE_INSERT) {
            uint32_t valuelen = decode32(&ptr);
            value.assign(ptr, valuelen);
            ptr += valuelen;
            db->insert(key, value);
        } else if (type == LOG_TYPE_ERASE) {
            db->erase(key);
        } else {
            panic("replay: unknown type");
        }
    }
    munmap(start, map_size);
    recovery = false;
}

void redo_log::put_change_node(node *node)
{
    remove_node(node);
    node->lsn = cur_lsn;
    flush_list[cur_lsn].nodelist.emplace_back(node);
}

void redo_log::put_complete()
{
    flush_list[cur_lsn].header = db->header;
}

void redo_log::remove_node(node *node)
{
    auto it = flush_list.find(node->lsn);
    if (it == flush_list.end()) return;
    auto& nodelist = it->second.nodelist;
    for (auto& x : nodelist) {
        if (x == node) {
            std::swap(x, nodelist.back());
            nodelist.pop_back();
            if (nodelist.empty()) flush_list.erase(node->lsn);
            break;
        }
    }
}

void redo_log::check_point(char type)
{
    if (type == FUZZY_CHECK_POINT) {
        fuzzy_check_point();
    } else if (type == SHARP_CHECK_POINT) {
        sharp_check_point();
    }
}

void redo_log::write_check_point(off_t checkpoint)
{
    lseek(check_point_fd, 0, SEEK_SET);
    write(check_point_fd, &checkpoint, sizeof(checkpoint));
    fsync(check_point_fd);
}

void redo_log::fuzzy_check_point()
{
    header_t header;
    int flush_pages = 0;
    for (auto it = flush_list.begin(); it != flush_list.end(); ) {
        auto e = it++;
        off_t lsn = e->first;
        auto& nodelist = e->second.nodelist;
        for (auto node : nodelist) {
            flush_node(node);
        }
        flush_pages += nodelist.size();
        header = e->second.header;
        flush_list.erase(lsn);
        if (flush_pages >= FUZZY_CHECK_POINT_PAGES) {
            write_check_point(lsn);
            break;
        }
    }
    if (flush_pages > 0) {
        if (flush_pages < FUZZY_CHECK_POINT_PAGES) {
            write_check_point(next_lsn);
            db->translation_table.save_header(&db->header);
        } else {
            db->translation_table.save_header(&header);
        }
        fsync(db->fd);
    }
}

void redo_log::sharp_check_point()
{
    for (auto& it : flush_list) {
        for (auto node : it.second.nodelist) {
            flush_node(node);
        }
    }
    flush_list.clear();
    // 就算什么也没做，我们也强制flush一次根节点
    // 以便重启后可以成功load根节点
    db->translation_table.save_node(db->header.root_off, db->root.get());
    db->translation_table.save_header(&db->header);
    fsync(db->fd);
}

void redo_log::flush_node(node *node)
{
    if (!node->dirty) return;
    off_t off = db->translation_table.to_off(node);
    db->translation_table.save_node(off, node);
    node->dirty = false;
}
