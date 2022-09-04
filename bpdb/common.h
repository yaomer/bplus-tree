#ifndef _BPLUS_TREE_COMMON_H
#define _BPLUS_TREE_COMMON_H

#include <string>
#include <vector>
#include <mutex>
#include <shared_mutex>

#include <limits.h>

namespace bpdb {

typedef std::string key_t;

typedef off_t page_id_t;
typedef uint64_t trx_id_t;

struct value_t {
    ~value_t() { delete val; }
    page_id_t over_page_id = 0;
    uint16_t page_off = 0;
    uint32_t reallen;
    std::string *val;
    trx_id_t trx_id = 0;
};

struct header_t {
    int8_t magic = 0x1a;
    size_t page_size = 1024 * 16;
    size_t key_nums = 0;
    page_id_t root_id = 0;
    page_id_t leaf_id = 0;
    page_id_t free_list_head = page_size;
    size_t free_pages = 0;
    page_id_t over_page_list_head = 0;
    size_t over_pages = 0;
};

struct limit_t {
    // key允许的最大长度，较小的key有助于最大化索引节点的分支因子
    const size_t max_key = UINT8_MAX;
    const size_t max_value = UINT32_MAX;
    const size_t type_field = 1;
    const size_t key_nums_field = 2;
    const size_t key_len_field = 1;
    const size_t value_len_field = 4;
    // 如果一个value的长度超过了over_value，那么超出的部分将被存放到溢出页
    // 由header.page_size决定
    size_t over_value;
};

extern struct limit_t limit;

struct node {
    node(bool leaf) : leaf(leaf)
    {
        page_used = limit.type_field + limit.key_nums_field;
        if (leaf) page_used += sizeof(page_id_t) * 2; // left and right
    }
    ~node()
    {
        if (!leaf) return;
        for (auto it = values.begin(); it != values.end(); ) {
            auto e = it++;
            delete *e;
        }
    }

    void lock_shared() { latch.lock_shared(); }
    void unlock_shared() { latch.unlock_shared(); }
    void lock() { latch.lock(); }
    void unlock() { latch.unlock(); }

    void resize(int n)
    {
        keys.resize(n);
        if (leaf) values.resize(n);
        else childs.resize(n);
    }
    void remove_from(int from)
    {
        keys.erase(keys.begin() + from, keys.end());
        if (leaf) values.erase(values.begin() + from, values.end());
        else childs.erase(childs.begin() + from, childs.end());
        update();
    }
    void remove(int i)
    {
        int n = keys.size();
        for (int j = i + 1; j < n; j++) {
            copy(j - 1, j);
            if (!leaf) childs[j - 1] = childs[j];
        }
        resize(n - 1);
        update();
    }
    void copy(int i, node *x, int j)
    {
        keys[i] = x->keys[j];
        if (leaf) values[i] = x->values[j];
    }
    void copy(int i, int j)
    {
        copy(i, this, j);
    }
    void update(bool dirty = true);

    void free()
    {
        resize(0);
        deleted = true;
    }

    bool leaf;
    bool dirty = false;
    bool maybe_using = false;
    bool deleted = false;
    std::vector<key_t> keys;
    std::vector<page_id_t> childs;
    std::vector<value_t*> values;
    size_t page_used;
    page_id_t left = 0, right = 0;
    // 保护节点本身以及对应的磁盘页
    std::shared_mutex latch;
};

typedef std::lock_guard<std::mutex> lock_t;
typedef std::shared_lock<std::shared_mutex> rlock_t;
typedef std::unique_lock<std::shared_mutex> wlock_t;
typedef std::lock_guard<std::recursive_mutex> recursive_lock_t;

struct status {
public:
    status() {  }
    status(const status& s) : code(s.code), msg(s.msg) {  }
    status& operator=(const status& s)
    {
        code = s.code;
        msg = s.msg;
        return *this;
    }
    status(status&& s) : code(s.code), msg(std::move(s.msg)) {  }
    status& operator=(status&& s)
    {
        code = s.code;
        msg = std::move(s.msg);
        return *this;
    }

    bool is_ok() { return code == Ok; }
    bool is_not_found() { return code == NotFound; }
    bool is_exists() { return code == Exists; }
    const std::string& to_str() { return msg; }
    static status ok() { return status(Ok, "Ok"); }
    static status not_found() { return status(NotFound, "Not Found"); }
    static status exists() { return status(Exists, "Key already exists"); }
    static status error(const char *msg) { return status(Error, msg); }
private:
    enum Code {
        Ok = 1,
        NotFound = 2,
        Exists = 3,
        Error = 4,
    } code;
    status(Code code, const char *msg) : code(code), msg(msg) {  }
    std::string msg;
};

}

#endif // _BPLUS_TREE_COMMON_H
