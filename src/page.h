#ifndef _BPLUS_TREE_PAGE_H
#define _BPLUS_TREE_PAGE_H

#include <map>
#include <vector>
#include <unordered_map>
#include <mutex>

#include "common.h"

namespace bpdb {

class DB;
// <page-id, page-off>
typedef std::pair<page_id_t, uint16_t> over_page_id_t;

class page_manager {
public:
    page_manager(DB *db) : db(db) {  }
    page_manager(const page_manager&) = delete;
    page_manager& operator=(const page_manager&) = delete;
    void init();
    page_id_t alloc_page();
    void free_page(page_id_t page_id);
    over_page_id_t write_over_page(const char *data, uint16_t n);
    void free_over_page(page_id_t page_id, uint16_t freep, uint16_t n);
private:
    DB *db;
    void clear();
    // 加快查找header.over_page_list_head
    struct over_page_info {
        page_id_t prev_page_id;
        page_id_t next_page_id;
        uint16_t avail;
        uint16_t free_block_head;
    };
    over_page_id_t write_new_over_page(const char *data, uint16_t n);
    uint16_t search_and_try_write(page_id_t page_id, const char *data, uint16_t n);
    void remove_by_avail(page_id_t page_id, uint16_t avail);
    std::unordered_map<page_id_t, over_page_info> over_page_map;
    std::map<uint16_t, std::vector<page_id_t>> avail_map;
    // 1) 保护相应的内存数据结构
    // 2) 间接保证不会同时修改同一个shared-over-page
    std::mutex latch;
};
}

#endif // _BPLUS_TREE_PAGE_H
