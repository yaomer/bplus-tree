#ifndef _BPLUS_TREE_PAGE_H
#define _BPLUS_TREE_PAGE_H

#include <map>
#include <vector>
#include <unordered_map>

namespace bplus_tree_db {

class DB;

typedef std::pair<off_t, uint16_t> over_page_off_t;

class page_manager {
public:
    page_manager(DB *db) : db(db) {  }
    void init();
    off_t alloc_page();
    void free_page(off_t off);
    over_page_off_t write_over_page(const char *data, uint16_t n);
    void free_over_page(off_t off, uint16_t freep, uint16_t n);
private:
    DB *db;
    // 加快查找header.over_page_list_head
    struct over_page_info {
        off_t prev_off, next_off;
        uint16_t avail;
        uint16_t free_block_head;
    };
    over_page_off_t write_new_over_page(const char *data, uint16_t n);
    uint16_t search_and_try_write(off_t off, const char *data, uint16_t n);
    void remove_by_avail(off_t off, uint16_t avail);
    std::unordered_map<off_t, over_page_info> over_page_map;
    std::map<uint16_t, std::vector<off_t>> avail_map;
};
}

#endif // _BPLUS_TREE_PAGE_H
