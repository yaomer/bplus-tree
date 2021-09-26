#ifndef _BPLUS_TREE_PAGE_H
#define _BPLUS_TREE_PAGE_H

#include <sys/types.h>

namespace bplus_tree_db {

class DB;

class page_manager {
public:
    page_manager(DB *db) : db(db) {  }
    off_t alloc_page();
    void free_page(off_t off);
private:
    DB *db;
};
}

#endif // _BPLUS_TREE_PAGE_H
