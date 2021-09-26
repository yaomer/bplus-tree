#include "page.h"
#include "db.h"

using namespace bplus_tree_db;

// 分配一个新页
off_t page_manager::alloc_page()
{
    off_t off = db->header.free_list_head;
    if (db->header.free_pages > 0) {
        db->header.free_pages--;
        lseek(db->fd, off, SEEK_SET);
        read(db->fd, &db->header.free_list_head, sizeof(db->header.free_list_head));
    } else {
        db->header.free_list_head += db->header.page_size;
    }
    return off;
}

void page_manager::free_page(off_t off)
{
    lseek(db->fd, off, SEEK_SET);
    write(db->fd, &db->header.free_list_head, sizeof(db->header.free_list_head));
    db->header.free_list_head = off;
    db->header.free_pages++;
}
