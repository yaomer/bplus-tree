#include "page.h"
#include "db.h"
#include "codec.h"

#include <sys/mman.h>
#include <sys/stat.h>

using namespace bpdb;

void page_manager::init()
{
    clear();
    // build over_page_map and avail_map
    off_t off = db->header.over_page_list_head;
    over_page_info over_page;
    for (int i = 0; i < db->header.over_pages; i++) {
        struct iovec iov[3];
        lseek(db->fd, off, SEEK_SET);
        iov[0].iov_base = &over_page.next_off;
        iov[0].iov_len = sizeof(over_page.next_off);
        iov[1].iov_base = &over_page.avail;
        iov[1].iov_len = sizeof(over_page.avail);
        iov[2].iov_base = &over_page.free_block_head;
        iov[2].iov_len = sizeof(over_page.free_block_head);
        readv(db->fd, iov, 3);
        over_page_map.emplace(off, over_page);
        avail_map[over_page.avail].push_back(off);
        over_page.prev_off = off;
        off = over_page.next_off;
    }
}

void page_manager::clear()
{
    over_page_map.clear();
    avail_map.clear();
}

// 分配一个新页，有3种用途：
// 1) 分配给一个B+树中的节点
// 2) 分配给一个很大的value存放溢出的值，并且它能占满整页
// 3) 一个value溢出的值不能占满整页，此时我们将管理所有这些未用完的页
off_t page_manager::alloc_page()
{
    recursive_lock_t lk(db->header_mtx);
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

#define ASSERT_OFF(off) (assert(off > 0))

// 所有空闲页会用一个链表串起来，释放一页只是简单的将它插到空闲链表头部
// ----------------------------------------
// |       8 bytes      | xxxxxxxxxxxxxxx |
// | next-free-page-off | xxxxxxxxxxxxxxx |
// ----------------------------------------
void page_manager::free_page(off_t off)
{
    ASSERT_OFF(off);
    recursive_lock_t lk(db->header_mtx);
    lseek(db->fd, off, SEEK_SET);
    write(db->fd, &db->header.free_list_head, sizeof(db->header.free_list_head));
    db->header.free_list_head = off;
    db->header.free_pages++;
}

#define OVER_PAGE_AVAIL_OFF (sizeof(off_t) + 2 + 2)

#define ASSERT_AVAIL(avail) (assert((avail) + OVER_PAGE_AVAIL_OFF <= db->header.page_size))

// 我们至少需要4bytes来存放空闲块的信息
inline uint16_t round4(uint16_t n)
{
    return (ceil(n / 4.0) * 4);
}

// 向某个共享溢出页内写入data[n]，并返回写入的溢出页的首地址和页内写入的偏移位置
// ------------------------------------------------------
// |      8 bytes       |    2 bytes  |     2 bytes     |
// | next-over-page-off |  avail-size | free-block-head |
// ------------------------------------------------------
// 页内的空闲块也以链表的形式串连起来，管理方式类似于基于显式空闲链表的内存分配器实现
// -----------------------------------------
// |      2 bytes        |     2 bytes     |
// | next-free-block-off | free-block-size |
// -----------------------------------------
over_page_off_t page_manager::write_over_page(const char *data, uint16_t n)
{
    ASSERT_AVAIL(n);
    uint16_t round_n = round4(n); // n会被向上取整到4的倍数
    lock_t lk(mtx);
    auto it = avail_map.lower_bound(round_n);
    if (it == avail_map.end()) { // 没有找到剩余可用大小至少为round_n的页
        return write_new_over_page(data, n);
    }
    for ( ; it != avail_map.end(); ++it) {
        for (auto& off : it->second) {
            uint16_t freep = search_and_try_write(off, data, n);
            if (freep > 0) {
                off_t _off = off;
                std::swap(off, it->second.back());
                it->second.pop_back();
                if (it->second.empty()) avail_map.erase(it->first);
                return { _off, freep };
            }
        }
    }
    // 所有页内都没有找到合适的空闲块
    return write_new_over_page(data, n);
}

// 分配一个新页，并写入data[n]
over_page_off_t page_manager::write_new_over_page(const char *data, uint16_t n)
{
    off_t off = alloc_page();
    db->lock_header();
    // 因为如果mmap()映射的区域超出了文件大小，那么我们读写超出的区域就是非法的
    // 所以如有必要，我们需要事先增长文件大小
    struct stat st;
    fstat(db->fd, &st);
    if (off + db->header.page_size > st.st_size)
        ftruncate(db->fd, off + db->header.page_size);
    uint16_t round_n = round4(n);
    db->header.over_pages++;
    lseek(db->fd, off, SEEK_SET);
    over_page_info over_page;
    over_page.prev_off = 0;
    over_page.next_off = db->header.over_page_list_head;
    over_page.avail = db->header.page_size - OVER_PAGE_AVAIL_OFF - round_n;
    over_page.free_block_head = OVER_PAGE_AVAIL_OFF + round_n;
    db->header.over_page_list_head = off;
    db->unlock_header();
    struct iovec iov[7];
    iov[0].iov_base = &over_page.next_off;
    iov[0].iov_len = sizeof(over_page.next_off);
    iov[1].iov_base = &over_page.avail;
    iov[1].iov_len = sizeof(over_page.avail);
    iov[2].iov_base = &over_page.free_block_head;
    iov[2].iov_len = sizeof(over_page.free_block_head);
    iov[3].iov_base = const_cast<char*>(data);
    iov[3].iov_len = n;
    std::string zeros(round_n - n, '\0');
    iov[4].iov_base = const_cast<char*>(zeros.data());
    iov[4].iov_len = zeros.size();
    uint16_t next_free_block_off = 0;
    iov[5].iov_base = &next_free_block_off;
    iov[5].iov_len = sizeof(next_free_block_off);
    iov[6].iov_base = &over_page.avail;
    iov[6].iov_len = sizeof(over_page.avail);
    writev(db->fd, iov, 7);

    if (over_page.next_off > 0) {
        over_page_map[over_page.next_off].prev_off = off;
    }
    over_page_map.emplace(off, over_page);
    avail_map[over_page.avail].push_back(off);
    return { off, OVER_PAGE_AVAIL_OFF };
}

// 在溢出页off内查找是否有长度至少为n的空闲块
// 如果有的话，我们写入data[n]，并返回写入位置的页内偏移；否则就返回0
uint16_t page_manager::search_and_try_write(off_t off, const char *data, uint16_t n)
{
    uint16_t round_n = round4(n);
    auto& over_page = over_page_map[off];
    // 我们这里使用PROT_WRITE以便直接修改溢出页
    void *start = mmap(nullptr, db->header.page_size, PROT_READ | PROT_WRITE, MAP_SHARED, db->fd, off);
    if (start == MAP_FAILED) {
        panic("search_and_try_write: load page failed from off=%lld: %s", off, strerror(errno));
    }
    char *buf = reinterpret_cast<char*>(start);
    uint16_t prev_off = 0;
    uint16_t cur_off = over_page.free_block_head;
    uint16_t avail = over_page.avail;
    while (true) {
        char *ptr = buf + cur_off;
        uint16_t next_off = decode16(&ptr);
        uint16_t cur_size = decode16(&ptr);
        if (cur_size >= round_n) { // 找到了一个合适的块
            // 将data[n]拷贝到cur_off处
            memcpy(buf + cur_off, data, n);
            // 如果还有剩余的，我们就将它插回到原空闲块链表中
            uint16_t remain_size = cur_size - round_n;
            if (remain_size > 0) {
                uint16_t new_off = cur_off + round_n;
                memcpy(buf + new_off, &next_off, sizeof(next_off));
                memcpy(buf + new_off + 2, &remain_size, sizeof(remain_size));
                next_off = new_off;
            }
            // 更新前一个块的next-free-block-off信息
            if (prev_off > 0)
                memcpy(buf + prev_off, &next_off, sizeof(next_off));
            else
                over_page.free_block_head = next_off;
            // 更新该页的剩余可用大小
            over_page.avail -= round_n;
            if (over_page.avail > 0) {
                avail_map[over_page.avail].push_back(off);
            }
            // 更新该页的相关记录信息
            memcpy(buf + sizeof(off_t), &over_page.avail, sizeof(over_page.avail));
            memcpy(buf + sizeof(off_t) + 2, &over_page.free_block_head, sizeof(over_page.free_block_head));
            break;
        }
        avail -= cur_size;
        // 剩下的块中已经找不到合适的了
        if (avail < round_n) {
            cur_off = 0;
            break;
        }
        prev_off = cur_off;
        cur_off = next_off;
    }
    munmap(start, db->header.page_size);
    return cur_off;
}

// 释放溢出页off内偏移为freep处的n个字节
void page_manager::free_over_page(off_t off, uint16_t freep, uint16_t n)
{
    ASSERT_OFF(off);
    lock_t lk(mtx);
    auto& over_page = over_page_map[off];
    ASSERT_AVAIL(over_page.avail + n);
    n = round4(n);
    over_page.avail += n;
    if (over_page.avail == db->header.page_size - OVER_PAGE_AVAIL_OFF) {
        // 如果该页没人使用了，就整个释放掉
        recursive_lock_t lk(db->header_mtx);
        if (over_page.prev_off > 0) {
            lseek(db->fd, over_page.prev_off, SEEK_SET);
            write(db->fd, &over_page.next_off, sizeof(over_page.next_off));
        } else {
            db->header.over_page_list_head = over_page.next_off;
        }
        db->header.over_pages--;
        remove_by_avail(off, over_page.avail);
        over_page_map.erase(off);
        free_page(off);
        return;
    }
    remove_by_avail(off, over_page.avail);
    void *start = mmap(nullptr, db->header.page_size, PROT_READ | PROT_WRITE, MAP_SHARED, db->fd, off);
    if (start == MAP_FAILED) {
        panic("free_over_page: load page failed from off=%lld: %s", off, strerror(errno));
    }
    char *buf = reinterpret_cast<char*>(start);
    uint16_t cur_off = over_page.free_block_head;
    uint16_t prev_off = 0;
    uint16_t prev_size = 0;
    char *ptr = buf + cur_off;
    uint16_t next_off = decode16(&ptr);
    uint16_t cur_size = decode16(&ptr);
    if (cur_off == 0) { // 第一个被释放的块
        uint16_t next_off = 0;
        memcpy(buf + freep, &next_off, sizeof(next_off));
        memcpy(buf + freep + 2, &n, sizeof(n));
        over_page.free_block_head = freep;
        goto end;
    }
    // 找到合适的插入位置，按偏移地址升序插入
    while (cur_off < freep && next_off > 0) {
        prev_off = cur_off;
        prev_size = cur_size;
        cur_off = next_off;
        ptr = buf + cur_off;
        next_off = decode16(&ptr);
        cur_size = decode16(&ptr);
    }
    // 释放并尝试合并相邻块
    if (freep < cur_off) {
        if (freep + n == cur_off) {
            n += cur_size;
            memcpy(buf + freep, &next_off, sizeof(next_off));
            memcpy(buf + freep + 2, &n, sizeof(n));
            if (prev_off + prev_size == freep) {
                prev_size += n;
                memcpy(buf + prev_off, &next_off, sizeof(next_off));
                memcpy(buf + prev_off + 2, &prev_size, sizeof(prev_size));
            }
        } else {
            memcpy(buf + freep, &cur_off, sizeof(cur_off));
            memcpy(buf + freep + 2, &n, sizeof(n));
            if (prev_off + prev_size == freep) {
                prev_size += n;
                memcpy(buf + prev_off + 2, &prev_size, sizeof(prev_size));
            }
        }
        if (prev_off == 0) over_page.free_block_head = freep;
    } else {
        if (cur_off + cur_size == freep) {
            cur_size += n;
            memcpy(buf + cur_off + 2, &cur_size, sizeof(cur_size));
        } else {
            memcpy(buf + cur_off, &freep, sizeof(freep));
            memcpy(buf + freep, &next_off, sizeof(next_off));
            memcpy(buf + freep + 2, &n, sizeof(n));
        }
    }
end:
    memcpy(buf + sizeof(off_t), &over_page.avail, sizeof(over_page.avail));
    memcpy(buf + sizeof(off_t) + 2, &over_page.free_block_head, sizeof(over_page.free_block_head));
    avail_map[over_page.avail].push_back(off);
    munmap(start, db->header.page_size);
}

void page_manager::remove_by_avail(off_t off, uint16_t avail)
{
    auto& off_list = avail_map[avail];
    for (auto it = off_list.begin(); it != off_list.end(); ++it) {
        if (*it == off) {
            off_list.erase(it);
            break;
        }
    }
    if (off_list.empty()) avail_map.erase(avail);
}
