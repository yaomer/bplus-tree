#ifndef _BPLUS_TREE_CODEC_H
#define _BPLUS_TREE_CODEC_H

#include <string>

#include "common.h"

namespace bpdb {

inline void encode8(std::string& buf, uint8_t n)
{
    buf.append(reinterpret_cast<char*>(&n), 1);
}

inline void encode16(std::string& buf, uint16_t n)
{
    buf.append(reinterpret_cast<char*>(&n), 2);
}

inline void encode32(std::string& buf, uint32_t n)
{
    buf.append(reinterpret_cast<char*>(&n), 4);
}

inline void encode64(std::string& buf, uint64_t n)
{
    buf.append(reinterpret_cast<char*>(&n), 8);
}

static_assert(sizeof(off_t) == 8, "");

inline void encode_page_id(std::string& buf, page_id_t page_id)
{
    encode64(buf, page_id);
}

inline uint8_t decode8(char **ptr)
{
    uint8_t n = *reinterpret_cast<uint8_t*>(*ptr);
    *ptr += 1;
    return n;
}

inline uint16_t decode16(char **ptr)
{
    uint16_t n = *reinterpret_cast<uint16_t*>(*ptr);
    *ptr += 2;
    return n;
}

inline uint32_t decode32(char **ptr)
{
    uint32_t n = *reinterpret_cast<uint32_t*>(*ptr);
    *ptr += 4;
    return n;
}

inline uint64_t decode64(char **ptr)
{
    uint64_t n = *reinterpret_cast<uint64_t*>(*ptr);
    *ptr += 8;
    return n;
}

inline page_id_t decode_page_id(char **ptr)
{
    return decode64(ptr);
}

}

#endif // _BPLUS_TREE_CODEC_H
