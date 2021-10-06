#ifndef _BPLUS_TREE_CODEC_H
#define _BPLUS_TREE_CODEC_H

#include <string>

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

inline void encodeoff(std::string& buf, off_t off)
{
    buf.append(reinterpret_cast<char*>(&off), sizeof(off));
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

inline off_t decodeoff(char **ptr)
{
    off_t n = *reinterpret_cast<off_t*>(*ptr);
    *ptr += sizeof(n);
    return n;
}

}

#endif // _BPLUS_TREE_CODEC_H
