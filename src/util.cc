#include <unistd.h>
#include <fcntl.h>

#include "config.h"

namespace bpdb {

int sync_fd(int fd)
{
#if defined (HAVE_FULLFSYNC)
    // 在Mac OS上，直接使用fsync()是不安全的(See man 2 fsync)
    return ::fcntl(fd, F_FULLFSYNC);
#endif
#if defined (HAVE_FDATASYNC)
    // 在不更改文件尺寸的情况下，
    // fdatasync()相比fsync()会少一次更新元数据的开销
    return ::fdatasync(fd);
#else
    // 最坏情况下，我们回退到fsync()
    return ::fsync(fd);
#endif
}
}
