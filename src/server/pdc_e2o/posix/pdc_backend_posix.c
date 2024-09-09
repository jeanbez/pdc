#include "pdc_backend_posix.h"

perr_t
PDC_Server_posix_write(int fd, void *buf, uint64_t write_size)
{
    // Write 1GB at a time
    uint64_t write_bytes = 0, max_write_size = 1073741824;
    perr_t   ret_value = SUCCEED;
    ssize_t  ret;

    FUNC_ENTER(NULL);

    while (write_size > max_write_size) {
        ret = write(fd, buf, max_write_size);

        if (ret < 0 || ret != (ssize_t)max_write_size) {
            printf("==PDC_SERVER[%d]: in-loop: write %d failed, ret = %ld, max_write_size = %llu\n",
                   pdc_server_rank_g, fd, ret, max_write_size);
            ret_value = FAIL;
            goto done;
        }
        write_bytes += ret;
        buf += max_write_size;
        write_size -= max_write_size;
    }

    ret = write(fd, buf, write_size);
    if (ret < 0 || ret != (ssize_t)write_size) {
        printf("==PDC_SERVER[%d]: write %d failed, not all data written %llu/%llu\n", pdc_server_rank_g, fd,
               write_bytes, write_size);
        ret_value = FAIL;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_posix_read(int fd, void *buf, uint64_t read_size, uint64_t offset)
{
    perr_t ret_value = SUCCEED;

    int ret = pread(fd, buf, read_size, offset);

    if (ret < 0 || ret != (ssize_t)read_size) {
        printf("==PDC_SERVER[%d]: read %d failed, not all data read %llu/%llu\n", pdc_server_rank_g, fd, ret,
               read_size);
        ret_value = FAIL;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}
