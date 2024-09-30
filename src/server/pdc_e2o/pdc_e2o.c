#include "include/pdc_e2o.h"
#include "aws/pdc_backend_s3.h"
#include "posix/pdc_backend_posix.h"

int
PDC_Server_write(int backend, int fd, char *location, void *buf, uint64_t size, uint64_t offset, int seek)
{
    perr_t ret_value = SUCCEED;

#if defined(PDC_HAS_S3) || defined(PDC_HAS_S3_CHECKPOINT)
    if (backend == PDC_BACKEND_S3 ||
        (backend == PDC_BACKEND_DEFAULT && default_backend_g == PDC_BACKEND_S3)) {
        // TODO: review if we still need the write option back
        // printf("----> S3 write backend (seek = %d)\n", seek);
        ret_value = PDC_Server_S3_write_region(location, buf, size, offset, seek);
    }
    else {
#endif
        // printf("----> POSIX write backend (fd = %d, seek = %d, offset = %d) A\n", fd, seek, offset);
        offset = lseek(fd, offset, seek);
        // printf("----> POSIX write backend (fd = %d, seek = %d, offset = %d) D\n", fd, seek, offset);
        ret_value = PDC_Server_posix_write(fd, buf, size);
#if defined(PDC_HAS_S3) || defined(PDC_HAS_S3_CHECKPOINT)
    }
#endif

    return ret_value;
}

int
PDC_Server_read(int backend, int fd, char *location, void *buf, uint64_t size, uint64_t offset)
{
    perr_t ret_value = SUCCEED;

#if defined(PDC_HAS_S3) || defined(PDC_HAS_S3_CHECKPOINT)
    if (backend == PDC_BACKEND_S3 ||
        (backend == PDC_BACKEND_DEFAULT && default_backend_g == PDC_BACKEND_S3)) {
        // printf("----> S3 read backend\n");
        ret_value = PDC_Server_S3_read(location, buf, size, offset);
        // printf(">>>>> ret_value = %d\n", ret_value);
        if (ret_value == FAIL) {
            // printf("... fallback to local storage ...\n");

            ret_value = PDC_Server_posix_read(fd, buf, size, offset);
        }
    }
    else {
#endif
        // printf("----> POSIX read backend (fd = %d, size = %d, offset = %d) D\n", fd, size, offset);
        ret_value = PDC_Server_posix_read(fd, buf, size, offset);
#if defined(PDC_HAS_S3) || defined(PDC_HAS_S3_CHECKPOINT)
    }
#endif

    return ret_value;
}

int
PDC_Server_size(int backend, int fd, char *location)
{
#if defined(PDC_HAS_S3) || defined(PDC_HAS_S3_CHECKPOINT)
    if (backend == PDC_BACKEND_S3 ||
        (backend == PDC_BACKEND_DEFAULT && default_backend_g == PDC_BACKEND_S3)) {
        return PDC_Server_S3_size(location);
    }
#endif
    return lseek(fd, 0, SEEK_END);
}

void
PDC_Server_download(char *location)
{
#if defined(PDC_HAS_S3_CHECKPOINT)
    PDC_Server_S3_download(location);
#endif
}