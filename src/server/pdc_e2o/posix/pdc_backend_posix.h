#include "pdc_config.h"

#include "pdc_utlist.h"
#include "pdc_public.h"
#include "pdc_interface.h"
#include "pdc_region.h"
#include "pdc_client_server_common.h"

perr_t PDC_Server_posix_write(int fd, void *buf, uint64_t write_size);
perr_t PDC_Server_posix_read(int fd, void *buf, uint64_t read_size, uint64_t offset);