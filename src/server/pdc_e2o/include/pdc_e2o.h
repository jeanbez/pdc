#include "pdc_config.h"

#include "pdc_utlist.h"
#include "pdc_public.h"
#include "pdc_interface.h"
#include "pdc_region.h"
#include "pdc_server.h"
#include "pdc_client_server_common.h"

int  PDC_Server_size(int backend, int fd, char *location);
int  PDC_Server_write(int backend, int fd, char *location, void *buf, uint64_t size, uint64_t offset,
                      int seek);
int  PDC_Server_read(int backend, int fd, char *location, void *buf, uint64_t size, uint64_t offset);
void PDC_Server_download(char *location);