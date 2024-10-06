#include "pdc_config.h"

#include "pdc_utlist.h"
#include "pdc_public.h"
#include "pdc_interface.h"
#include "pdc_region.h"
#include "pdc_client_server_common.h"

uint64_t PDC_Server_S3_size(char *aws_s3_location);
int PDC_Server_S3_write(char *aws_s3_location, void *buf, uint64_t write_size);
int PDC_Server_S3_write_region(char *aws_s3_location, void *buf, uint64_t write_size, uint64_t region_offset,
                               int seek_end);
int PDC_Server_S3_read(char *aws_s3_location, void *buf, uint64_t size, uint64_t offset);
int PDC_Server_S3_download(char *aws_s3_location);
