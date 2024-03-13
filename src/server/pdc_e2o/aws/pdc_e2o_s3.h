#include <stdbool.h>

#ifndef E2O_S3_HEADER
#define E2O_S3_HEADER

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    int pdc_server_id;
    bool use_crt;
    bool use_express;
    char region[255];
    char zone[255];
    char key[255];
    char secret[255];
    char bucket[255];
    double throughput_target;
    uint64_t part_size;
    int      max_connections;
} pdc_aws_config;

void PDC_Server_aws_init(pdc_aws_config config);
void PDC_Server_aws_finalize();

bool ListBuckets();
bool CreateBucket();
bool DeleteBucket();

bool ListObjects();
bool PutObject(char *objectName, char *fileName);
bool PutObjectBuffer(char *objectName, void *content, uint64_t size, void *metadata);
bool GetObject(char* objectKey, void *buffer);
uint64_t GetObjectBytes(char* objectKey, void *buffer);
uint64_t GetSize(char* objectKey);
bool GetObjectRange(char* objectKey, void *buffer, uint64_t offset, uint64_t size);
bool DeleteObject(char* objectKey);

#ifdef __cplusplus
} // extern "C"

std::shared_ptr<Aws::S3::S3Client>       aws_client;
std::shared_ptr<Aws::S3Crt::S3CrtClient> aws_crt_client;

pdc_aws_config aws_s3_config;

Aws::SDKOptions options;
#endif

#endif /* E2O_S3_HEADER */