#include <stdbool.h>

#ifndef LIB_H_HEADER
#define LIB_H_HEADER

#ifdef __cplusplus
extern "C" 
{
#endif

typedef struct {
    bool use_crt;
    char region[255];
    char key[255];
    char secret[255];
    char bucket[255];
    char endpoint[255];
    double throughput_target;
    uint64_t part_size;
    int max_connections;
} pdc_aws_config;

void PDC_Server_aws_init(pdc_aws_config config);
void PDC_Server_aws_finalize();

bool ListBuckets();
bool CreateBucket(char *bucketName);
bool DeleteBucket(char *bucketName);

bool ListObjects(char *bucketName);
bool PutObject(char *bucketName, char *objectName, char *fileName);
bool PutObjectBuffer(char *bucketName, char *objectName, void *content, uint64_t size, void *metadata);
//bool PutObjectBufferAsync(char *bucketName, char *objectName, void *content, uint64_t size, void *metadata);
bool GetObject(char* objectKey, char* fromBucket, void *buffer);
uint64_t GetObjectBytes(char* objectKey, char* fromBucket, void *buffer);
uint64_t GetSize(char* objectKey, char* fromBucket);
bool GetObjectRange(char* objectKey, char* fromBucket, void *buffer, uint64_t offset, uint64_t size);
bool DeleteObject(char* objectKey, char* fromBucket);

#ifdef __cplusplus
} // extern "C"

std::shared_ptr<Aws::S3::S3Client> aws_client;
std::shared_ptr<Aws::S3Crt::S3CrtClient> aws_crt_client;

pdc_aws_config aws_s3_config;

Aws::SDKOptions options;
#endif

#endif /* LIB_H_HEADER */