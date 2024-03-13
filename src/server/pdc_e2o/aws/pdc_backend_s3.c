#include "pdc_backend_s3.h"
#include "pdc_e2o_s3.h"

#if defined(PDC_HAS_S3) || defined(PDC_HAS_S3_CHECKPOINT)
int 
PDC_Server_S3_write(char* aws_s3_location, void *buf, uint64_t write_size)
{
    perr_t   ret_value = SUCCEED;
    ssize_t  ret;

    FUNC_ENTER(NULL);

    //char name[1024];
    //sprintf(name, "%u/server%d", obj_id, pdc_server_rank_g);

    // printf("==PDC_SERVER[%d] PDC_Server_S3_write ===> write_size = %lld\n", pdc_server_rank_g, write_size);
    //printf("~~~~~~~~~~~~~~> PDC_Server_S3_write\n");
    //ret = PutObjectBuffer("pdc-test-11022023", name, buf, write_size, NULL);
    //ret = PutObjectBuffer("pdc-test-11022023", name, buf, write_size, NULL);
    ret = PutObjectBuffer(aws_s3_location, buf, write_size, NULL);

    if (ret < 0) {
        printf("==PDC_SERVER[%d]: write %s failed\n", pdc_server_rank_g, aws_s3_location);
        ret_value = FAIL;
        goto done;
    } /*else {
        printf("==PDC_SERVER[%d]: write %s succeeded\n", pdc_server_rank_g, aws_s3_location);
    } */

done:
    FUNC_LEAVE(ret_value);
}
// region_start, region_size, write_size : region_start + region_size
// if start + write_size < region_size (we can just replace)
// if start > region_size (we need to realloc for the non-overlapping buffer)
// else (we need to realloc for part of the overlapping buffer)
int PDC_Server_S3_write_region(char *aws_s3_location, void *buf, uint64_t write_size, uint64_t region_offset, int seek_end)
{
    perr_t   ret_value = SUCCEED;
    ssize_t  ret, current_size;

    FUNC_ENTER(NULL);

    //char name[1024];
    //sprintf(name, "%u/server%d", obj_id, pdc_server_rank_g);

    // if region, read the entire content, merge the updated data, and write it back
    // since updates or partial writes are not allowed in ASWS

    void *original_buf = NULL;
    //printf("==PDC_SERVER[%d] ---> PDC_Server_S3_write_region ---> region_offset = %lld\n", pdc_server_rank_g, region_offset);

    if (seek_end || region_offset > 0) {
        current_size = GetSize(aws_s3_location);

        /*if (seek_end) {
            region_offset = current_size;
        }*/

        // printf("CUREENT_SIZE: %d || region_offset: %d || seek_end = %d\n", current_size, region_offset, seek_end);

        // TODO use upload_and_copy to optimize
        // merge by memcpy'ing
        // printf("==PDC_SERVER[%d] ===> current_size = %lld\n", pdc_server_rank_g, current_size);
        // printf("==PDC_SERVER[%d] ===> region_offset = %lld\n", pdc_server_rank_g, region_offset);
        // printf("==PDC_SERVER[%d] ===> write_size = %lld\n", pdc_server_rank_g, write_size);
        // NOT printf("===> original_buf[region_offset] = %ld buf[0] = %ld write_size = %ld\n", &original_buf[region_offset], &buf[0], write_size);

        //  printf("---> PDC_Server_S3_write_region ---> from %lld to %lld...\n", current_size, region_offset + write_size);
    } else {
        current_size = 0;
    }

    ssize_t new_size = fmax(current_size, region_offset + write_size);

    if (new_size > current_size) {
        // printf("==PDC_SERVER[%d] ---> PDC_Server_S3_write_region ---> realloc'ing from %lld to %lld...\n", pdc_server_rank_g, current_size, region_offset + write_size);
    
        if (current_size != 0) {
            // original_buf = malloc(region_offset + write_size);
        //} else {
            // printf("==PDC_SERVER[%d]    increasing original_buf from %lld to %lld bytes\n", pdc_server_rank_g, current_size, new_size);
            //original_buf = realloc(original_buf, current_size + region_offset + write_size);  
            original_buf = realloc(original_buf, new_size);  
        } /*else {
            original_buf = malloc(current_size);    
        } */
    } else {
        // printf("==PDC_SERVER[%d]   allocating %lld for the original buffer...\n", pdc_server_rank_g, current_size);
        original_buf = malloc(current_size);
    }

    if (current_size) {
        // printf("  getting original buffer...\n");
        ret = GetObjectRange(aws_s3_location, original_buf, 0, current_size);
        //printf("  got original buffer\n");

        if (!ret) {
            ret_value = FAIL;
            goto done;
        }

        memcpy(&original_buf[region_offset], buf, write_size);

        //printf("---> PDC_Server_S3_write_region ---> copied buffer\n");
        ret = PutObjectBuffer(aws_s3_location, original_buf, new_size, NULL);
    } else {
        //printf("==PDC_SERVER[%d] PDC_Server_S3_write_region ===> write_size = %lld\n", pdc_server_rank_g, write_size);
        ret = PutObjectBuffer(aws_s3_location, buf, write_size, NULL);
    }

    free(original_buf);

    if (ret < 0) {
        printf("==PDC_SERVER[%d]: write %s failed\n", pdc_server_rank_g, aws_s3_location);
        ret_value = FAIL;
        goto done;
    } /*else {
        printf("==PDC_SERVER[%d]: write %s succeeded\n", pdc_server_rank_g, aws_s3_location);
    } */

done:
    FUNC_LEAVE(ret_value);
}

int PDC_Server_S3_read(char* aws_s3_location, void *buf, uint64_t size, uint64_t offset)
{
    perr_t   ret_value = SUCCEED;
    ssize_t  ret;

    FUNC_ENTER(NULL);

    //char name[1024];
    //sprintf(name, "%u/server%d", obj_id, pdc_server_rank_g);

    // ssize_t current_size = GetSize(aws_s3_location, "pdc-test-11022023");
    // printf("GetObjectRange (trying) ---> offset = %lld size = %lld of object size = %lld\n", offset, size, current_size);
    //printf("GetObjectRange ---> offset = %lld size = %lld\n", offset, size);

    // printf("~~~~~~~~~~> PDC_Server_S3_read\n");

    ret = GetObjectRange(aws_s3_location, buf, offset, size);

    if (!ret) {
        printf("==PDC_SERVER[%d]: read %s failed\n", pdc_server_rank_g, aws_s3_location);
        ret_value = FAIL;
        goto done;
    } /* else {
        printf("==PDC_SERVER[%d]: read %s succeeded\n", pdc_server_rank_g, aws_s3_location);
    } */

done:
    FUNC_LEAVE(ret_value);
}

int PDC_Server_S3_size(char *aws_s3_location)
{
    return GetSize(aws_s3_location);
}

#endif