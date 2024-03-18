#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "pdc.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

double diff(struct timespec start_time, struct timespec end_time) {
    return ((end_time.tv_nsec - start_time.tv_nsec) + ((end_time.tv_sec - start_time.tv_sec)*1000000000L)) / 1000000000.0;
}

long long int nano(struct timespec time) {
    return ((time.tv_nsec) + ((time.tv_sec)*1000000000L));
}

int main(int argc, char **argv)
{
    perr_t ret;

    int i, rank = 0, size = 1;
    int ret_value = 0;

    double elapsed;

    size_t ndim = 3;
    uint64_t dims[3];

    dims[0] = 64;
    dims[1] = 3;
    dims[2] = 4;

    struct timespec start_time, end_time;

    FILE *file;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    char filename[512];
    sprintf(filename, "%s/%s-metrics-%d.csv", argv[1], argv[2], rank);

    file = fopen(filename, "w");

    fprintf(file, "rank;function;start;end;elapsed\n");

    pdcid_t pdc, cont_prop, cont, obj_prop, obj;
    pdcid_t cont_scale, obj_scale;

    struct pdc_cont_info *cont_info;
    struct pdc_obj_info *obj_info;

    char cont_name[128], obj_name[128], tag_value[128], *tag_value_ret;
    char cont_name_scale[128], obj_name_scale[128];

    pdcid_t obj_scale_ids[1000];
    pdcid_t cont_scale_ids[1000];

    strcpy(tag_value, "ACCEPTED");

    pdc_var_type_t value_type;
    psize_t value_size;

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // create a pdc
    pdc = PDCinit("pdc");

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "init", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (cont_prop <= 0) {
        printf("fail to create container property @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "container-property-create", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    sprintf(cont_name, "container-%d", rank);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // create a container
    cont = PDCcont_create(cont_name, cont_prop);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (cont <= 0) {
        printf("fail to create container @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "container-create", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    for (i = 0; i < 1000; i++) {
        sprintf(cont_name_scale, "container-%d-%d", rank, i);

        // create a container
        cont_scale_ids[i] = PDCcont_create(cont_name_scale, cont_prop);

        if (cont_scale_ids[i] <= 0) {
            printf("fail to create container @ line %d\n", __LINE__);
            return EXIT_FAILURE;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "container-create-scale", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (obj_prop <= 0) {
        printf("fail to create object property @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "object-property-create", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    ret = PDCprop_set_obj_dims(obj_prop, ndim, dims);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (ret != SUCCEED) {
        printf("fail to set obj time step @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "object-property-set-dimensions", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    ret = PDCprop_set_obj_type(obj_prop, PDC_DOUBLE);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (ret != SUCCEED) {
        printf("fail to set obj time step @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "object-property-set-type", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    sprintf(obj_name, "object-%d", rank);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // create object
    obj = PDCobj_create(cont, obj_name, obj_prop);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (obj <= 0) {
        printf("fail to create object @ line  %d!\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "object-create", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    for (i = 0; i < 1000; i++) {
        sprintf(obj_name_scale, "object-%d-%d", rank, i);

        // create object
        obj_scale_ids[i] = PDCobj_create(cont, obj_name_scale, obj_prop);

        if (obj_scale_ids[i] <= 0) {
            printf("fail to create object @ line  %d!\n", __LINE__);
            return EXIT_FAILURE;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "object-create-scale", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    for (i = 0; i < 1000; i++) {
        // delete object
        ret = PDCobj_del(obj_scale_ids[i]);

        if (ret != SUCCEED) {
            printf("fail to delete object @ line  %d!\n", __LINE__);
            return EXIT_FAILURE;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "object-delete-scale", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    for (i = 0; i < 1000; i++) {
        // delete a container
        ret = PDCcont_del(cont_scale_ids[i]);

        if (ret != SUCCEED) {
            printf("fail to delete container @ line %d\n", __LINE__);
            return EXIT_FAILURE;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "container-delete-scale", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    ret = PDCcont_put_tag(cont, "SC-24-PDC", tag_value, PDC_STRING, strlen(tag_value) + 1);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (ret != SUCCEED) {
        printf("fail to put tag in container @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "container-put-tag", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    ret = PDCcont_get_tag(cont, "SC-24-PDC", (void **)&tag_value_ret, &value_type, &value_size);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (ret != SUCCEED) {
        printf("fail to get tag in container @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    if (strcmp(tag_value, tag_value_ret) != 0) {
        printf("wrong tag value at container, expected = [%s], got [%s]\n", tag_value, tag_value_ret);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "container-get-tag", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    ret = PDCobj_put_tag(obj, "SC-24-PDC", tag_value, PDC_STRING, strlen(tag_value) + 1);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (ret != SUCCEED) {
        printf("fail to put tag in object @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "object-put-tag", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    ret = PDCobj_get_tag(obj, "SC-24-PDC", (void **)&tag_value_ret, &value_type, &value_size);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (ret != SUCCEED) {
        printf("fail to get tag in object @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    if (strcmp(tag_value, tag_value_ret) != 0) {
        printf("wrong tag value at object, expected = [%s], got [%s]\n", tag_value, tag_value_ret);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "object-get-tag", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    cont_info = PDCcont_get_info(cont_name);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (strcmp(cont_info->name, cont_name) != 0) {
        printf("container name is wrong @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "container-get-info", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    obj_info = PDCobj_get_info(obj);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (strcmp(obj_info->name, obj_name) != 0) {
        printf("object name is wrong @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    if (obj_info->obj_pt->type != PDC_DOUBLE) {
        printf("type is not properly inherited from object property @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    if (obj_info->obj_pt->ndim != ndim) {
        printf("number of dimensions is not properly inherited from object property @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }
    if (obj_info->obj_pt->dims[0] != dims[0]) {
        printf("first dimension is not properly inherited from object property @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }
    if (obj_info->obj_pt->dims[1] != dims[1]) {
        printf("second dimension is not properly inherited from object property @ line %d\n", __LINE__);;
        return EXIT_FAILURE;
    }
    if (obj_info->obj_pt->dims[2] != dims[2]) {
        printf("third dimension is not properly inherited from object property @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "object-get-info", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // close object
    ret = PDCobj_close(obj);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (ret < 0) {
        printf("fail to close object @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "object-close", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // open object
    obj = PDCobj_open(obj_name, pdc);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (ret < 0) {
        printf("fail to open object @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "object-open", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // close a container
    ret = PDCcont_close(cont);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (ret < 0) {
        printf("fail to close container @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "container-close", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // open a container
    cont = PDCcont_open(cont_name, pdc);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (ret < 0) {
        printf("fail to open container @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "container-open", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // close a object property
    ret = PDCprop_close(obj_prop);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    if (ret < 0) {
        printf("fail to close property @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "object-property-close", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    ret = PDCprop_close(cont_prop);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    // close a container property
    if (ret < 0) {
        printf("fail to close property @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "container-property-close", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    ret = PDCobj_del(obj);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    // close a container property
    if (ret != SUCCEED) {
        printf("fail to delete object @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "object-delete", nano(start_time), nano(end_time), elapsed);

    // we can close the object after this test
    PDCobj_close(obj);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    ret = PDCcont_del(cont);

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    // close a container property
    if (ret != SUCCEED) {
        printf("fail to delete container @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "container-delete", nano(start_time), nano(end_time), elapsed);
    
    // we can close the container after this test
    PDCcont_close(cont);

    // ------------------------------------------------------------------------

    MPI_Barrier(MPI_COMM_WORLD);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // close pdc
    ret = PDCclose(pdc);

    clock_gettime(CLOCK_MONOTONIC, &end_time);
    
    if (ret < 0) {
        printf("fail to close PDC @ line %d\n", __LINE__);
        return EXIT_FAILURE;
    }

    // execution time in nano seconds and convert it to seconds
    elapsed = diff(start_time, end_time);

    fprintf(file, "%d;%s;%ld;%ld;%lf\n", rank, "close", nano(start_time), nano(end_time), elapsed);

    // ------------------------------------------------------------------------

    fclose(file);

    MPI_Finalize();

    return 0;
}
