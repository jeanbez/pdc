/*
 * Copyright Notice for 
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***
 
 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.
 
 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.
 
 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>

/* #define ENABLE_MPI 1 */

#ifdef ENABLE_MPI
  #include "mpi.h"
#endif

#include "pdc.h"
#include "pdc_client_server_common.h"
#include "pdc_client_connect.h"

/* #define NUM_VAR         2 */
/* #define NUM_FLOAT_VAR   2 */
#define NUM_VAR         8
#define NUM_FLOAT_VAR   6
#define NUM_INT_VAR     2
#define NDIM            1
#define NPARTICLES      8388608
#define XDIM            64
#define YDIM            64
#define ZDIM            64


float uniform_random_number()
{
    return (((float)rand())/((float)(RAND_MAX)));
}

int main(int argc, char **argv)
{
    int rank = 0, size = 1, i;
    perr_t ret;
    pdcid_t pdc_id, cont_prop, cont_id;

    char *obj_names[] = {"x", "y", "z", "px", "py", "pz", "id1", "id2"};

//    pdcid_t         obj_ids[NUM_VAR];
    struct PDC_region_info obj_regions[NUM_VAR];
    pdc_metadata_t *obj_metas[NUM_VAR];

    uint64_t float_bytes  = NPARTICLES * sizeof(float);
    uint64_t int_bytes    = NPARTICLES * sizeof(int);

//    uint64_t float_dims[NDIM] = {float_bytes*size};
//    uint64_t int_dims[NDIM] = {int_bytes*size};

    uint64_t myoffset[NDIM], mysize[NDIM];
    void *mydata[NUM_VAR];

    int read_var = NUM_VAR;

    // Float vars are first in the array follow by int vars
    for (i = 0; i < NUM_FLOAT_VAR; i++) 
        mydata[i] = (void*)malloc(float_bytes);

    for (; i < NUM_VAR; i++) 
        mydata[i] = (void*)malloc(int_bytes);

    PDC_Request_t request[NUM_VAR];

    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    struct timeval  pdc_timer_start_1;
    struct timeval  pdc_timer_end_1;

    double sent_time = 0.0, sent_time_total = 0.0, wait_time = 0.0, wait_time_total = 0.0;
    double read_time = 0.0, total_size = 0.0;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    if (argc > 1) 
        read_var = atoi(argv[1]);

    if (read_var < 0 || read_var > 8) 
        read_var = NUM_VAR;
    

    // create a pdc
    pdc_id = PDC_init("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont_id = PDCcont_create("VPIC_cont", cont_prop);
    if(cont_id <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);


    // Query obj metadata and create read region one by one
    for (i = 0; i < NUM_VAR; i++) {

        #ifdef ENABLE_MPI
        ret = PDC_Client_query_metadata_name_timestep_agg(obj_names[i], 0, &obj_metas[i]);
        #else
        ret = PDC_Client_query_metadata_name_timestep(obj_names[i], 0, &obj_metas[i]);
        #endif
        if (ret != SUCCEED || obj_metas[i] == NULL || obj_metas[i]->obj_id == 0) {
            printf("Error with metadata!\n");
            exit(-1);
        }

        if (i<NUM_FLOAT_VAR) {
            myoffset[0] = rank * float_bytes;
            mysize[0]   = float_bytes;
        }
        else {
            myoffset[0] = rank * int_bytes;
            mysize[0]   = int_bytes;
        }
        obj_regions[i].ndim      = NDIM;
        obj_regions[i].offset    = myoffset;
        obj_regions[i].size      = mysize;
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    if (rank == 0) 
        printf("Read %d variables\n", read_var);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    // Timing
    gettimeofday(&pdc_timer_start, 0);

    for (i = 0; i < read_var; i++) {

        // Timing
        gettimeofday(&pdc_timer_start_1, 0);

        request[i].n_client = (size/31==0 ? 1 : 31);
        request[i].n_update = read_var;
        ret = PDC_Client_iread(obj_metas[i], &obj_regions[i], &request[i], mydata[i]);
        if (ret != SUCCEED) {
            printf("Error with PDC_Client_iread!\n");
            goto done;
        }

        #ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        #endif
        gettimeofday(&pdc_timer_end_1, 0);
        sent_time = PDC_get_elapsed_time_double(&pdc_timer_start_1, &pdc_timer_end_1);
        sent_time_total += sent_time;

        /* // Timing */
        /* gettimeofday(&pdc_timer_start_1, 0); */

        /* /1* ret = PDC_Client_wait(&request[i], 30000000, 100); *1/ */
        /* ret = PDC_Client_wait(&request[i], 60000, 100); */
        /* if (ret != SUCCEED) { */
        /*     printf("Error with PDC_Client_wait!\n"); */
        /*     goto done; */
        /* } */

        /* #ifdef ENABLE_MPI */
        /* MPI_Barrier(MPI_COMM_WORLD); */
        /* #endif */
        /* gettimeofday(&pdc_timer_end_1, 0); */
        /* wait_time = PDC_get_elapsed_time_double(&pdc_timer_start_1, &pdc_timer_end_1); */
        /* wait_time_total += wait_time; */

        /* if (rank == 0) */ 
        /*     printf("Sent time %.2f, wait time %.2f\n", sent_time, wait_time); */
    }

    if (rank == 0) 
        printf("Finished sending all read requests, now waiting\n");

    gettimeofday(&pdc_timer_start_1, 0);
    for (i = 0; i < read_var; i++) {
        ret = PDC_Client_wait(&request[i], 200000, 100);
        if (ret != SUCCEED) {
            printf("Error with PDC_Client_wait!\n");
            goto done;
        }
    }
    gettimeofday(&pdc_timer_end_1, 0);
    wait_time = PDC_get_elapsed_time_double(&pdc_timer_start_1, &pdc_timer_end_1);
    wait_time_total += wait_time;


#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif


    gettimeofday(&pdc_timer_end, 0);
    read_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    total_size = NPARTICLES * 4.0 * 8.0 * size / 1024.0 / 1024.0; 
    if (rank == 0) { 
        printf("Read %.2f MB data with %d ranks\nTotal read time: %.2f\nSent %.2f, wait %.2f, Throughput %.2f MB/s\n", 
                total_size, size, read_time, sent_time_total, wait_time_total, total_size/read_time);
        fflush(stdout);
    }

    int verify = 1;
    if (rank == 0) 
        printf("Verifying data correctness ... ");

    // Data verification
    for (i = 0; i < NPARTICLES; i++) {
        if ( ((int*)mydata[7])[i] != i*2                         || 
             ((int*)mydata[6])[i] != i                           ||
             ((float*)mydata[5])[i] != (i*2.0/NPARTICLES) * ZDIM ||
             ((float*)mydata[2])[i] != (i*1.0/NPARTICLES) * ZDIM    ) {
            printf("\nERROR on rank %d at element %d. [%d %d %.2f %.2f]/[%d %d %.2f %.2f]\n", 
                    rank, i, ((int*)mydata[7])[i], ((int*)mydata[6])[i], 
                             ((float*)mydata[5])[i], ((float*)mydata[2])[i], 
                             i*2, i, (i*2.0/NPARTICLES) * ZDIM, (i*1.0/NPARTICLES) * ZDIM);
            verify = 0;
            break;
        } // end if
    } // end of
    if (verify == 1) {
        if (rank == 0) 
            printf("SUCCEED\n");
    }

done:
    fflush(stdout);
    /* for (i = 0; i < NUM_VAR; i++) { */
        /* if(PDCobj_close(obj_ids[i]) < 0) */
        /*     printf("Fail to close %s\n", obj_names[i]); */

        /* if(PDCregion_close(obj_regions[i]) < 0) */
        /*     printf("Fail to close region %s\n", obj_names[i]); */
    /* } */
    

    if(PDCcont_close(cont_id) < 0)
        printf("Fail to close container\n");

    if(PDCprop_close(cont_prop) < 0)
        printf("Fail to close container property\n");

    if(PDC_close(pdc_id) < 0)
       printf("Fail to close PDC\n");

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}

