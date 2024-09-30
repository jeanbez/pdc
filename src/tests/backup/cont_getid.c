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
#include <errno.h>
#include "pdc.h"

int
main(int argc, char **argv)
{
    int     rank = 0, size = 1;
    pdcid_t pdc_id, cont_prop, cont_id, cont_id2, cont_id3;
    int     ret_value = 0;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc
    pdc_id = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if (cont_prop <= 0) {
        printf("Fail to create container property @ line  %d!\n", __LINE__);
        MPI_Abort(MPI_COMM_WORLD, EIO);
    }
    // create a container
    cont_id = PDCcont_create_col("c1", cont_prop);
    if (cont_id <= 0) {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        MPI_Abort(MPI_COMM_WORLD, EIO);
    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    cont_id2 = PDCcont_open("c1", pdc_id);
    if (cont_id2 == 0) {
        printf("Fail to open container @ line  %d!\n", __LINE__);
        MPI_Abort(MPI_COMM_WORLD, EIO);
    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    cont_id3 = PDCcont_get_id("c1", pdc_id);
    if (PDCcont_close(cont_id3) < 0) {
        printf("fail to close container cont_id3\n");
        MPI_Abort(MPI_COMM_WORLD, EIO);
    }
    // close a container
    if (PDCcont_close(cont_id) < 0) {
        printf("fail to close container cont_id1\n");
        MPI_Abort(MPI_COMM_WORLD, EIO);
    }
    if (PDCcont_close(cont_id2) < 0) {
        printf("fail to close container cont_id2\n");
        MPI_Abort(MPI_COMM_WORLD, EIO);
    }
    // close a container property
    if (PDCprop_close(cont_prop) < 0) {
        printf("Fail to close property @ line %d\n", __LINE__);
        MPI_Abort(MPI_COMM_WORLD, EIO);
    }
    if (PDCclose(pdc_id) < 0) {
        printf("fail to close PDC\n");
        MPI_Abort(MPI_COMM_WORLD, EIO);
    }
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return EXIT_SUCCESS;
}
