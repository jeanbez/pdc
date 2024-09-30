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
#include <errno.h>
#include "pdc.h"

int
main(int argc, char **argv)
{
    pdcid_t pdc, create_prop, cont;
    // struct _pdc_cont_prop *prop;
    int rank = 0, size = 1;
    int ret_value = 0;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    // create a pdc
    pdc = PDCinit("pdc");
    printf("create a new pdc\n");

    // create a container property
    create_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (create_prop > 0) {
        printf("Create a container property\n");
    }
    else {
        printf("Fail to create container property @ line  %d!\n", __LINE__);
        MPI_Abort(MPI_COMM_WORLD, EIO);
    }
    // print default container lifetime (persistent)
    // prop = PDCcont_prop_get_info(create_prop);
    PDCcont_prop_get_info(create_prop);

    // create a container
    cont = PDCcont_create("c1", create_prop);
    if (cont > 0) {
        printf("Create a container, c1\n");
    }
    else {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        MPI_Abort(MPI_COMM_WORLD, EIO);
    }
    // set container lifetime to transient
    PDCprop_set_cont_lifetime(create_prop, PDC_TRANSIENT);
    // prop = PDCcont_prop_get_info(create_prop);
    PDCcont_prop_get_info(create_prop);

    // set container lifetime to persistent
    PDCcont_persist(cont);
    // prop = PDCcont_prop_get_info(create_prop);
    PDCcont_prop_get_info(create_prop);

    // close a container
    if (PDCcont_close(cont) < 0) {
        printf("fail to close container c1\n");
        MPI_Abort(MPI_COMM_WORLD, EIO);
    }
    else {
        printf("successfully close container c1\n");
    }
    // close a container property
    if (PDCprop_close(create_prop) < 0) {
        printf("Fail to close property @ line %d\n", __LINE__);
        MPI_Abort(MPI_COMM_WORLD, EIO);
    }
    else {
        printf("successfully close container property\n");
    }
    // close pdc
    if (PDCclose(pdc) < 0) {
        printf("fail to close PDC\n");
        MPI_Abort(MPI_COMM_WORLD, EIO);
    }
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return EXIT_SUCCESS;
}
