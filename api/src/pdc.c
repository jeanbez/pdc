#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "pdc.h"
#include "pdc_private.h"
#include "pdc_malloc.h"
#include "pdc_interface.h"

//
#include "mercury.h"
#include "pdc_client_server_common.h"
#include "pdc_client_connect.h"


static PDC_CLASS_t *PDC__class_create() {
    PDC_CLASS_t *ret_value = NULL;         /* Return value */

    FUNC_ENTER(NULL);

    PDC_CLASS_t *pc;
    if(NULL == (pc = PDC_CALLOC(PDC_CLASS_t)))
        PGOTO_ERROR(NULL, "create pdc class: memory allocation failed"); 
    ret_value = pc;
done:
    FUNC_LEAVE(ret_value);
} /* end of PDC__create_class() */

pdcid_t PDC_init(PDC_prop_t property) {
    pdcid_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);
    PDC_CLASS_t *pc = PDC__class_create();
    if(pc == NULL)

	    PGOTO_ERROR(FAIL, "fail to allocate pdc type");

    if(PDCprop_init(pc) < 0)
        PGOTO_ERROR(FAIL, "PDC property init error");
    if(PDCcont_init(pc) < 0)
        PGOTO_ERROR(FAIL, "PDC container init error");
    if(PDCobj_init(pc) < 0)
        PGOTO_ERROR(FAIL, "PDC object init error");
    if(PDCregion_init(pc) < 0)
        PGOTO_ERROR(FAIL, "PDC object init error");

    // METADATA Init: client server connection
    pdc_server_info = NULL;
    // get server address and fill in $pdc_server_info
    PDC_Client_read_server_addr_from_file();
    printf("PDC_init(): found %d servers\n", pdc_server_num_g);


    // PDC Client Server connection init
    PDC_Client_init();


    // create pdc id
    pdcid_t pdcid = (pdcid_t)pc;
    ret_value = pdcid;
done:
    FUNC_LEAVE(ret_value);
} /* end of PDC_init() */

perr_t PDC_close(pdcid_t pdcid) {
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    // check every list before closing
    // container property
    if(PDC_prop_cont_list_null(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to close container property");
    // object property
    if(PDC_prop_obj_list_null(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to close object property");
    // container
    if(PDC_cont_list_null(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to close container");
    // object
    if(PDC_obj_list_null(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to close object");
    // region
    if(PDC_region_list_null(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to close region");
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *) pdcid;
    if(pc == NULL)
        PGOTO_ERROR(FAIL, "PDC init fails");
    if(PDCprop_end(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to destroy property");
    if(PDCcont_end(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to destroy container");
    if(PDCobj_end(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to destroy object");
    if(PDCregion_end(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to destroy object");
    pc = PDC_FREE(PDC_CLASS_t, pc);

    // Finalize METADATA
    PDC_Client_finalize();

done:
    FUNC_LEAVE(ret_value);
} /* end of PDC_close() */

pdcid_t PDCtype_create(PDC_STRUCT pdc_struct) {
}

perr_t PDCtype_struct_field_insert(pdcid_t type_id, const char *name, uint64_t offset, PDC_var_type_t var_type) {
}

perr_t PDCget_loci_count(pdcid_t pdc_id, pdcid_t *nloci) {
}

perr_t PDCget_loci_info(pdcid_t pdc_id, pdcid_t n, PDC_loci_info_t *info) {
}

pdcid_t PDC_query_create(pdcid_t pdc_id, PDC_query_type_t query_type, PDC_query_op_t query_op, ...) {
}

pdcid_t PDC_query_combine(pdcid_t query1_id, PDC_com_op_mode_t combine_op, pdcid_t query2_id) {
}

