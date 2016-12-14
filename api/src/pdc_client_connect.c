#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* #define ENABLE_MPI 1 */
#ifdef ENABLE_MPI
    #include "mpi.h"
#endif

#include "mercury.h"
#include "mercury_request.h"
#include "mercury_macros.h"
#include "mercury_hash_table.h"

#include "pdc_interface.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

int                    pdc_client_mpi_rank_g = 0;
int                    pdc_client_mpi_size_g = 1;

int                    pdc_server_num_g;
pdc_server_info_t     *pdc_server_info_g = NULL;
int                    pdc_use_local_server_only_g = 0;

static int             mercury_has_init_g = 0;
static hg_class_t     *send_class_g = NULL;
static hg_context_t   *send_context_g = NULL;
static int             work_todo_g = 0;
static hg_id_t         gen_obj_register_id_g;

hg_hash_table_t       *obj_names_cache_g = NULL;

static int            *debug_server_id_count = NULL;

static uint32_t pdc_hash_djb2(const char *pc) {
        uint32_t hash = 5381, c;
        while (c = *pc++)
            hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
        if (hash < 0) 
            hash *= -1;
        
        return hash;
}

static uint32_t pdc_hash_sdbm(const char *pc) {
        uint32_t hash = 0, c;
        while (c = (*pc++)) 
                hash = c + (hash << 6) + (hash << 16) - hash;
        if (hash < 0) 
            hash *= -1;
        return hash;
}

static int
PDC_Client_int_equal(hg_hash_table_key_t vlocation1, hg_hash_table_key_t vlocation2)
{
    return *((int *) vlocation1) == *((int *) vlocation2);
}

static unsigned int
PDC_Client_int_hash(hg_hash_table_key_t vlocation)
{
    return *((unsigned int *) vlocation);
}

static void
PDC_Client_int_hash_key_free(hg_hash_table_key_t key)
{
    free((int *) key);
}

static void
PDC_Client_int_hash_value_free(hg_hash_table_value_t value)
{
    free((int *) value);
}

typedef struct hash_value_client_obj_name_t {
    char obj_name[PATH_MAX];
} hash_value_client_obj_name_t;

static void
metadata_hash_value_free(hg_hash_table_value_t value)
{
    free((hash_value_client_obj_name_t*) value);
}


// ^ Client hash table related functions

int PDC_Client_read_server_addr_from_file()
{
    FUNC_ENTER(NULL);

    int ret_value;
    int i = 0;
    char  *p;
    FILE *na_config = NULL;
    char config_fname[PATH_MAX];
    sprintf(config_fname, "%s/%s", pdc_server_tmp_dir_g, pdc_server_cfg_name_g);
    /* printf("config file:%s\n",config_fname); */
    na_config = fopen(config_fname, "r");
    if (!na_config) {
        fprintf(stderr, "Could not open config file from default location: %s\n", pdc_server_cfg_name_g);
        exit(0);
    }
    char n_server_string[PATH_MAX];
    // Get the first line as $pdc_server_num_g
    fgets(n_server_string, PATH_MAX, na_config);
    pdc_server_num_g = atoi(n_server_string);

    // Allocate $pdc_server_info_g
    pdc_server_info_g = (pdc_server_info_t*)malloc(sizeof(pdc_server_info_t) * pdc_server_num_g);
    /* for (i = 0; i < pdc_server_num_g; i++) { */
        /* pdc_server_info_g[i].addr_valid = 0; */
        /* pdc_server_info_g[i].rpc_handle_valid = 0; */
    /* } */

    while (fgets(pdc_server_info_g[i].addr_string, ADDR_MAX, na_config)) {
        p = strrchr(pdc_server_info_g[i].addr_string, '\n');
        if (p != NULL) *p = '\0';
        /* printf("%s", pdc_server_info_g[i].addr_string); */
        i++;
    }
    fclose(na_config);

    if (i != pdc_server_num_g) {
        printf("Invalid number of servers in server.cfg\n");
        exit(0);
    }

    ret_value = i;

done:
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    /* printf("Entered client_rpc_cb()"); */
    struct client_lookup_args *client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    /* Get output from server*/
    gen_obj_id_out_t output;
    ret_value = HG_Get_output(handle, &output);
    /* printf("Return value=%llu\n", output.ret); */
    client_lookup_args->obj_id = output.ret;

    work_todo_g--;

done:
    FUNC_LEAVE(ret_value);
}

// Callback function for HG_Addr_lookup()
// Start RPC connection
static hg_return_t
client_lookup_cb(const struct hg_cb_info *callback_info)
{

    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;

    struct client_lookup_args *client_lookup_args = (struct client_lookup_args *) callback_info->arg;
    int server_id = client_lookup_args->server_id;
    pdc_server_info_g[server_id].addr = callback_info->info.lookup.addr;
    pdc_server_info_g[server_id].addr_valid = 1;

    // Create HG handle if needed
    int handle_valid = pdc_server_info_g[server_id].rpc_handle_valid;
    if (handle_valid != 1) {
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, gen_obj_register_id_g, &pdc_server_info_g[server_id].rpc_handle);
        pdc_server_info_g[server_id].rpc_handle_valid = 1;
    }

    // Fill input structure
    gen_obj_id_in_t in;
    in.obj_name   = client_lookup_args->obj_name;
    in.hash_value = client_lookup_args->hash_value;
    in.user_id    = client_lookup_args->user_id;
    in.app_name   = client_lookup_args->app_name;
    in.time_step  = client_lookup_args->time_step;
    in.tags       = client_lookup_args->tags;
    /* printf("Hash(%s) = %d\n", client_lookup_args->obj_name, in.hash_value); */

    /* printf("Sending input to target\n"); */
    ret_value = HG_Forward(pdc_server_info_g[server_id].rpc_handle, client_rpc_cb, client_lookup_args, &in);
    if (ret_value != HG_SUCCESS) {
        fprintf(stderr, "client_lookup_cb(): Could not start HG_Forward()\n");
        return EXIT_FAILURE;
    }

done:
    FUNC_LEAVE(ret_value);
}

// Init Mercury class and context
// Register gen_obj_id rpc
perr_t PDC_Client_mercury_init(hg_class_t **hg_class, hg_context_t **hg_context, int port)
{

    FUNC_ENTER(NULL);

    perr_t ret_value;

    char na_info_string[PATH_MAX];
    sprintf(na_info_string, "bmi+tcp://%d", port);
    /* sprintf(na_info_string, "cci+tcp://%d", port); */
    if (pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT: using %.7s\n\n", na_info_string);
        fflush(stdout);
    }
    
    if (!na_info_string) {
        fprintf(stderr, HG_PORT_NAME " environment variable must be set, e.g.:\nMERCURY_PORT_NAME=\"cci+tcp://22222\"\n");
        exit(0);
    }

    /* Initialize Mercury with the desired network abstraction class */
    /* printf("Using %s\n", na_info_string); */
    *hg_class = HG_Init(na_info_string, NA_FALSE);
    if (*hg_class == NULL) {
        printf("Error with HG_Init()\n");
        goto done;
    }

    /* Create HG context */
    *hg_context = HG_Context_create(*hg_class);

    // Register RPC
    gen_obj_register_id_g = gen_obj_id_register(*hg_class);

    ret_value = 0;

done:
    FUNC_LEAVE(ret_value);
}

// Check if all work has been processed
// Using global variable $mercury_work_todo_g
perr_t PDC_Client_check_response(hg_context_t **hg_context)
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    hg_return_t hg_ret;
    do {
        unsigned int actual_count = 0;
        do {
            hg_ret = HG_Trigger(*hg_context, 0/* timeout */, 1 /* max count */, &actual_count);
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* printf("actual_count=%d\n",actual_count); */
        /* Do not try to make progress anymore if we're done */
        if (work_todo_g <= 0)  break;

        hg_ret = HG_Progress(*hg_context, HG_MAX_IDLE_TIME);
    } while (hg_ret == HG_SUCCESS);

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_init()
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    pdc_server_info_g = NULL;

    // get server address and fill in $pdc_server_info_g
    PDC_Client_read_server_addr_from_file();

#ifdef ENABLE_MPI
    MPI_Comm_rank(MPI_COMM_WORLD, &pdc_client_mpi_rank_g);
    MPI_Comm_size(MPI_COMM_WORLD, &pdc_client_mpi_size_g);
#endif
    if (pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT: Found %d PDC Metadata servers, running with %d PDC clients\n", pdc_server_num_g ,pdc_client_mpi_size_g);
    }
    /* printf("pdc_rank:%d\n", pdc_client_mpi_rank_g); */
    fflush(stdout);

    // Init client hash table to cache created object names
    obj_names_cache_g = hg_hash_table_new(PDC_Client_int_hash, PDC_Client_int_equal);
    hg_hash_table_register_free_functions(obj_names_cache_g, PDC_Client_int_hash_key_free, PDC_Client_int_hash_value_free);


    // Init debug info
    if (pdc_server_num_g > 0) { 
        debug_server_id_count = (int*)malloc(sizeof(int) * pdc_server_num_g);
        memset(debug_server_id_count, 0, sizeof(int) * pdc_server_num_g);
    }
    else
        printf("==PDC_CLIENT: Server number not properly initialized!\n");

    char *tmp= NULL;
    int   tmp_env;
    tmp = getenv("PDC_USE_LOCAL_SERVER");
    if (tmp != NULL) {
        tmp_env = atoi(tmp);
        if (tmp_env == 1) {
            pdc_use_local_server_only_g = 1;
            if(pdc_client_mpi_rank_g == 0)
                printf("==PDC_CLIENT: Contact local server only!\n");
        }
    }

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_finalize()
{
    FUNC_ENTER(NULL);

    perr_t ret_value;

    // Send close server request to all servers
    if (pdc_client_mpi_rank_g == 0) 
        PDC_Client_close_all_server();


    // Finalize Mercury
    int i;
    for (i = 0; i < pdc_server_num_g; i++) {
        if (pdc_server_info_g[i].addr_valid) {
            HG_Addr_free(send_class_g, pdc_server_info_g[i].addr);
        }
    }
    HG_Context_destroy(send_context_g);
    HG_Finalize(send_class_g);

    if (pdc_server_info_g != NULL)
        free(pdc_server_info_g);

    // Free client hash table
    if (obj_names_cache_g != NULL) 
        hg_hash_table_free(obj_names_cache_g);

    // Output and free debug info
#ifdef ENABLE_MPI

/*     // Print local server connection count */
/*     for (i = 0; i < pdc_server_num_g; i++) */ 
/*         printf("%d, %d, %d\n", pdc_client_mpi_rank_g, i, debug_server_id_count[i]); */
/*     fflush(stdout); */

    int *all_server_count = (int*)malloc(sizeof(int)*pdc_server_num_g);
    MPI_Reduce(debug_server_id_count, all_server_count, pdc_server_num_g, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    if (pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT: server connection count:\n");
        for (i = 0; i < pdc_server_num_g; i++) 
            printf("  Server[%3d], %d\n", i, all_server_count[i]);
    }
    free(all_server_count);
#else
    for (i = 0; i < pdc_server_num_g; i++) {
        printf("  Server%3d, %d\n", i, debug_server_id_count[i]);
    }

#endif
    if (debug_server_id_count != NULL) 
        free(debug_server_id_count);

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}
uint64_t PDC_Client_send_name_to_server(const char *obj_name, int hash_name_value, pdcid_t property, int time_step, uint32_t server_id)
{
    FUNC_ENTER(NULL);

    uint64_t ret_value;
    hg_return_t  hg_ret = 0;

    uint32_t port = pdc_client_mpi_rank_g + 8000; 

    debug_server_id_count[server_id]++;

    if (mercury_has_init_g == 0) {
        // Init Mercury network connection
        PDC_Client_mercury_init(&send_class_g, &send_context_g, port);
        if (send_class_g == NULL || send_context_g == NULL) {
            printf("Error with Mercury Init, exiting...\n");
            exit(0);
        }
        mercury_has_init_g = 1;
    }


    struct client_lookup_args lookup_args;
    // TODO: parse pdc prop structure once Kimmy adds these fields to PDC_obj_prop_t
    lookup_args.user_id     = getuid();
    lookup_args.app_name    = "test_app_name";
    lookup_args.time_step   = time_step;
    lookup_args.hash_value  = hash_name_value;
    lookup_args.tags        = "tag0=11";

    lookup_args.obj_name    = obj_name;
    lookup_args.obj_id      = -1;
    lookup_args.server_id   = server_id;

    /* printf("==PDC_CLIENT: obj_name=%s, user_id=%u, time_step=%u\n", lookup_args.obj_name, lookup_args.user_id, lookup_args.time_step); */

    // Initiate server lookup and send obj name in client_lookup_cb()
    char *target_addr_string = pdc_server_info_g[server_id].addr_string;

    // Check if previous connection had been made and reuse handle 
    if (pdc_server_info_g[server_id].rpc_handle_valid != 1) {
        // This is the first connection to server $server_id
        hg_ret = HG_Addr_lookup(send_context_g, client_lookup_cb, &lookup_args, target_addr_string, HG_OP_ID_IGNORE);
    }
    else {
        // Fill input structure
        gen_obj_id_in_t in;
        in.obj_name   = lookup_args.obj_name;
        in.hash_value = lookup_args.hash_value;
        in.user_id    = lookup_args.user_id;
        in.app_name   = lookup_args.app_name;
        in.time_step  = lookup_args.time_step;
        in.tags       = lookup_args.tags;
        /* printf("Hash(%s) = %d\n", obj_name, in.hash_value); */

        /* printf("Sending input to target\n"); */
        ret_value = HG_Forward(pdc_server_info_g[server_id].rpc_handle, client_rpc_cb, &lookup_args, &in);
        if (ret_value != HG_SUCCESS) {
            fprintf(stderr, "client_lookup_cb(): Could not start HG_Forward()\n");
            return EXIT_FAILURE;
        }
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    // Now we have obj id stored in lookup_args.obj_id
    /* if (lookup_args.obj_id == -1) { */
    /*     printf("==PDC_CLIENT: Have not obtained valid obj id from PDC server!\n"); */
    /* } */

    /* printf("Received obj_id=%llu\n", lookup_args.obj_id); */
    /* fflush(stdout); */


    ret_value = lookup_args.obj_id;

done:
    FUNC_LEAVE(ret_value);
}

// Send a name to server and receive an obj id
uint64_t PDC_Client_send_name_recv_id(const char *obj_name, pdcid_t property)
{

    FUNC_ENTER(NULL);

    uint64_t ret_value;
    uint32_t server_id;

    // TODO: this is temp solution to convert "Obj_%d" to name="Obj_" and time_step=%d 
    //       will need to delete once Kimmy adds the pdc_prop related functions
    int i, obj_name_len;
    uint32_t tmp_time_step = 0;
    obj_name_len = strlen(obj_name);
    char *tmp_obj_name = (char*)malloc(sizeof(char) * (obj_name_len+1));
    strcpy(tmp_obj_name, obj_name);
    for (i = 0; i < obj_name_len; i++) {
        if (isdigit(obj_name[i])) {
            tmp_time_step = atoi(obj_name+i);
            /* printf("Converted [%s] = %d\n", obj_name, tmp_time_step); */
            tmp_obj_name[i] = 0;
            break;
        }
    }

    // TODO: Compute server id
    uint32_t hash_name_value = pdc_hash_djb2(tmp_obj_name);
    server_id = (hash_name_value + tmp_time_step) % pdc_server_num_g; 

    // Use local server only?
    if (pdc_use_local_server_only_g == 1) {
        server_id = pdc_client_mpi_rank_g % pdc_server_num_g;
    }

    /* printf("Obj_name: %s, hash_value: %d, server_id:%d\n", tmp_obj_name, hash_name_value, server_id); */
   
    /* printf("[%d]: target server %d\n", pdc_client_mpi_rank_g, server_id); */
    /* fflush(stdout); */
    ret_value = PDC_Client_send_name_to_server(tmp_obj_name, hash_name_value, property, tmp_time_step, server_id);

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_close_all_server()
{

    FUNC_ENTER(NULL);

    uint64_t ret_value;
    hg_return_t  hg_ret = 0;
    int server_id;
    int port      = pdc_client_mpi_rank_g + 8000; 

    if (mercury_has_init_g == 0) {
        // Init Mercury network connection
        PDC_Client_mercury_init(&send_class_g, &send_context_g, port);
        if (send_class_g == NULL || send_context_g == NULL) {
            printf("Error with Mercury Init, exiting...\n");
            exit(0);
        }
        mercury_has_init_g = 1;
    }

    int i;
    for (i = 0; i < pdc_server_num_g; i++) {
        server_id = i;

        /* printf("Closing server %d\n", server_id); */
        /* fflush(stdout); */
        ret_value = PDC_Client_send_name_to_server("==PDC Close Server", 0, NULL, 0, server_id);
    }

    printf("All server closed\n");
    fflush(stdout);

    ret_value = 0;

done:
    FUNC_LEAVE(ret_value);
}
