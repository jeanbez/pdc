#include <aws/auth/credentials.h>
#include <aws/common/command_line_parser.h>
#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/common/zero.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/logging.h>
#include <aws/io/uri.h>
#include <aws/s3/s3.h>
#include <aws/s3/s3_client.h>
#include <aws/s3/private/s3_list_objects.h>


struct aws_allocator *allocator;
struct aws_host_resolver *resolver;
struct aws_event_loop_group *event_loop_group;

struct s3_ls_app_data {
    struct aws_uri uri;
    struct app_ctx *app_ctx;
    struct aws_mutex mutex;
    struct aws_condition_variable cvar;
    bool execution_completed;
    bool long_format;
};

struct app_ctx {
    struct aws_allocator *allocator;
    struct aws_s3_client *client;
    struct aws_credentials_provider *credentials_provider;
    struct aws_client_bootstrap *client_bootstrap;
    struct aws_logger logger;
    struct aws_mutex mutex;
    struct aws_condition_variable c_var;
    bool execution_completed;
    struct aws_signing_config_aws signing_config;
    const char *region;
    enum aws_log_level log_level;
    bool help_requested;
    void *sub_command_data;
};

struct app_ctx app_ctx;

void PDC_Server_aws_init();
void PDC_Server_aws_finalize();

int s_on_object(const struct aws_s3_object_info *info, void *user_data);
void s_on_list_finished(struct aws_s3_paginator *paginator, int error_code, void *user_data);