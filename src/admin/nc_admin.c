/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <nc_admin.h>
#include <nc_admin_connection.h>
#include <nc_admin_poll.h>
#include <nc_core.h>
#include <nc_util.h>
#include <nc_string.h>

const uint8_t RRA_DATATYPE[] = "rra";
const size_t RRA_DATATYPE_LEN = sizeof(RRA_DATATYPE) - 1;
const uint8_t RRA_SET_DATATYPE[] = "rra_set";
const size_t RRA_SET_DATATYPE_LEN = sizeof(RRA_SET_DATATYPE) - 1;
const uint8_t RRA_COUNTER_DATATYPE[] = "rra_counter";
const size_t RRA_COUNTER_DATATYPE_LEN = sizeof(RRA_COUNTER_DATATYPE) - 1;
const uint8_t RRA_SERVICE_BUCKET[] = "rra_config";
const size_t RRA_SERVICE_BUCKET_LEN = sizeof(RRA_SERVICE_BUCKET) - 1;
const uint8_t RRA_SERVICE_KEY[] = "rra_buckets";
const size_t RRA_SERVICE_KEY_LEN = sizeof(RRA_SERVICE_KEY) - 1;

const char *ALLOWED_PROPERTIES[] = {
    "ttl",
    /* should be finished with empty line */
    ""
};

static int
nc_admin_print(const char *format, ...)
{
    va_list ap;
    int res;
    va_start(ap, format);
    res = vprintf(format, ap);
    va_end(ap);
    printf(CRLF);
    return res;
}

static int
nc_admin_set_bucket_prop(const char *host, const char *bucket,
                         const char *prop, const char *value)
{
    int sock = nc_admin_connection_resolve_connect(host);
    if (sock == INVALID_SOCKET) {
        nc_admin_print("Error while connecting to riak");
        return NC_ADMIN_ERROR;
    }

    bool res = nc_admin_connection_set_bucket_prop(sock, bucket, prop, value);
    nc_admin_connection_disconnect(sock);
    if (res) {
        nc_admin_print("OK");
        return NC_ADMIN_OK;
    }
    nc_admin_print("ERROR");
    return NC_ADMIN_ERROR;
}

static int
nc_admin_get_bucket_prop(const char *host, const char *bucket,
                         const char *prop)
{
    uint32_t i;
    int sock = nc_admin_connection_resolve_connect(host);
    if (sock == INVALID_SOCKET) {
        nc_admin_print("Error while connecting to riak");
        return NC_ADMIN_ERROR;
    }
    RpbGetResp *rpbresp = nc_admin_connection_get_bucket_prop(sock, bucket,
                                                              prop);
    nc_admin_connection_disconnect(sock);
    if (rpbresp) {
        if (rpbresp->n_content == 0) {
            nc_admin_print("Not found");
        } else {
            for (i = 0; i < rpbresp->n_content; i++) {
                nc_admin_print("%.*s", rpbresp->content[i]->value.len,
                     rpbresp->content[i]->value.data);
            }
        }
        nc_free(rpbresp);
        return NC_ADMIN_OK;
    }
    return NC_ADMIN_ERROR;
}

static int
nc_admin_del_bucket_prop(const char *host, const char *bucket, const char *prop)
{
    int sock = nc_admin_connection_resolve_connect(host);
    if (sock == INVALID_SOCKET) {
        nc_admin_print("Error while connecting to riak");
        return NC_ADMIN_ERROR;
    }
    bool res = nc_admin_connection_del_bucket_prop(sock, bucket, prop);
    nc_admin_connection_disconnect(sock);
    if (res) {
        nc_admin_print("OK");
        return NC_ADMIN_OK;
    }
    nc_admin_print("ERROR");
    return NC_ADMIN_ERROR;
}

static int
nc_admin_del_bucket_props(const char *host, const char *bucket)
{
    int sock = nc_admin_connection_resolve_connect(host);
    if (sock == INVALID_SOCKET) {
        nc_admin_print("Error while connecting to riak");
        return NC_ADMIN_ERROR;
    }
    bool res = nc_admin_connection_del_bucket_props(sock, bucket);
    nc_admin_connection_disconnect(sock);
    if (res) {
        nc_admin_print("OK");
        return NC_ADMIN_OK;
    }
    nc_admin_print("ERROR");
    return NC_ADMIN_ERROR;
}

static int
nc_admin_list_bucket_props(const char *host, const char *bucket)
{
    uint32_t i = 0;
    bool not_found = true;
    int sock = nc_admin_connection_resolve_connect(host);
    if (sock == INVALID_SOCKET) {
        nc_admin_print("Error while connecting to riak");
        return NC_ADMIN_ERROR;
    }
    while (ALLOWED_PROPERTIES[i][0]) {
        RpbGetResp *rpbresp =
                nc_admin_connection_get_bucket_prop(sock, bucket,
                                                    ALLOWED_PROPERTIES[i]);
        if (rpbresp) {
            if (rpbresp->n_content != 0) {
                if (rpbresp->n_content > 0) {
                    nc_admin_print("%s", ALLOWED_PROPERTIES[i]);
                    not_found = false;
                }
            }
            nc_free(rpbresp);
        } else {
            nc_admin_print("Error while list bucket properties");
            nc_admin_connection_disconnect(sock);
            return NC_ADMIN_ERROR;
        }
        i++;
    }
    nc_admin_connection_disconnect(sock);
    if (not_found) {
        nc_admin_print("Not found");
    }
    return NC_ADMIN_OK;
}

static int
nc_admin_list_buckets(const char *host)
{
    uint32_t i;
    int sock = nc_admin_connection_resolve_connect(host);
    if (sock == INVALID_SOCKET) {
        nc_admin_print("Error while connecting to riak");
        return NC_ADMIN_ERROR;
    }
    DtFetchResp *rpbresp = nc_admin_connection_list_buckets(sock);
    nc_admin_connection_disconnect(sock);
    if (rpbresp) {
        if (rpbresp->value == NULL) {
            nc_admin_print("Nothing found");
        } else if (rpbresp->value->n_set_value == 0) {
            nc_admin_print("Nothing found");
        } else {
            for (i = 0; i < rpbresp->value->n_set_value; i++) {
                nc_admin_print("%.*s", rpbresp->value->set_value[i].len,
                               rpbresp->value->set_value[i].data);
            }
        }
        nc_free(rpbresp);
        return NC_ADMIN_OK;
    }
    return NC_ADMIN_ERROR;
}

static int
nc_admin_list_all(const char *host)
{
    uint32_t i;
    uint32_t j;
    int sock = nc_admin_connection_resolve_connect(host);
    if (sock == INVALID_SOCKET) {
        nc_admin_print("Error while connecting to riak");
        return NC_ADMIN_ERROR;
    }

    DtFetchResp *bl = nc_admin_connection_list_buckets(sock);
    if (bl == NULL) {
        nc_admin_connection_disconnect(sock);
        return NC_ADMIN_ERROR;
    }
    bool nf = (bl->value == 0);
    if (!nf) {
        nf = (bl->value->n_set_value == 0);
    }
    if (nf) {
        nc_admin_connection_disconnect(sock);
        nc_admin_print("Nothing found");
        nc_free(bl);
        return NC_ADMIN_OK;
    }
    for (i = 0; i < bl->value->n_set_value; i++) {
        uint8_t bucket[bl->value->set_value[i].len + 1];
        sprintf((char *)bucket, "%.*s", (int)bl->value->set_value[i].len,
                        bl->value->set_value[i].data);
        nc_admin_print("%s", bucket);
        j = 0;
        while (ALLOWED_PROPERTIES[j][0]) {
            RpbGetResp *prop;
            prop = nc_admin_connection_get_bucket_prop(sock, (char *)bucket,
                                                       ALLOWED_PROPERTIES[j]);
            if (prop == NULL) {
                nc_admin_connection_disconnect(sock);
                nc_free(bl);
                return NC_ADMIN_ERROR;
            }
            if (prop->n_content > 0) {
                if (prop->content[0]->value.len > 0) {
                    nc_admin_print("  %s: %.*s", ALLOWED_PROPERTIES[j],
                                   prop->content[0]->value.len,
                                   prop->content[0]->value.data);
                }
            }
            nc_free(prop);
            j++;
        }
    }
    nc_admin_connection_disconnect(sock);
    nc_free(bl);

    return NC_ADMIN_OK;
}

static bool
nc_admin_check_args(int need, const char *arg1, const char *arg2,
                    const char *arg3, const char *prop,
                    const char *value)
{
    if (arg1 == NULL && need > 0) {
        nc_admin_print("Bucket name require");
        return false;
    }
    if (arg2 == NULL && need > 1) {
        nc_admin_print("Property name require");
        return false;
    }
    if (arg3 == NULL && need > 2) {
        nc_admin_print("Value require");
        return false;
    }
    if ((arg1 && need <= 0) || (arg2 && need <= 1) || (arg2 && need <= 1)) {
        nc_admin_print("Extra argument found");
        return false;
    }

    if (prop == NULL) {
        return true;
    }
    uint32_t i = 0;
    while (ALLOWED_PROPERTIES[i][0]) {
        if (nc_c_strequ(prop, ALLOWED_PROPERTIES[i])) {
            /* validate prop values */
            if (value) {
                int64_t ttl;
                if (nc_c_strequ(prop, "ttl")) {
                    struct string str = {nc_strlen(value), (uint8_t *)value};
                    if (!nc_read_ttl_value(&str, &ttl)) {
                        nc_admin_print("Invalid ttl value, specify quantity "
                                       "and units, ie '15s' for 15 seconds");
                        return false;
                    }
                }
            }
            return true;
        }
        i++;
    }
    nc_admin_print("Unknown bucket property");
    return false;
}

void
nc_admin_show_usage(void)
{
    uint32_t i = 0;
    nc_admin_print(
        "Usage: nutcracker admin riak_host:port command args" CRLF
        "Available commands and their arguments are:" CRLF
        "    set-bucket-prop bucket property value - set bucket property" CRLF
        "    get-bucket-prop bucket property - get bucket property value" CRLF
        "    del-bucket-prop bucket property - delete bucket property" CRLF
        "    del-bucket-props bucket - delete bucket with all properties" CRLF
        "    list-buckets - list existing buckets" CRLF
        "    list-bucket-props bucket - list existing properties for bucket" CRLF
        "    list-all - list all buckets with all properties and values" CRLF
        "Available properties are:");
    while (ALLOWED_PROPERTIES[i][0]) {
        nc_admin_print("    %s", ALLOWED_PROPERTIES[i]);
        i++;
    }
}

static char *
nc_admin_check_bucket(const char *str)
{
    struct string datatype;
    struct string bucket;
    char *res = NULL;
    uint32_t str_len = nc_strlen(str);
    if (nc_parse_datatype_bucket((uint8_t *)str, str_len, &datatype, &bucket)) {
        if (str_len == bucket.len) {
            nc_admin_print("Using '%.*s' datatype for bucket '%.*s'",
                           datatype.len, datatype.data,
                           bucket.len, bucket.data);
        }
        res = nc_alloc(datatype.len + bucket.len + 2);
        if (res) {
            int l = sprintf(res, "%.*s:%.*s", datatype.len,
                            datatype.data, bucket.len, bucket.data);
            ASSERT(l == datatype.len + bucket.len + 1);
        }
        string_deinit(&datatype);
        string_deinit(&bucket);
    }
    return res;
}

int
nc_admin_command(const char *host, const char *command,
                 const char *arg1, const char *arg2,
                 const char *arg3)
{
    int res = NC_ADMIN_ERROR;
    char *bucket;
    if (nc_c_strequ(command, "set-bucket-prop")) {
        if (nc_admin_check_args(3, arg1, arg2, arg3, arg2, arg3)) {
            bucket = nc_admin_check_bucket(arg1);
            if (bucket) {
                res = nc_admin_set_bucket_prop(host, bucket, arg2, arg3);
                nc_free(bucket);
            }
        }
    } else if (nc_c_strequ(command, "get-bucket-prop")) {
        if (nc_admin_check_args(2, arg1, arg2, arg3, arg2, NULL)) {//
            bucket = nc_admin_check_bucket(arg1);
            if (bucket) {
                res = nc_admin_get_bucket_prop(host, bucket, arg2);
                nc_free(bucket);
            }
        }
    } else if (nc_c_strequ(command, "del-bucket-props")) {
        if (nc_admin_check_args(1, arg1, arg2, arg3, NULL, NULL)) {
            bucket = nc_admin_check_bucket(arg1);
            if (bucket) {
                res = nc_admin_del_bucket_props(host, bucket);
                nc_free(bucket);
            }
        }
    } else if (nc_c_strequ(command, "del-bucket-prop")) {
        if (nc_admin_check_args(2, arg1, arg2, arg3, arg2, NULL)) {
            bucket = nc_admin_check_bucket(arg1);
            if (bucket) {
                res = nc_admin_del_bucket_prop(host, bucket, arg2);
                nc_free(bucket);
            }
        }
    } else if (nc_c_strequ(command, "list-bucket-props")) {
        if (nc_admin_check_args(1, arg1, arg2, arg3, NULL, NULL)) {
            bucket = nc_admin_check_bucket(arg1);
            if (bucket) {
                res = nc_admin_list_bucket_props(host, bucket);
                nc_free(bucket);
            }
        }
    } else if (nc_c_strequ(command, "list-buckets")) {
        if (nc_admin_check_args(0, arg1, arg2, arg3, NULL, NULL)) {
            res = nc_admin_list_buckets(host);
        }
    } else if (nc_c_strequ(command, "list-all")) {
        if (nc_admin_check_args(0, arg1, arg2, arg3, NULL, NULL)) {
            res = nc_admin_list_all(host);
        }
    } else {
        nc_admin_print("Unknown command");
        nc_admin_show_usage();
    }

    return res;
}
