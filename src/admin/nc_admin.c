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
#include <admin/nc_admin.h>
#include <admin/nc_admin_connection.h>
#include <nc_core.h>
#include <nc_string.h>

/*
 * To enable datatypes which is require for this admin util, run:
 * riak-admin bucket-type create rra
 * riak-admin bucket-type activate rra
 * riak-admin bucket-type create rra_set '{"props":{"datatype":"set"}}'
 * riak-admin bucket-type activate rra_set
 * riak-admin bucket-type create rra_counter '{"props":{"datatype":"counter"}}'
 * riak-admin bucket-type activate rra_counter
 */

const uint8_t RRA_DATATYPE[] = "rra";
const size_t RRA_DATATYPE_LEN = sizeof(RRA_DATATYPE) - 1;
const uint8_t RRA_SET_DATATYPE[] = "rra_set";
const size_t RRA_SET_DATATYPE_LEN = sizeof(RRA_SET_DATATYPE) - 1;
const uint8_t RRA_COUNTER_DATATYPE[] = "rra_counter";
const size_t RRA_COUNTER_DATATYPE_LEN = sizeof(RRA_COUNTER_DATATYPE) - 1;
const uint8_t RRA_SERVICE_BUCKET[] = "rra_config";
const size_t RRA_SERVICE_BUCKET_LEN = sizeof(RRA_DATATYPE) - 1;
const uint8_t RRA_SERVICE_KEY[] = "rra_buckets";
const size_t RRA_SERVICE_KEY_LEN = sizeof(RRA_DATATYPE) - 1;

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
    int sock = nc_admin_connection_connect(host);
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
    int sock = nc_admin_connection_connect(host);
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
nc_admin_list_bucket_props(const char *host)
{
    uint32_t i = 0;
    while (ALLOWED_PROPERTIES[i][0]) {
        nc_admin_print(ALLOWED_PROPERTIES[i]);
        i++;
    }
    return NC_ADMIN_OK;
}

static int
nc_admin_list_buckets(const char *host)
{
    uint32_t i;
    int sock = nc_admin_connection_connect(host);
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
nc_admin_check_args(int need, const char *arg1, const char *arg2,
                    const char *arg3, const char *prop)
{
    if (arg1 == NULL && need > 0) {
        nc_admin_print("Bucket name require");
        return NC_ADMIN_ERROR;
    }
    if (arg2 == NULL && need > 1) {
        nc_admin_print("Property name require");
        return NC_ADMIN_ERROR;
    }
    if (arg3 == NULL && need > 2) {
        nc_admin_print("Value require");
        return NC_ADMIN_ERROR;
    }
    if ((arg1 && need <= 0) || (arg2 && need <= 1) || (arg2 && need <= 1)) {
        nc_admin_print("Extra argument found");
        return NC_ADMIN_ERROR;
    }

    if (prop == NULL) {
        return NC_ADMIN_OK;
    }
    uint32_t i = 0;
    while (ALLOWED_PROPERTIES[i][0]) {
        if (nc_strcmp(prop, ALLOWED_PROPERTIES[i]) == 0) {
            return NC_ADMIN_OK;
        }
        i++;
    }
    nc_admin_print("Unknown bucket property");
    return NC_ADMIN_ERROR;
}

int
nc_admin_command(const char *host, const char *command,
                 const char *arg1, const char *arg2,
                 const char *arg3)
{
    if (nc_strcmp(command, "set-bucket-prop") == 0) {
        if(nc_admin_check_args(3, arg1, arg2, arg3, arg2)) {
            return NC_ADMIN_ERROR;
        }
        return nc_admin_set_bucket_prop(host, arg1, arg2, arg3);
    } else if (nc_strcmp(command, "get-bucket-prop") == 0) {
        if(nc_admin_check_args(2, arg1, arg2, arg3, NULL)) {
            return NC_ADMIN_ERROR;
        }
        return nc_admin_get_bucket_prop(host, arg1, arg2);
    } else if (nc_strcmp(command, "list-bucket-props") == 0) {
        if(nc_admin_check_args(0, arg1, arg2, arg3, NULL)) {
            return NC_ADMIN_ERROR;
        }
        return nc_admin_list_bucket_props(host);
    } else if (nc_strcmp(command, "list-buckets") == 0) {
        if(nc_admin_check_args(0, arg1, arg2, arg3, NULL)) {
            return NC_ADMIN_ERROR;
        }
        return nc_admin_list_buckets(host);
    }

    nc_admin_print(
        "Unknown command, available commands are:" CRLF
        "    set-bucket-prop bucket property value - set bucket property" CRLF
        "    get-bucket-prop bucker property - get property value" CRLF
        "    list-buckets - list existing buckets with properties" CRLF
        "    list-bucket-props - list allowed properties for buckets" CRLF
        "");
    return NC_ADMIN_ERROR;
}
