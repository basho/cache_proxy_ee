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

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <stdarg.h>

#include <nc_admin.h>
#include <nc_core.h>
#include <nc_string.h>
#include <nc_riak_private.h>
#include <proto/riak.pb-c.h>
#include <proto/riak_kv.pb-c.h>
#include <proto/riak_dt.pb-c.h>

#define INVALID_SOCKET (-1)

const static uint8_t ADMIN_DATATYPE[] = "rra";
const static size_t ADMIN_DATATYPE_LEN = sizeof(ADMIN_DATATYPE) - 1;

typedef size_t (*pack_func)(const void *message,uint8_t *out);

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
nc_admin_connect(const char *host)
{
    const size_t hostlen = nc_strlen(host);
    char hostname[hostlen + 1];
    char *port = NULL;
    strcpy(hostname, host);
    char *p = hostname;
    while (p < hostname + hostlen) {
        if (*p == ':') {
            *p = 0;
            port = p + 1;
            break;
        }
        p++;
    }
    struct addrinfo *servinfo;
    if (getaddrinfo(hostname, port ? port: "8087", NULL, &servinfo)) {
        nc_admin_print("Wrong riak host");
        return 1;
    }
    int sock = socket(AF_INET , SOCK_STREAM , 0);
    if (sock == INVALID_SOCKET) {
        freeaddrinfo(servinfo);
        nc_admin_print("Failed to create socket");
        return INVALID_SOCKET;
    }
    if (connect(sock, servinfo->ai_addr, servinfo->ai_addrlen) < 0) {
        close(sock);
        freeaddrinfo(servinfo);
        nc_admin_print("Failed to connect");
        return INVALID_SOCKET;
    }
    freeaddrinfo(servinfo);
    return sock;
}

static uint8_t *
nc_admin_pack_buffer(riak_req_t type, const void *req, uint32_t *pack_size)
{
    pack_func func;
    switch (type) {
    case REQ_RIAK_PUT:
        *pack_size = (uint32_t)rpb_put_req__get_packed_size((RpbPutReq *)req);
        func = (pack_func)rpb_put_req__pack;
        break;
    case REQ_RIAK_GET:
        *pack_size = (uint32_t)rpb_get_req__get_packed_size((RpbGetReq *)req);
        func = (pack_func)rpb_get_req__pack;
        break;
    case REQ_RIAK_LIST_KEYS:
        *pack_size = (uint32_t)rpb_list_keys_req__get_packed_size((RpbListKeysReq *)req);
        func = (pack_func)rpb_list_keys_req__pack;
        break;
    case REQ_RIAK_LIST_BUCKETS:
        *pack_size = (uint32_t)rpb_list_buckets_req__get_packed_size((RpbListBucketsReq *)req);
        func = (pack_func)rpb_list_buckets_req__pack;
        break;
    default:
        nc_admin_print("Unknown riak request");
        return NULL;
    }
    uint32_t netlen = htonl(*pack_size + 1); // +1 for msg type
    *pack_size += (uint32_t)sizeof(netlen) + 1;
    uint8_t *pack_data = (uint8_t *)nc_zalloc(*pack_size);
    if (pack_data == NULL) {
        nc_admin_print("No memory");
        return NULL;
    }
    uint32_t *pack_data_netlen = (uint32_t *)pack_data;
    *pack_data_netlen = netlen;
    pack_data[sizeof(netlen)] = type;
    func(req, &pack_data[sizeof(netlen) + 1]);
    return pack_data;
}

static uint8_t *
nc_admin_riak_send(const char *host, riak_req_t type, const void *req,
                   uint32_t *len)
{
    ssize_t res;
    uint32_t netlen;
    int sock = nc_admin_connect(host);
    if (sock == INVALID_SOCKET) {
        return NULL;
    }
    uint32_t pack_size;
    uint8_t *pack_data =  nc_admin_pack_buffer(type, req, &pack_size);
    if (pack_data == NULL) {
        return NULL;
    }
    res = send(sock, pack_data, pack_size, 0);
    nc_free(pack_data);
    if(res < 0) {
        close(sock);
        nc_admin_print("Failed to send riak request");
        return NULL;
    }
    res = recv(sock, &netlen, 4 ,0);
    if(res < 0) {
        close(sock);
        nc_admin_print("Failed to recv riak response length");
        return NULL;
    }
    size_t body_len = ntohl(netlen);
    uint8_t *body = nc_zalloc(body_len);
    if (body == NULL) {
        close(sock);
        nc_admin_print("No memory");
        return NULL;
    }
    res = recv(sock, body, body_len, 0);
    if(res < 0) {
        close(sock);
        nc_admin_print("Failed to recv riak response");
        return NULL;
    }
    if(len) {
        *len = (uint32_t)body_len;
    }
    close(sock);
    return body;
}

static RpbGetResp *
nc_admin_get_request(const char *host, const char *bucket, const char *prop)
{
    RpbGetReq req = RPB_GET_REQ__INIT;
    req.has_type = 1;
    req.type.data = (uint8_t *)ADMIN_DATATYPE;
    req.type.len = ADMIN_DATATYPE_LEN;
    req.bucket.data = (uint8_t *)bucket;
    req.bucket.len = nc_strlen(bucket);
    req.key.data = (uint8_t *)prop;
    req.key.len = nc_strlen(prop);
    uint32_t len;
    uint8_t *rsp = nc_admin_riak_send(host, REQ_RIAK_GET, &req, &len);
    if (rsp == NULL) {
        return NULL;
    }
    RpbGetResp *rpbresp = rpb_get_resp__unpack(NULL, len - 1, rsp + 1);
    nc_free(rsp);
    return rpbresp;
}

static int
nc_admin_set_bucket_prop(const char *host, const char *bucket,
                         const char *prop, const char *value)
{
    RpbPutReq req = RPB_PUT_REQ__INIT;
    req.has_type = 1;
    req.type.data = (uint8_t *)ADMIN_DATATYPE;
    req.type.len = ADMIN_DATATYPE_LEN;
    req.bucket.data = (uint8_t *)bucket;
    req.bucket.len = nc_strlen(bucket);
    req.has_key = 1;
    req.key.data = (uint8_t *)prop;
    req.key.len = nc_strlen(prop);
    RpbContent content = RPB_CONTENT__INIT;
    req.content = &content;
    req.content[0].value.data = (uint8_t *)value;
    req.content[0].value.len = nc_strlen(value);
    req.content[0].has_content_type = 1;
    req.content[0].content_type.len = 10;
    req.content[0].content_type.data = (uint8_t*)"text/plain";

    /* read vclock before write */
    RpbGetResp *getresp = nc_admin_get_request(host, bucket, prop);
    if (getresp) {
        if (getresp->has_vclock == 0) {
            nc_free(getresp);
            getresp = NULL;
        }
    }
    if (getresp) {
        req.has_vclock = 1;
        req.vclock = getresp->vclock;
    }

    uint32_t len;
    uint8_t *rsp = nc_admin_riak_send(host, REQ_RIAK_PUT, &req, &len);
    nc_free(getresp);
    if (rsp == NULL) {
        return NC_ADMIN_ERROR;
    }
    uint8_t id = rsp[0];
    nc_free(rsp);
    if (id == RSP_RIAK_PUT) {
        nc_admin_print("OK");
        return NC_ADMIN_OK;
    }
    return NC_ADMIN_ERROR;
}

static int
nc_admin_get_bucket_prop(const char *host, const char *bucket,
                         const char *prop)
{
    uint32_t i;
    RpbGetResp *rpbresp = nc_admin_get_request(host, bucket, prop);
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
nc_admin_list_bucket_props(const char *host, const char *bucket)
{
    uint32_t i;
    RpbListKeysReq req = RPB_LIST_KEYS_REQ__INIT;
    req.has_type = 1;
    req.type.data = (uint8_t *)ADMIN_DATATYPE;
    req.type.len = ADMIN_DATATYPE_LEN;
    req.bucket.data = (uint8_t *)bucket;
    req.bucket.len = nc_strlen(bucket);
    uint32_t len;
    uint8_t *rsp = nc_admin_riak_send(host, REQ_RIAK_LIST_KEYS, &req, &len);
    if (rsp == NULL) {
        return NC_ADMIN_ERROR;
    }
    RpbListKeysResp *rpbresp = rpb_list_keys_resp__unpack(NULL, len - 1,
                                                          rsp + 1);
    nc_free(rsp);
    if (rpbresp) {
        if (rpbresp->n_keys == 0) {
            nc_admin_print("Not found");
        } else {
            for (i = 0; i < rpbresp->n_keys; i++) {
                nc_admin_print("%.*s", rpbresp->keys[i].len,
                               rpbresp->keys[i].data);
            }
        }
        nc_free(rpbresp);
        return NC_ADMIN_OK;
    }
    return NC_ADMIN_ERROR;
}

static int
nc_admin_list_buckets(const char *host)
{
    uint32_t i;
    RpbListBucketsReq req = RPB_LIST_BUCKETS_REQ__INIT;
    req.has_type = 1;
    req.type.data = (uint8_t *)ADMIN_DATATYPE;
    req.type.len = ADMIN_DATATYPE_LEN;
    uint32_t len;
    uint8_t *rsp = nc_admin_riak_send(host, REQ_RIAK_LIST_BUCKETS, &req, &len);
    if (rsp == NULL) {
        return NC_ADMIN_ERROR;
    }
    RpbListBucketsResp *rpbresp = rpb_list_buckets_resp__unpack(NULL, len - 1,
                                                                rsp + 1);
    nc_free(rsp);
    if (rpbresp) {
        if (rpbresp->n_buckets == 0) {
            nc_admin_print("Not found");
        } else {
            for (i = 0; i < rpbresp->n_buckets; i++) {
                nc_admin_print("%.*s", rpbresp->buckets[i].len,
                               rpbresp->buckets[i].data);
            }
        }
        nc_free(rpbresp);
        return NC_ADMIN_OK;
    }
    return NC_ADMIN_ERROR;
}

static int
nc_admin_check_args(int need, const char *arg1, const char *arg2,
                    const char *arg3)
{
    if (arg1 == NULL && need > 0) {
        log_stderr("Bucket name require");
        return NC_ADMIN_ERROR;
    }
    if (arg2 == NULL && need > 1) {
        log_stderr("Property name require");
        return NC_ADMIN_ERROR;
    }
    if (arg3 == NULL && need > 2) {
        log_stderr("Value require");
        return NC_ADMIN_ERROR;
    }
    if ((arg1 && need <= 0) || (arg2 && need <= 1) || (arg2 && need <= 1)) {
        log_stderr("Extra argument found");
        return NC_ADMIN_ERROR;
    }
    return NC_ADMIN_OK;
}

int
nc_admin_command(const char *host, const char *command,
                 const char *arg1, const char *arg2,
                 const char *arg3)
{
    if (nc_strcmp(command, "set-bucket-prop") == 0) {
        if(nc_admin_check_args(3, arg1, arg2, arg3)) {
            return NC_ADMIN_ERROR;
        }
        return nc_admin_set_bucket_prop(host, arg1, arg2, arg3);
    } else if (nc_strcmp(command, "get-bucket-prop") == 0) {
        if(nc_admin_check_args(2, arg1, arg2, arg3)) {
            return NC_ADMIN_ERROR;
        }
        return nc_admin_get_bucket_prop(host, arg1, arg2);
    } else if (nc_strcmp(command, "list-bucket-props") == 0) {
        if(nc_admin_check_args(1, arg1, arg2, arg3)) {
            return NC_ADMIN_ERROR;
        }
        return nc_admin_list_bucket_props(host, arg1);
    } else if (nc_strcmp(command, "list-buckets") == 0) {
        if(nc_admin_check_args(0, arg1, arg2, arg3)) {
            return NC_ADMIN_ERROR;
        }
        return nc_admin_list_buckets(host);
    } else {
        log_stderr(
            "Unknown command, available commands are:" CRLF
            "    set-bucket-prop bucket property value - set bucket property" CRLF
            "    get-bucket-prop bucker property - get property value" CRLF
            "    list-bucket-props bucket - list bucket properties" CRLF
            "    list-buckets - list existing buckets with properties" CRLF
            "");
        return NC_ADMIN_ERROR;
    }

    return NC_ADMIN_OK;
}
