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

#include <nc_core.h>
#include <nc_log.h>
#include <nc_string.h>
#include <proto/nc_riak_private.h>
#include <proto/riak.pb-c.h>
#include <proto/riak_kv.pb-c.h>
#include <proto/riak_dt.pb-c.h>
#include <nc_admin_connection.h>
#include <nc_admin.h>

typedef size_t (*pack_func)(const void *message,uint8_t *out);

int
nc_admin_connection_resolve_connect(const char *host)
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
        log_debug(LOG_ERR, "Wrong riak host");
        return 1;
    }
    int sock = nc_admin_connection_connect(servinfo->ai_addr,
                                           servinfo->ai_addrlen);
    freeaddrinfo(servinfo);
    return sock;
}

int
nc_admin_connection_connect(const struct sockaddr *addr, socklen_t len)
{
    int sock = socket(AF_INET , SOCK_STREAM , 0);
    if (sock == INVALID_SOCKET) {
        log_debug(LOG_ERR, "Failed to create socket");
        return INVALID_SOCKET;
    }
    if (connect(sock, addr, len) < 0) {
        close(sock);
        log_debug(LOG_ERR, "Failed to connect");
        return INVALID_SOCKET;
    }
    return sock;
}

void
nc_admin_connection_disconnect(int sock)
{
    if (close(sock) != 0) {
        log_debug(LOG_ERR, "Couldn't close socket %d", sock);
    }
}

static uint8_t *
pack_buffer(riak_req_t type, const void *req, uint32_t *pack_size)
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
    case REQ_RIAK_DEL:
        *pack_size = (uint32_t)rpb_del_req__get_packed_size((RpbDelReq *)req);
        func = (pack_func)rpb_del_req__pack;
        break;
    case REQ_RIAK_DT_FETCH:
        *pack_size = (uint32_t)dt_fetch_req__get_packed_size((DtFetchReq *)req);
        func = (pack_func)dt_fetch_req__pack;
        break;
    case REQ_RIAK_DT_UPDATE:
        *pack_size = (uint32_t)dt_update_req__get_packed_size((DtUpdateReq *)req);
        func = (pack_func)dt_update_req__pack;
        break;
    case REQ_RIAK_COUNTER_GET:
        *pack_size = (uint32_t)rpb_counter_get_req__get_packed_size((RpbCounterGetReq *)req);
        func = (pack_func)rpb_counter_get_req__pack;
        break;
    case REQ_RIAK_COUNTER_UPDATE:
        *pack_size = (uint32_t)rpb_counter_update_req__get_packed_size((RpbCounterUpdateReq *)req);
        func = (pack_func)rpb_counter_update_req__pack;
        break;
    default:
        log_debug(LOG_ERR, "Unknown riak request");
        return NULL;
    }
    uint32_t netlen = htonl(*pack_size + 1); // +1 for msg type
    *pack_size += (uint32_t)sizeof(netlen) + 1;
    uint8_t *pack_data = (uint8_t *)nc_zalloc(*pack_size);
    if (pack_data == NULL) {
        log_debug(LOG_ERR, "No memory");
        return NULL;
    }
    uint32_t *pack_data_netlen = (uint32_t *)pack_data;
    *pack_data_netlen = netlen;
    pack_data[sizeof(netlen)] = type;
    func(req, &pack_data[sizeof(netlen) + 1]);
    return pack_data;
}

static uint8_t *
riak_send(int sock, riak_req_t type, const void *req,
          uint32_t *len)
{
    ssize_t res;
    uint32_t netlen;
    uint32_t pack_size;
    uint8_t *pack_data =  pack_buffer(type, req, &pack_size);
    if (pack_data == NULL) {
        return NULL;
    }
    res = send(sock, pack_data, pack_size, 0);
    nc_free(pack_data);
    if(res < 0) {
        log_debug(LOG_ERR, "Failed to send riak request");
        return NULL;
    }
    res = recv(sock, &netlen, 4 ,0);
    if(res < 0) {
        log_debug(LOG_ERR, "Failed to recv riak response length");
        return NULL;
    }
    size_t body_len = ntohl(netlen);
    uint8_t *body = nc_zalloc(body_len);
    if (body == NULL) {
        log_debug(LOG_ERR, "No memory");
        return NULL;
    }
    res = recv(sock, body, body_len, 0);
    if(res < 0) {
        log_debug(LOG_ERR, "Failed to recv riak response");
        return NULL;
    }
    if(len) {
        *len = (uint32_t)body_len;
    }
    return body;
}

static RpbGetResp *
get_request(int sock, const char *bucket, const char *prop)
{
    RpbGetReq req = RPB_GET_REQ__INIT;
    req.has_type = 1;
    req.type.data = (uint8_t *)RRA_DATATYPE;
    req.type.len = RRA_DATATYPE_LEN;
    req.bucket.data = (uint8_t *)bucket;
    req.bucket.len = nc_strlen(bucket);
    req.key.data = (uint8_t *)prop;
    req.key.len = nc_strlen(prop);
    uint32_t len;
    uint8_t *rsp = riak_send(sock, REQ_RIAK_GET, &req, &len);
    if (rsp == NULL) {
        return NULL;
    }
    RpbGetResp *rpbresp = rpb_get_resp__unpack(NULL, len - 1, rsp + 1);
    nc_free(rsp);
    return rpbresp;
}

bool
nc_admin_connection_set_bucket_prop(int sock, const char *bucket,
                                    const char *prop, const char *value)
{
    RpbPutReq preq = RPB_PUT_REQ__INIT;
    preq.has_type = 1;
    preq.type.data = (uint8_t *)RRA_DATATYPE;
    preq.type.len = RRA_DATATYPE_LEN;
    preq.bucket.data = (uint8_t *)bucket;
    preq.bucket.len = nc_strlen(bucket);
    preq.has_key = 1;
    preq.key.data = (uint8_t *)prop;
    preq.key.len = nc_strlen(prop);
    RpbContent content = RPB_CONTENT__INIT;
    preq.content = &content;
    preq.content[0].value.data = (uint8_t *)value;
    preq.content[0].value.len = nc_strlen(value);
    preq.content[0].has_content_type = 1;
    preq.content[0].content_type.len = 10;
    preq.content[0].content_type.data = (uint8_t*)"text/plain";

    /* read vclock before write */
    RpbGetResp *getresp = get_request(sock, bucket, prop);
    if (getresp) {
        if (getresp->has_vclock == 0) {
            nc_free(getresp);
            getresp = NULL;
        }
    }
    if (getresp) {
        preq.has_vclock = 1;
        preq.vclock = getresp->vclock;
    }

    /* write value */
    uint32_t len;
    uint8_t *prsp = riak_send(sock, REQ_RIAK_PUT, &preq, &len);
    if (getresp) {
        nc_free(getresp);
    }
    if (prsp == NULL) {
        return false;
    }
    uint8_t id = prsp[0];
    nc_free(prsp);
    if (id != RSP_RIAK_PUT) {
        return false;
    }

    /* update set with list of buckets */
    DtUpdateReq ureq = DT_UPDATE_REQ__INIT;
    DtOp uop = DT_OP__INIT;
    SetOp setop = SET_OP__INIT;
    ProtobufCBinaryData setvalue;
    ureq.has_key = 1;
    ureq.op = &uop;
    uop.set_op = &setop;
    setop.n_adds = 1;
    setop.adds = &setvalue;
    setvalue.data = (uint8_t *)bucket;
    setvalue.len = nc_strlen(bucket);
    ureq.type.data = (uint8_t *)RRA_SET_DATATYPE;
    ureq.type.len = RRA_SET_DATATYPE_LEN;
    ureq.bucket.data = (uint8_t *)RRA_SERVICE_BUCKET;
    ureq.bucket.len = RRA_SERVICE_BUCKET_LEN;
    ureq.has_key = 1;
    ureq.key.data = (uint8_t *)RRA_SERVICE_KEY;
    ureq.key.len = RRA_SERVICE_KEY_LEN;
    uint8_t *ursp = riak_send(sock, REQ_RIAK_DT_UPDATE, &ureq, &len);
    if (ursp == NULL) {
        return false;
    }
    id = ursp[0];
    nc_free(ursp);
    if (id != RSP_RIAK_DT_UPDATE) {
        return false;
    }

    /* update counter to mark change */
    DtUpdateReq creq = DT_UPDATE_REQ__INIT;
    DtOp cop = DT_OP__INIT;
    CounterOp counterop = COUNTER_OP__INIT;
    creq.has_key = 1;
    creq.op = &cop;
    cop.counter_op = &counterop;
    counterop.has_increment = 1;
    counterop.increment = 1;
    creq.type.data = (uint8_t *) RRA_COUNTER_DATATYPE;
    creq.type.len = RRA_COUNTER_DATATYPE_LEN;
    creq.bucket.data = (uint8_t *) RRA_SERVICE_BUCKET;
    creq.bucket.len = RRA_SERVICE_BUCKET_LEN;
    creq.has_key = 1;
    creq.key.data = (uint8_t *) RRA_SERVICE_KEY;
    creq.key.len = RRA_SERVICE_KEY_LEN;
    uint8_t *crsp = riak_send(sock, REQ_RIAK_DT_UPDATE, &creq, &len);
    if (crsp == NULL) {
        return false;
    }
    id = crsp[0];
    nc_free(crsp);
    if (id != RSP_RIAK_DT_UPDATE) {
        return false;
    }
    return true;
}

RpbGetResp *
nc_admin_connection_get_bucket_prop(int sock, const char *bucket,
                                    const char *prop)
{
    return get_request(sock, bucket, prop);
}

bool
nc_admin_connection_del_bucket(int sock, const char *bucket)
{
    uint32_t i = 0;
    while (ALLOWED_PROPERTIES[i][0]) {
        RpbDelReq req = RPB_DEL_REQ__INIT;
        req.has_type = 1;
        req.type.data = (uint8_t *)RRA_DATATYPE;
        req.type.len = RRA_DATATYPE_LEN;
        req.bucket.data = (uint8_t *)bucket;
        req.bucket.len = nc_strlen(bucket);
        req.key.data = (uint8_t *)ALLOWED_PROPERTIES[i];
        req.key.len = nc_strlen(ALLOWED_PROPERTIES[i]);
        uint32_t len;
        uint8_t *rsp = riak_send(sock, REQ_RIAK_DEL, &req, &len);
        if (rsp == NULL) {
            return false;
        }
        uint8_t id = rsp[0];
        nc_free(rsp);
        if (id != RSP_RIAK_DEL) {
            return false;
        }
        i++;
    }
    /* update set with list of buckets */
    DtUpdateReq ureq = DT_UPDATE_REQ__INIT;
    DtOp uop = DT_OP__INIT;
    SetOp setop = SET_OP__INIT;
    ProtobufCBinaryData setvalue;
    ureq.has_key = 1;
    ureq.op = &uop;
    uop.set_op = &setop;
    setop.n_removes = 1;
    setop.removes = &setvalue;
    setvalue.data = (uint8_t *)bucket;
    setvalue.len = nc_strlen(bucket);
    ureq.type.data = (uint8_t *)RRA_SET_DATATYPE;
    ureq.type.len = RRA_SET_DATATYPE_LEN;
    ureq.bucket.data = (uint8_t *)RRA_SERVICE_BUCKET;
    ureq.bucket.len = RRA_SERVICE_BUCKET_LEN;
    ureq.has_key = 1;
    ureq.key.data = (uint8_t *)RRA_SERVICE_KEY;
    ureq.key.len = RRA_SERVICE_KEY_LEN;
    uint32_t len;
    uint8_t *ursp = riak_send(sock, REQ_RIAK_DT_UPDATE, &ureq, &len);
    if (ursp == NULL) {
        return false;
    }
    uint8_t id = ursp[0];
    nc_free(ursp);
    if (id != RSP_RIAK_DT_UPDATE) {
        return false;
    }
    return true;
}

DtFetchResp *
nc_admin_connection_list_buckets(int sock)
{
    DtFetchReq req = DT_FETCH_REQ__INIT;
    req.type.data = (uint8_t *)RRA_SET_DATATYPE;
    req.type.len = RRA_SET_DATATYPE_LEN;
    req.bucket.data = (uint8_t*)RRA_SERVICE_BUCKET;
    req.bucket.len = RRA_SERVICE_BUCKET_LEN;
    req.key.data = (uint8_t*)RRA_SERVICE_KEY;
    req.key.len = RRA_SERVICE_KEY_LEN;
    uint32_t len;
    uint8_t *rsp = riak_send(sock, REQ_RIAK_DT_FETCH, &req, &len);
    if (rsp == NULL) {
        return NULL;
    }
    DtFetchResp *rpbresp = dt_fetch_resp__unpack(NULL, len - 1, rsp + 1);
    nc_free(rsp);
    return rpbresp;
}

bool
nc_admin_connection_get_counter(int sock, int64_t *val)
{
    DtFetchReq req = DT_FETCH_REQ__INIT;
    req.type.data = (uint8_t *) RRA_COUNTER_DATATYPE;
    req.type.len = RRA_COUNTER_DATATYPE_LEN;
    req.bucket.data = (uint8_t *)RRA_SERVICE_BUCKET;
    req.bucket.len = RRA_SERVICE_BUCKET_LEN;
    req.key.data = (uint8_t *)RRA_SERVICE_KEY;
    req.key.len = RRA_SERVICE_KEY_LEN;
    uint32_t len;
    uint8_t *rsp = riak_send(sock, REQ_RIAK_DT_FETCH, &req, &len);
    if (rsp == NULL || len == 0) {
        return false;
    }
    if (rsp[0] != RSP_RIAK_DT_FETCH) {
        return false;
    }
    DtFetchResp *rpbresp = dt_fetch_resp__unpack(NULL, len - 1, rsp + 1);
    nc_free(rsp);
    if (rpbresp == NULL) {
        return false;
    }
    if (rpbresp->value == NULL) {
        *val = 0;
        return true;
    }
    *val = rpbresp->value->has_counter_value ?
                      rpbresp->value->counter_value : 0;
    nc_free(rpbresp);
    return true;
}
