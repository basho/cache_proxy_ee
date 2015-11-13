#include <nc_core.h>
#include <nc_proto.h>

#include <nc_riak_private.h>

static const char DATA_TYPE_SETS[] = "sets";
typedef enum {
    SADD,
    SREM
} SetOpAction;

static rstatus_t
encode_pb_setop_req(struct msg* r, struct conn* s_conn, msg_type_t type, SetOpAction act)
{
    ASSERT(r != NULL);
    ASSERT(s_conn != NULL);

    // We should have command name, bucket:key and at least one value
    if(r->narg < 3) {
        return NC_ERROR;
    }
    const unsigned int value_count = r->narg - 2;

    rstatus_t status;

    DtUpdateReq req = DT_UPDATE_REQ__INIT;
    DtOp op = DT_OP__INIT;
    SetOp setop = SET_OP__INIT;
    ProtobufCBinaryData values[value_count];

    req.has_key = 1;
    req.op = &op;
    op.set_op = &setop;

    switch (act) {
    case SADD:
        setop.n_adds = value_count;
        setop.adds = values;
    break;
    case SREM:
        setop.n_removes = value_count;
        setop.removes = values;
    break;
    default:
        NOT_REACHED();
        return NC_ERROR;
    }

    ProtobufCBinaryData *bucket = &req.bucket;
    ProtobufCBinaryData *key = &req.key;
    unsigned int value_num = 0;

    struct msg_pos keyname_start_pos = msg_pos_init();
    while (value_num < value_count) {
        status = extract_bucket_key_value(r, bucket, key, &values[value_num],
                                          &keyname_start_pos, true);
        if(status != NC_OK)
            break;
        if(bucket) {
            bucket = NULL;
            key = NULL;
            if (req.bucket.len <= 0) {
                // if no bucket specified, return it back to frontend
                nc_free(req.bucket.data);
                nc_free(values[value_num].data);
                return NC_EBADREQ;
            }
        }
        value_num++;
    }

    if(status == NC_OK) {
        struct server* server = (struct server*)(s_conn->owner);
        const struct server_pool* pool = (struct server_pool*)(server->owner);
        const struct backend_opt* opt = &pool->backend_opt;

        req.has_w = (opt->riak_w != CONF_UNSET_NUM);
        if (req.has_w) {
            req.w = opt->riak_w;
        }

        req.has_pw = (opt->riak_pw != CONF_UNSET_NUM);
        if (req.has_pw) {
            req.pw = opt->riak_pw;
        }

        req.has_n_val = (opt->riak_n != CONF_UNSET_NUM);
        if (req.has_n_val) {
            req.n_val = opt->riak_n;
        }

        req.has_sloppy_quorum =
                (opt->riak_sloppy_quorum != CONF_UNSET_NUM);
        if (req.has_sloppy_quorum) {
            req.sloppy_quorum = opt->riak_sloppy_quorum;
        }

        req.type.data = (uint8_t *)DATA_TYPE_SETS;
        req.type.len = sizeof(DATA_TYPE_SETS) - 1;

        status = pack_message(r, type, dt_update_req__get_packed_size(&req), REQ_RIAK_DT_UPDATE, (pb_pack_func)dt_update_req__pack, &req, req.bucket.len);
    }
    if(status == NC_OK) {
        struct conn *c_conn = r->owner;
        struct context *ctx = conn_to_ctx(c_conn);
        add_pexpire_msg_key(ctx, c_conn, (char*)req.bucket.data, req.bucket.len + req.key.len + 1, 0);
        r->integer = value_num;
    }
    nc_free(req.bucket.data);
    unsigned int i;
    for(i = 0; i < value_num; i++) {
        nc_free(values[i].data);
    }
    return status;
}

/**.......................................................................
 * Take a Redis SADD request, and remap it to a PB message suitable for
 * sending to riak.
 */
rstatus_t
encode_pb_sadd_req(struct msg* r, struct conn* s_conn, msg_type_t type)
{
    return encode_pb_setop_req(r, s_conn, type, SADD);
}

/**.......................................................................
 * Take a Redis SREM request, and remap it to a PB message suitable for
 * sending to riak.
 */
rstatus_t
encode_pb_srem_req(struct msg* r, struct conn* s_conn, msg_type_t type)
{
    return encode_pb_setop_req(r, s_conn, type, SREM);
}

rstatus_t
encode_pb_smembers_req(struct msg* r, struct conn* s_conn, msg_type_t type)
{
    ASSERT(r != NULL);
        ASSERT(s_conn != NULL);

        rstatus_t status;

        DtFetchReq req = DT_FETCH_REQ__INIT;
        struct msg_pos keyname_start_pos = msg_pos_init();

        if ((status = extract_bucket_key_value(r, &req.bucket, &req.key, NULL,
                                               &keyname_start_pos, false))
            != NC_OK) {
            return status;
        }

        struct server* server = (struct server*)(s_conn->owner);
        const struct server_pool* pool = (struct server_pool*)(server->owner);
        const struct backend_opt* opt = &pool->backend_opt;

        req.has_r = (opt->riak_r != CONF_UNSET_NUM);
        if (req.has_r) {
            req.r = opt->riak_r;
        }

        req.has_pr = (opt->riak_pr != CONF_UNSET_NUM);
        if (req.has_pr) {
            req.pr = opt->riak_pr;
        }

        req.has_n_val = (opt->riak_n != CONF_UNSET_NUM);
        if (req.has_n_val) {
            req.n_val = opt->riak_n;
        }

        if (opt->riak_basic_quorum != CONF_UNSET_NUM) {
            req.has_basic_quorum = true;
            req.basic_quorum = opt->riak_basic_quorum;
        } else {
            req.has_basic_quorum = true;
            req.basic_quorum = 1;
        }

        req.has_sloppy_quorum =
                (opt->riak_sloppy_quorum != CONF_UNSET_NUM);
        if (req.has_sloppy_quorum) {
            req.sloppy_quorum = opt->riak_sloppy_quorum;
        }

        req.has_notfound_ok = (opt->riak_notfound_ok != CONF_UNSET_NUM);
        if (req.has_notfound_ok) {
            req.notfound_ok = opt->riak_notfound_ok;
        }

        req.has_timeout = (opt->riak_timeout != CONF_UNSET_NUM);
        if (req.has_timeout) {
            req.timeout = opt->riak_timeout;
        }

        req.type.data = (uint8_t *)DATA_TYPE_SETS;
        req.type.len = sizeof(DATA_TYPE_SETS) - 1;

        status = pack_message(r, type, dt_fetch_req__get_packed_size(&req), REQ_RIAK_DT_FETCH, (pb_pack_func)dt_fetch_req__pack, &req, req.bucket.len);
        nc_free(req.bucket.data);

        return status;
}

rstatus_t
encode_pb_sismember_req(struct msg* r, struct conn* s_conn, msg_type_t type)
{
    return encode_pb_smembers_req(r, s_conn, type);
}

rstatus_t
encode_pb_scard_req(struct msg* r, struct conn* s_conn, msg_type_t type)
{
    return encode_pb_smembers_req(r, s_conn, type);
}

DtUpdateResp*
extract_dt_update_rsp(struct msg* r, uint32_t len, uint8_t* msgid)
{
    DtUpdateResp* dtresp;
    extract_rsp(r, len, msgid, (unpack_func)dt_update_resp__unpack,
                (void*)&dtresp);
    return dtresp;
}

DtFetchResp*
extract_dt_fetch_rsp(struct msg* r, uint32_t len, uint8_t* msgid)
{
    DtFetchResp* dtresp;
    extract_rsp(r, len, msgid, (unpack_func)dt_fetch_resp__unpack,
                (void*)&dtresp);
    return dtresp;
}

rstatus_t
repack_dt_update_resp(struct msg* r, DtUpdateResp* dtresp)
{
    ASSERT(r != NULL);
    ASSERT(dtresp != NULL);

    rstatus_t status = NC_OK;

    msg_rewind(r);
    r->mlen = 0;

    // TODO find a better way to get result
    struct msg* pmsg = TAILQ_FIRST(&r->owner->omsg_q);
    const size_t total = pmsg->integer;

    if ((status = msg_prepend_format(r, ":%d\r\n", total)) != NC_OK) {
        return status;
    }

    r->type = MSG_RSP_REDIS_STATUS;

    return NC_OK;
}

/**.......................................................................
 * Function to copy CRDT values to message
 */
rstatus_t
copy_values_to_msg(ProtobufCBinaryData *values, uint32_t nval, struct msg *msg)
{
    rstatus_t status;
    uint32_t i;
    char lenbuf[32]; // this size should be enough for any 32 bits digits
    for(i = 0; i < nval; i++) {
        uint32_t lenbuf_len = sprintf(lenbuf, "$%u\r\n", (uint32_t)values[i].len);
        if ((status = msg_copy_char(msg, lenbuf, lenbuf_len))
            != NC_OK) {
            msg_put(msg);
            return status;
        }

        if ((status = msg_copy_char(msg, (char*)values[i].data, values[i].len))
            != NC_OK) {
            msg_put(msg);
            return status;
        }

        if ((status = msg_copy_char(msg, "\r\n",2)) != NC_OK) {
            msg_put(msg);
            return status;
        }
    }
    return NC_OK;
}

/**.......................................................................
 * Function to add a SADD message to the frontend server's queue,
 * with explicit members from DtFetchResp (values and nval)
 */
rstatus_t
add_sadd_msg(struct context *ctx, struct conn *c_conn, uint8_t *keyname,
             uint32_t keynamelen, ProtobufCBinaryData *values, uint32_t nval,
             uint32_t time)
{
    ASSERT(nval != 0);

    const char sadd_begin_proto[] = "*%u\r\n$4\r\nsadd\r\n$%u\r\n%.*s\r\n";
    rstatus_t status;
    struct conn* s_conn = server_pool_conn_frontend(ctx, c_conn->owner,
                                                    (uint8_t*)keyname,
                                                    keynamelen,
                                                    NULL);

    char sadd_begin[sizeof(sadd_begin_proto) - 8 + ndig(2 + nval)
                    + ndig(keynamelen) + keynamelen];
    const uint32_t sadd_begin_len = (uint32_t)sprintf(sadd_begin,
                                                      sadd_begin_proto,
                                                      2 + nval, keynamelen,
                                                      keynamelen, keyname);
    ASSERT(sadd_begin_len == sizeof(sadd_begin) - 1);

    struct msg* msg = msg_get(c_conn, true);
    if (msg == NULL) {
        c_conn->err = errno;
        return NC_ENOMEM;
    }

    if ((status = msg_copy_char(msg, sadd_begin, sizeof(sadd_begin) - 1))
        != NC_OK) {
        msg_put(msg);
        return status;
    }

    if ((status = copy_values_to_msg(values, nval, msg))
        != NC_OK) {
        msg_put(msg);
        return status;
    }

    msg->swallow = 1;

    if (TAILQ_EMPTY(&s_conn->imsg_q)) {
        event_add_out(ctx->evb, s_conn);
    }

    s_conn->enqueue_inq(ctx, s_conn, msg);
    s_conn->need_auth = 0;

    add_pexpire_msg_key(ctx, c_conn, (char *)keyname, keynamelen, time);

    return NC_OK;
}

/**.......................................................................
 * Parse a protobuf-encoded DT_FETCH request into a request structure
 */
void
parse_dt_fetch_req(struct msg *r, uint32_t* len, uint8_t* msgid, DtFetchReq** req)
{
    ASSERT(r != NULL);
    ASSERT(len != NULL);
    ASSERT(msgid != NULL);
    ASSERT(req != NULL);

    struct mbuf* mbuf = STAILQ_FIRST(&r->mhdr);

    get_pb_msglen(r, len, msgid);

    /* Skip the message length */

    uint8_t* pos = mbuf->start + 4 + 1;

    *req = dt_fetch_req__unpack(NULL, *len - 1, pos);
}

rstatus_t
repack_dt_fetch_resp(struct msg* r, DtFetchResp* dtresp)
{
    ASSERT(r != NULL);
    ASSERT(dtresp != NULL);

    msg_rewind(r);
    r->mlen = 0;

    rstatus_t status = NC_OK;
    // dtresp->value is NULL if nothing is stored
    uint32_t values_count = dtresp->value ? dtresp->value->n_set_value : 0;
    ProtobufCBinaryData *values = dtresp->value ? dtresp->value->set_value
                                                : NULL;

    struct msg* pmsg = TAILQ_FIRST(&r->owner->omsg_q);
    uint8_t msgid;
    uint32_t len;

    // sync with frontend
    if(values_count) {
        struct conn *c_conn = pmsg->owner;
        struct context *ctx = conn_to_ctx(c_conn);
        DtFetchReq *req = NULL;

        parse_dt_fetch_req(pmsg, &len, &msgid, &req);
        if(req == NULL) {
            return NC_ERROR;
        }
        char key[req->key.len + req->bucket.len + 2];
        uint32_t keylen = sprintf(key, "%.*s:%.*s", (uint32_t)req->bucket.len,
                                  req->bucket.data, (uint32_t)req->key.len,
                                  req->key.data);
        ASSERT(keylen == sizeof(key) - 1);
        dt_fetch_req__free_unpacked(req, NULL);

        struct server* server = r->owner->owner;
        struct server_pool* pool = (struct server_pool*)server->owner;
        add_sadd_msg(ctx, c_conn, (uint8_t*)key, keylen, values,
                     values_count, pool->server_ttl_ms);
    }


    // choose which was a command and response on it
    switch(pmsg->type) {

    case MSG_REQ_RIAK_SMEMBERS:
        if ((status = msg_prepend_format(r, "*%u\r\n", values_count))
            != NC_OK) {
            return status;
        }
        if ((status = copy_values_to_msg(values, values_count, r))
            != NC_OK) {
            return status;
        }

        break;

    case MSG_REQ_RIAK_SISMEMBER:
    {
        uint32_t result = 0;
// TODO pass value somehow
//        uint32_t i;
//        for(i = 0; i < dtresp->value->n_set_value; i++) {
//            if(dtresp->value->set_value[i].len == value.len) {
//                if(strncmp((char*)dtresp->value->set_value[i].data,
//                           (char*)value.data, value.len) == 0) {
//                    result = 1;
//                    break;
//                }
//            }
//        }
//
//        nc_free(value.data);

        if ((status = msg_prepend_format(r, ":%d\r\n", result)) != NC_OK) {
            return status;
        }
    }
        break;

    case MSG_REQ_RIAK_SCARD:
        if ((status = msg_prepend_format(r, ":%d\r\n", values_count)) != NC_OK) {
            return status;
        }
        break;

    default:
        break;
    }

    r->type = MSG_RSP_REDIS_STATUS;

    return NC_OK;
}
