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
                                          &keyname_start_pos, false);
        if(status != NC_OK)
            break;
        if(bucket) {
            bucket = NULL;
            key = NULL;
            if (req.bucket.len <= 0) {
                // if no bucket specified, return it back to frontend
                nc_free(req.bucket.data);
                nc_free(req.key.data);
                nc_free(values[value_num].data);
                return NC_EAGAIN;
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

DtUpdateResp*
extract_dt_update_rsp(struct msg* r, uint32_t len, uint8_t* msgid)
{
    DtUpdateResp* dtresp;
    extract_rsp(r, len, msgid, (unpack_func)dt_update_resp__unpack,
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
