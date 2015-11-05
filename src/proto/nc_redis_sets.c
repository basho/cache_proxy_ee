#include <nc_core.h>
#include <nc_proto.h>

#include <nc_riak.h>

static const char DATA_TYPE_SETS[] = "sets";

/**.......................................................................
 * Take a Redis SADD request, and remap it to a PB message suitable for
 * sending to riak.
 */
rstatus_t
encode_pb_sadd_req(struct msg* r, struct conn* s_conn, msg_type_t type)
{
    ASSERT(r != NULL);
    ASSERT(s_conn != NULL);

    rstatus_t status;

    DtUpdateReq req = DT_UPDATE_REQ__INIT;
    DtOp op = DT_OP__INIT;
    SetOp setop = SET_OP__INIT;
    ProtobufCBinaryData value;

    req.op = &op;
    op.set_op = &setop;
    setop.n_adds = 1;
    setop.adds = &value;

    req.has_key = 1;
    req.has_n_val = 1;
    req.n_val = 1;

    struct msg_pos keyname_start_pos = msg_pos_init();
    if ((status = extract_bucket_key_value(r, &req.bucket, &req.key,
                    &value, &keyname_start_pos, false))
            != NC_OK) {
        return status;
    }

    if (req.bucket.len <= 0) {
        return NC_EAGAIN;
    }

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

    status = pack_message(r, type, dt_update_req__get_packed_size(&req), REQ_RIAK_DTUPDATE, (pb_pack_func)dt_update_req__pack, &req, req.bucket.len);
    nc_free(req.bucket.data);
    return status;
}
