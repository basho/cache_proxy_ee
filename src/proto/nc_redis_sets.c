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

    RpbPutReq req = RPB_PUT_REQ__INIT;
    //SetOp prereq = SET_OP__INIT;

    //prereq.

    //req.has_key = 1;

    RpbContent content = RPB_CONTENT__INIT;
    req.content = &content;

    struct msg_pos keyname_start_pos = msg_pos_init();
    if ((status = extract_bucket_key_value(r, &req.bucket, &req.key,
                    &req.content[0].value, &keyname_start_pos, false))
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

    if (req.content != NULL) {
        req.content[0].has_content_type = (protobuf_c_boolean)1;
        req.content[0].content_type.len = 10; /*<< strlen("text/plain")*/
        req.content[0].content_type.data = (uint8_t*)"text/plain";
    }

    /* Set the vclock, otherwise causing "sibling explosion" */
    if (r->has_vclock && r->vclock.len > 0) {
        req.has_vclock = (protobuf_c_boolean)1;
        req.vclock = r->vclock;
    }

    req.has_type = 1;
    req.type.data = (uint8_t *)DATA_TYPE_SETS;
    req.type.len = sizeof(DATA_TYPE_SETS) - 1;

    status = pack_message(r, type, rpb_put_req__get_packed_size(&req), REQ_RIAK_PUT, (pb_pack_func)rpb_put_req__pack, &req, req.bucket.len);
    nc_free(req.bucket.data);
    nc_free(req.content->value.data);
    return status;
}
