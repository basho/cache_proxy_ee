#include <nc_core.h>
#include <nc_proto.h>

#include <nc_riak_private.h>

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

    ProtobufCBinaryData *datatype = &req.type;
    ProtobufCBinaryData *bucket = &req.bucket;
    ProtobufCBinaryData *key = &req.key;
    unsigned int value_num = 0;

    struct msg_pos keyname_start_pos = msg_pos_init();
    while (value_num < value_count) {
        status = extract_bucket_key_value(r,
                                          datatype, bucket, key,
                                          &values[value_num],
                                          &keyname_start_pos, true);
        if(status != NC_OK)
            break;

        if(bucket) {
            bucket = NULL;
            key = NULL;
            if (req.bucket.len <= 0) {
                // if no bucket specified, return it back to frontend
                free_request_ns_key(req);
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

        status = pack_message(r, type, dt_update_req__get_packed_size(&req), REQ_RIAK_DT_UPDATE, (pb_pack_func)dt_update_req__pack, &req, req.bucket.len);
    }
    if(status == NC_OK) {
        struct conn *c_conn = r->owner;
        struct context *ctx = conn_to_ctx(c_conn);
        if (req.type.len > 0) {
            add_pexpire_msg_key(ctx, c_conn, (char*)req.type.data,
                                req.type.len + req.bucket.len + req.key.len + 2, 0);
        } else {
            add_pexpire_msg_key(ctx, c_conn, (char*)req.bucket.data, req.bucket.len + req.key.len + 1, 0);
        }
        r->integer = value_num;
    }

    free_request_ns_key(req);
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

/**.......................................................................
 * Create request for fetching bucket:key from backend.
 * req should contain bucket and key name
 * result will be stored in struct msg* r
 */
rstatus_t
fetch_pb_req(DtFetchReq *req, struct msg* r, struct conn* s_conn, msg_type_t type)
{
    ASSERT(r != NULL);
    ASSERT(s_conn != NULL);

    rstatus_t status;

    struct msg_pos keyname_start_pos = msg_pos_init();

    msg_free_stored_arg(r);

    status = extract_bucket_key_value(r,
                                      &req->type, &req->bucket, &req->key,
                                      (type == MSG_REQ_RIAK_SISMEMBER) ? &r->stored_arg : NULL,
                                      &keyname_start_pos, false);
    if (status != NC_OK) {
        return status;
    }

    struct server* server = (struct server*)(s_conn->owner);
    const struct server_pool* pool = (struct server_pool*)(server->owner);
    const struct backend_opt* opt = &pool->backend_opt;

    req->has_r = (opt->riak_r != CONF_UNSET_NUM);
    if (req->has_r) {
        req->r = opt->riak_r;
    }

    req->has_pr = (opt->riak_pr != CONF_UNSET_NUM);
    if (req->has_pr) {
        req->pr = opt->riak_pr;
    }

    req->has_n_val = (opt->riak_n != CONF_UNSET_NUM);
    if (req->has_n_val) {
        req->n_val = opt->riak_n;
    }

    if (opt->riak_basic_quorum != CONF_UNSET_NUM) {
        req->has_basic_quorum = true;
        req->basic_quorum = opt->riak_basic_quorum;
    } else {
        req->has_basic_quorum = true;
        req->basic_quorum = 1;
    }

    req->has_sloppy_quorum = (opt->riak_sloppy_quorum != CONF_UNSET_NUM);
    if (req->has_sloppy_quorum) {
        req->sloppy_quorum = opt->riak_sloppy_quorum;
    }

    req->has_notfound_ok = (opt->riak_notfound_ok != CONF_UNSET_NUM);
    if (req->has_notfound_ok) {
        req->notfound_ok = opt->riak_notfound_ok;
    }

    req->has_timeout = (opt->riak_timeout != CONF_UNSET_NUM);
    if (req->has_timeout) {
        req->timeout = opt->riak_timeout;
    }

    return pack_message(r, type, dt_fetch_req__get_packed_size(req),
                        REQ_RIAK_DT_FETCH, (pb_pack_func)dt_fetch_req__pack,
                        req, req->bucket.len);
}

rstatus_t
encode_pb_smembers_req(struct msg* r, struct conn* s_conn, msg_type_t type)
{
    ASSERT(r != NULL);
    ASSERT(s_conn != NULL);

    rstatus_t status;

    DtFetchReq req = DT_FETCH_REQ__INIT;
    struct msg_pos keyname_start_pos = msg_pos_init();

    msg_free_stored_arg(r);

    status = extract_bucket_key_value(r, &req.type, &req.bucket, &req.key,
            (type == MSG_REQ_RIAK_SISMEMBER) ? &r->stored_arg : NULL,
            &keyname_start_pos, false);
    if (status != NC_OK) {
        return status;
    }

    status = fetch_pb_req(&req, r, s_conn, type);
    status = pack_message(r, type, dt_fetch_req__get_packed_size(&req),
                          REQ_RIAK_DT_FETCH, (pb_pack_func)dt_fetch_req__pack,
                          &req, req.bucket.len);

    free_request_ns_key(req);
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
             msg_type_t type)
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
    msg->type = type;

    if (TAILQ_EMPTY(&s_conn->imsg_q)) {
        event_add_out(ctx->evb, s_conn);
    }

    s_conn->enqueue_inq(ctx, s_conn, msg);
    s_conn->need_auth = 0;

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

/**.......................................................................
 * Handle backend fetch result.
 */
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
    struct conn *c_conn = pmsg->owner;
    struct context *ctx = conn_to_ctx(c_conn);
    struct server* server = r->owner->owner;
    struct server_pool* pool = (struct server_pool*)server->owner;

    // sync with frontend
    if(values_count) {
        DtFetchReq *req = NULL;

        parse_dt_fetch_req(pmsg, &len, &msgid, &req);
        if(req == NULL) {
            return NC_ERROR;
        }
        const uint32_t delimiter_count = ((req->type.len > 0) ? 1 : 0)
                                         + ((req->bucket.len > 0) ? 1 : 0);
 
        char key[req->type.len + req->bucket.len + req->key.len + delimiter_count + 1];
        uint32_t keylen = sprintf(key, "%.*s%s%.*s:%.*s",
                                  (uint32_t)req->type.len, req->type.data,
                                  (req->type.len > 0) ? ":" : "",
                                  (uint32_t)req->bucket.len, req->bucket.data,
                                  (uint32_t)req->key.len, req->key.data);
        ASSERT(keylen == sizeof(key) - 1);
        dt_fetch_req__free_unpacked(req, NULL);

        switch(pmsg->type) {
        case MSG_REQ_RIAK_SMEMBERS:
        case MSG_REQ_RIAK_SISMEMBER:
        case MSG_REQ_RIAK_SCARD:
            add_sadd_msg(ctx, c_conn, (uint8_t*)key, keylen, values, values_count, MSG_REQ_RIAK_SADD);
            add_pexpire_msg_key(ctx, c_conn, (char *)key, keylen, pool->server_ttl_ms);
            break;

        case MSG_REQ_RIAK_SDIFF:
        case MSG_REQ_RIAK_SINTER:
        case MSG_REQ_RIAK_SUNION:
        case MSG_REQ_RIAK_SDIFFSTORE:
        case MSG_REQ_RIAK_SINTERSTORE:
        case MSG_REQ_RIAK_SUNIONSTORE:
            add_sadd_msg(ctx, c_conn, (uint8_t*)key, keylen, values, values_count, MSG_REQ_HIDDEN);
            // TODO may be it have to be done after perforing command?
            add_pexpire_msg_key(ctx, c_conn, (char *)key, keylen, pool->server_ttl_ms);
            break;
        default:
            break;
        }
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
        uint32_t i;
        for(i = 0; i < values_count; i++) {
            if(values[i].len == pmsg->stored_arg.len) {
                if(strncmp((char*)values[i].data, (char*)pmsg->stored_arg.data,
                           pmsg->stored_arg.len) == 0) {
                    result = 1;
                    break;
                }
            }
        }
        msg_free_stored_arg(r);

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

    case MSG_REQ_RIAK_SDIFF:
    case MSG_REQ_RIAK_SINTER:
    case MSG_REQ_RIAK_SUNION:
    case MSG_REQ_RIAK_SDIFFSTORE:
    case MSG_REQ_RIAK_SINTERSTORE:
    case MSG_REQ_RIAK_SUNIONSTORE:
        // put something just to mark then me answered
        if ((status = msg_prepend_format(r, "+OK")) != NC_OK) {
            return status;
        }
        // check if all subcommands were sent
        struct msg *orgm = pmsg->frag_owner;
        ASSERT(orgm != NULL);
        orgm->integer--;
        if( orgm->integer == 0) {
            // if so, sent real command to perfrom it on frontend
            struct keypos *kpos = array_get(orgm->keys, 0);
            struct conn *s_conn = server_pool_conn_frontend(ctx, c_conn->owner,
                                                            kpos->start,
                                                            kpos->end - kpos->start,
                                                            NULL);
            // create new message for request, remapping store commands to simple
            char *ncline = NULL;
            struct msg *msg = NULL;
            switch (pmsg->type) {
            case MSG_REQ_RIAK_SDIFF:
            case MSG_REQ_RIAK_SINTER:
            case MSG_REQ_RIAK_SUNION:
                msg = msg_content_clone(orgm);
                if (msg == NULL) {
                    return NC_ENOMEM;
                }
                break;
            case MSG_REQ_RIAK_SDIFFSTORE:
                ncline = "\r\n$5\r\nsdiff\r\n";
                break;
            case MSG_REQ_RIAK_SINTERSTORE:
                ncline = "\r\n$6\r\nsinter\r\n";
                break;
            case MSG_REQ_RIAK_SUNIONSTORE:
                ncline = "\r\n$6\r\nsunion\r\n";
                break;
            default:
                NOT_REACHED();
            }
            if (ncline) {
                // remap to command without store
                struct msg_pos keyspos;
                char store[] = "store\r\n";
                struct msg_pos stpos;
                msg_pos_init_start(orgm, &stpos);
                msg_find_char(orgm, store, sizeof(store) - 1, &stpos, &keyspos);
                if(keyspos.result == MSG_NOTFOUND) {
                    return NC_ERROR;
                }
                msg_offset_from(&keyspos, sizeof(store) - 1, &keyspos);
                msg_find_char(orgm, "\r\n$", 3, &keyspos, &keyspos);
                if(keyspos.result == MSG_NOTFOUND) {
                    return NC_ERROR;
                }
                msg_offset_from(&keyspos, 2, &keyspos);

                msg = msg_get(orgm->owner, true);
                if (msg == NULL) {
                    c_conn->err = errno;
                    return NC_ENOMEM;
                }

                if ((status = msg_copy_char(msg, "*", 1)) != NC_OK) {
                    msg_put(msg);
                    return status;
                }

                char db[16];
                uint32_t dbl = sprintf(db, "%u", 1 + orgm->nsubs / 2);

                if ((status = msg_copy_char(msg, db, dbl)) != NC_OK) {
                    msg_put(msg);
                    return status;
                }

                if ((status = msg_copy_char(msg, ncline, strlen(ncline))) != NC_OK) {
                    msg_put(msg);
                    return status;
                }

                uint32_t klen;
                msg_offset_between(&stpos, &keyspos, &klen);
                klen = orgm->mlen - klen;

                msg_copy_from_pos(msg, &keyspos, klen);
            }

            msg->noreply = 0;
            msg->swallow = 1;
            msg->type = pmsg->type;

            if (TAILQ_EMPTY(&s_conn->imsg_q)) {
                event_add_out(ctx->evb, s_conn);
            }

            s_conn->enqueue_inq(ctx, s_conn, msg);
            s_conn->need_auth = 0;
        }
        break;

    default:
        break;
    }

    r->type = MSG_RSP_REDIS_STATUS;

    return NC_OK;
}


/*
 * Initiate request for key from backend to store the same key in frontend
 */
rstatus_t
riak_sync_key(struct context *ctx, struct msg* r, msg_type_t type, bool skip_first)
{
    ASSERT(r != NULL);
    ASSERT(r->owner != NULL);
    ASSERT(r->owner->owner != NULL);

    rstatus_t status;
    struct conn *c_conn = r->owner;
    struct msg_pos keyname_start_pos = msg_pos_init();

    DtFetchReq req = DT_FETCH_REQ__INIT;
    r->nsubs = 0;

    while ((status = extract_bucket_key_value(r, &req.type, &req.bucket, &req.key, NULL,
                                              &keyname_start_pos, true))
               == NC_OK) {
        if(skip_first) { // skip first if true
            skip_first = false;
            continue;
        }

        struct conn* s_conn = server_pool_conn_backend(ctx, c_conn->owner,
                                                       req.bucket.data,
                                                       req.bucket.len + req.key.len + 1,
                                                       NULL);

        struct msg* msg = msg_get(c_conn, true);
        if (msg == NULL) {
            c_conn->err = errno;
            return NC_ENOMEM;
        }
        struct mbuf* mbuf = mbuf_get();
        if (mbuf == NULL) {
            return NC_ENOMEM;
        }
        mbuf_insert(&msg->mhdr, mbuf);
        msg->pos = mbuf->pos;

        fetch_pb_req(&req, msg, s_conn, type);

        msg->swallow = 1;
        msg->frag_owner = r;
        r->nsubs++;

        if (TAILQ_EMPTY(&s_conn->imsg_q)) {
            event_add_out(ctx->evb, s_conn);
        }

        s_conn->enqueue_inq(ctx, s_conn, msg);
        s_conn->need_auth = 0;

        free_request_ns_key(req);
    }
    r->integer = r->nsubs;
    r->nsubs = r->nsubs * 2;

    return NC_OK;
}

/*
 * Store key in frontend and backend simultaneously
 */
rstatus_t
riak_synced_key(struct context *ctx, struct msg* pmsg, struct msg* amsg, uint32_t *keysfound)
{
    ASSERT(pmsg != NULL);
    ASSERT(amsg != NULL);

    // if no values present, just exit
    if(amsg->narg == 0)
        return NC_OK;

    // extract data
    DtUpdateReq req = DT_UPDATE_REQ__INIT;
    DtOp op = DT_OP__INIT;
    SetOp setop = SET_OP__INIT;
    ProtobufCBinaryData values[amsg->narg];
    uint32_t i;

    req.has_key = 1;
    req.op = &op;
    op.set_op = &setop;
    setop.n_adds = amsg->narg;
    setop.adds = values;

    rstatus_t status;

    struct msg_pos keyname_start_pos = msg_pos_init();

    status = extract_bucket_key_value(pmsg, &req.type, &req.bucket, &req.key, NULL,
            &keyname_start_pos, false);
    if (status != NC_OK) {
        return status;
    }

    struct msg_pos *keyname_start = NULL;

    for(i = 0; i < amsg->narg; i++) {
        if ((status = redis_get_next_string(amsg, keyname_start, &keyname_start_pos,
                                                    &values[i].len))
                    != NC_OK) {
            free_request_ns_key(req);
            if(i > 0) {
                uint32_t j;
                for(j = 0; j < i - 1; j++) {
                    nc_free(values[j].data);
                }
            }
            return status;
        }
        keyname_start = &keyname_start_pos;
        values[i].data = nc_alloc(values[i].len);

        if ((status = msg_extract_from_pos_char((char*)values[i].data,
                                                &keyname_start_pos, values[i].len))
            != NC_OK) {
            free_request_ns_key(req);
            if(i > 0) {
                uint32_t j;
                for(j = 0; j < i - 1; j++) {
                    nc_free(values[j].data);
                }
            }
            return status;
        }
    }

    struct conn *c_conn = pmsg->owner;
    struct server* server = amsg->owner->owner;
    struct server_pool* pool = (struct server_pool*)server->owner;

    // sync frontend
    add_sadd_msg(ctx, c_conn, req.bucket.data, req.bucket.len + req.key.len + 1,
                 values, amsg->narg, MSG_REQ_HIDDEN);
    if (req.type.len > 0) {
        add_pexpire_msg_key(ctx, c_conn, (char*)req.type.data,
                        req.type.len + req.bucket.len + req.key.len + 2, pool->server_ttl_ms);
    } else {
        add_pexpire_msg_key(ctx, c_conn, (char*)req.bucket.data,
                        req.bucket.len + req.key.len + 1, pool->server_ttl_ms);
    }

    // sync backend
    struct conn* s_conn = server_pool_conn_backend(ctx, c_conn->owner,
                                                   req.bucket.data,
                                                   req.bucket.len + req.key.len + 1,
                                                   NULL);
    struct msg* msg = msg_get(c_conn, true);
    if (msg) {
        struct mbuf* mbuf = mbuf_get();
        if (mbuf) {
            mbuf_insert(&msg->mhdr, mbuf);
            msg->pos = mbuf->pos;
            pack_message(msg, MSG_REQ_HIDDEN,
                         dt_update_req__get_packed_size(&req),
                         REQ_RIAK_DT_UPDATE, (pb_pack_func)dt_update_req__pack,
                         &req, req.bucket.len);
            msg->swallow = 1;
            msg->type = MSG_REQ_HIDDEN;

            if (TAILQ_EMPTY(&s_conn->imsg_q)) {
                event_add_out(ctx->evb, s_conn);
            }

            s_conn->enqueue_inq(ctx, s_conn, msg);
            s_conn->need_auth = 0;
        }
    }

    // cleanup
    for(i = 0; i < amsg->narg; i++) {
        nc_free(values[i].data);
    }
    free_request_ns_key(req);
    *keysfound = amsg->narg;
    return NC_OK;
}
