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

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/ioctl.h>

#include <nc_core.h>
#include <nc_proto.h>
#include <nc_riak_private.h>


/**.......................................................................
 * Stub to parse a request received from a Riak server.  We don't
 * currently support requests from Riak servers, so should never get
 * here.
 */
void
riak_parse_req(struct msg *r)
{
    abort();
}

/**.......................................................................
 * Parse a response received from a Riak server
 */
void
riak_parse_rsp(struct msg *r)
{
    ASSERT(r != NULL);

    r->result = MSG_PARSE_OK;

    /*
     * Do we have enough information to parse the message length?  If
     * not, try again later
     */

    uint32_t len = 0;
    uint8_t msgid = RSP_RIAK_UNKNOWN;

    if (!get_pb_msglen(r, &len, &msgid)) {
        r->result = MSG_PARSE_AGAIN;
        return;
    }

    /* 
     * If the mbuf len is less than message len + sizeof(len), we are
     * not done reading
     */

    if (r->mlen < len + 4) {
        r->result = MSG_PARSE_AGAIN;
        return;
    }

    /*
     * On return from this method, the message position should point to
     * the beginning of any unparsed data.  This is used in msg_parsed()
     * to determine if the mbuf should be split i.e., if more than one
     * message is encoded in the same set of mbufs)
     */

    struct msg_pos msg_start = msg_pos_init();
    msg_pos_init_start(r, &msg_start);

    struct msg_pos msg_end = msg_pos_init();
    msg_offset_from(&msg_start, len + 4, &msg_end);

    r->pos = msg_end.ptr;

    /*
     * If we get here, we are done reading at least one message --
     * use the msgid to determine what the message is, and extract the
     * PB-formatted response from the message buffer
     */
    switch (msgid) {
    case RSP_RIAK_GET:
        /*
         * if you really need to do something here, just use sample below
         * {
         *    RpbGetResp* rpbresp = extract_get_rsp(r, len, &msgid);
         *    if (rpbresp == NULL) {
         *        r->result = MSG_PARSE_ERROR;
         *    } else {
         *        rpb_get_resp__free_unpacked(rpbresp);
         *    }
         * }
         * break;
         */
    case RSP_RIAK_PUT:
    case RSP_RIAK_DT_UPDATE:
    case RSP_RIAK_DT_FETCH:
    case RSP_RIAK_DEL:
        /* DEL doesn't have response */
        break;
    case RSP_RIAK_UNKNOWN:
    {
        // While removing non-existent values from we will have msgid equal 0
        struct msg* pmsg = TAILQ_FIRST(&r->owner->omsg_q);
        if (pmsg->type == MSG_REQ_RIAK_SREM) {
            break;
        }
    }
        /* no break */
    default:
        r->result = MSG_PARSE_ERROR;
        break;
    }
}

/**.......................................................................
 * Return the length of the PB encoded version of this mbuf
 */
bool
get_pb_mbuflen(struct mbuf* mbuf, uint32_t* len, uint8_t* msgid)
{
    ASSERT(mbuf != NULL);
    ASSERT(len != NULL);
    /* TODO: determine why mbuf_length() which is based on mbuf->pos is not used here. */
    ASSERT(mbuf->last >= mbuf->start);
    uint32_t mlen = mbuf->last - mbuf->start;

    size_t len_size = sizeof(*len);
    size_t hdr_size = len_size + 1;
    uint32_t netlen;
    uint8_t* netlenptr = (uint8_t*)&netlen;

    if (mlen >= hdr_size) {
        unsigned i = 0;
        for (i = 0; i < len_size; i++) {
            *(netlenptr + i) = *(mbuf->start + i);
        }

        *len = ntohl(netlen);

        *msgid = *(mbuf->start + len_size);

        return true;
    }

    return false;
}

/**.......................................................................
 * Return the length of the PB encoded version of this message
 */
bool
get_pb_msglen(struct msg* r, uint32_t* len, uint8_t* msgid)
{
    ASSERT(r != NULL);
    ASSERT(len != NULL);

    return get_pb_mbuflen(STAILQ_FIRST(&r->mhdr), len, msgid);
}

/*
 * TODO: implement any of these
 */

rstatus_t
riak_add_auth_packet(struct context *ctx, struct conn *c_conn,
                     struct conn *s_conn)
{
    return NC_ERROR;
}

rstatus_t
riak_fragment(struct msg *r, uint32_t ncontinuum, struct msg_tqh *frag_msgq)
{
    return NC_ERROR;
}

void
riak_pre_coalesce(struct msg *r)
{
    struct msg *pr = r->peer; /* peer request */
    struct mbuf *mbuf;

    ASSERT(!r->request);
    ASSERT(pr->request);

    if (pr->frag_id == 0) {
        /* do nothing, if not a response to a fragmented request */
        return;
    }
    pr->frag_owner->nfrag_done++;

    switch (r->type) {
    case MSG_RSP_RIAK_INTEGER:
        /* only redis 'del' fragmented request sends back integer reply */
        ASSERT(pr->type == MSG_REQ_RIAK_DEL);

        mbuf = STAILQ_FIRST(&r->mhdr);
        /*
         * Our response parser guarantees that the integer reply will be
         * completely encapsulated in a single mbuf and we should skip over
         * all the mbuf contents and discard it as the parser has already
         * parsed the integer reply and stored it in msg->integer
         */
        ASSERT(mbuf == STAILQ_LAST(&r->mhdr, mbuf, next));
        ASSERT(r->mlen == mbuf_length(mbuf));

        r->mlen -= mbuf_length(mbuf);
        mbuf_rewind(mbuf);

        pr->frag_owner->integer = pr->integer;
        break;
    default:
        /*
         * Valid responses for a fragmented request are MSG_RSP_REDIS_INTEGER or,
         * MSG_RSP_REDIS_MULTIBULK. For an invalid response, we send out -ERR
         * with EINVAL errno
         */
        mbuf = STAILQ_FIRST(&r->mhdr);
        log_hexdump(LOG_ERR, mbuf->pos, mbuf_length(mbuf), "rsp fragment "
                    "with unknown type %d",
                    r->type);
        pr->error = 1;
        pr->err = EINVAL;
        break;
    }
}

void
riak_post_coalesce(struct msg *r)
{
    log_debug(LOG_NOTICE, "riak_post_coalesce() fired");
}

void
riak_post_connect(struct context *ctx, struct conn *conn, struct server *server)
{
    ASSERT(conn != NULL);
    ASSERT(server != NULL);

    ASSERT(!conn->client && conn->connected);
    ASSERT_(conn->type == CONN_RIAK,
            "expected conn->type == CONN_RIAK, but it is %d", conn->type);

    log_debug(LOG_NOTICE, "fired postconnect hook for a conn %d to riak.",
              conn->sd);
}

void
riak_swallow_msg(struct conn *conn, struct msg *pmsg, struct msg *msg)
{
    ASSERT(conn != NULL);
    if (msg->done) {
        return;
    }

    /* No-op; redis uses this to handle failed selects, we just log that
     * _something_ happened to a riak message
     */
    struct server* conn_server = 0;
    struct server_pool* conn_pool = 0;

    conn_server = (struct server*)conn->owner;
    conn_pool = conn_server->owner;

    log_warn("Riak msg id %d swallowed on %s | %s", msg->id,
             conn_pool->name.data, conn_server->name.data);
}

struct msg*
riak_rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    return rsp_recv_next(ctx, conn, alloc);
}

/**.......................................................................
 * Called when a request has been received, before sending the request
 * to a server
 */
struct msg*
riak_req_send_next(struct context *ctx, struct conn *conn)
{
    return req_send_next(ctx, conn);
}

/**.......................................................................
 * Remap Redis requests to the corresponding Riak requests
 *
 * Returns NC_OK if the message was successfully processed (remapped or
 * ignored)
 */
rstatus_t
riak_req_remap(struct conn* conn, struct msg* msg)
{
    ASSERT(msg != NULL);
    ASSERT(conn != NULL);
    /* related pre- or post-message */
    struct msg *msgp = NULL;
    rstatus_t status = NC_OK;

    switch (msg->type) {
    case MSG_REQ_RIAK_GET:
        break;

    case MSG_REQ_REDIS_GET:
        if ((status = encode_pb_get_req(msg, conn, MSG_REQ_RIAK_GET)) != NC_OK) {
            return status;
        }

        break;

    case MSG_REQ_REDIS_SET:
        if (msg->has_vclock) {
            if ((status = encode_pb_put_req(msg, conn, MSG_REQ_RIAK_SET)) != NC_OK) {
                return status;
            }
        } else {
            /* currently there is no configuration settings that would cause
             * no read_before_write, but there may be, ie for bucket patterns
             * or for all requests since Riak can be configured in a
             * last-write-wins (LWW) manner.
             *
             * read_before_write to get the vclock to avoid "sibling explosion".
             * cloning the SET since it will yield the equivalent key for a GET.
             */
            msgp = msg_content_clone(msg);
            if (msgp == NULL) {
                return NC_ENOMEM;
            }
            msgp->parser(msgp);

            /* PUT as a post message, so the vclock from the read-before-write
             * msg can be set on recv of the GET response.
             * do NOT encode PUT yet, will do when we have a vclock.
             */

            if ((status = _encode_pb_get_req(msg, conn, MSG_REQ_RIAK_GET, 1)) != NC_OK) {
                return status;
            }

            array_set(msg->msgs_post, msgp, sizeof(msgp), 1);
            array_push(msg->msgs_post);
        }
    break;

    case MSG_REQ_REDIS_DEL:
        if ((status = encode_pb_del_req(msg, conn, MSG_REQ_RIAK_DEL)) != NC_OK) {
            return status;
        }
        break;

    case MSG_REQ_REDIS_SADD:
        if ((status = encode_pb_sadd_req(msg, conn, MSG_REQ_RIAK_SADD)) != NC_OK) {
            return status;
        }
    	break;

    case MSG_REQ_REDIS_SREM:
        if ((status = encode_pb_srem_req(msg, conn, MSG_REQ_RIAK_SREM)) != NC_OK) {
            return status;
        }
        break;

    case MSG_REQ_REDIS_SMEMBERS:
        if ((status = encode_pb_smembers_req(msg, conn, MSG_REQ_RIAK_SMEMBERS)) != NC_OK) {
            return status;
        }
        break;

    case MSG_REQ_REDIS_SISMEMBER:
        if ((status = encode_pb_sismember_req(msg, conn, MSG_REQ_RIAK_SISMEMBER)) != NC_OK) {
            return status;
        }
        break;

    case MSG_REQ_REDIS_SCARD:
        if ((status = encode_pb_scard_req(msg, conn, MSG_REQ_RIAK_SCARD)) != NC_OK) {
            return status;
        }
        break;

    default:
        return NC_ERROR;
    }

    return NC_OK;
}

/**.......................................................................
 * Extract type, bucket, key and value form Redis command.
 * Data in either type.data, bucket.data, or key.data as well as value.data
 * should be freed with nc_free(req.type.data)
 */
rstatus_t
extract_bucket_key_value(struct msg *r,
                         ProtobufCBinaryData *datatype,
                         ProtobufCBinaryData *bucket,
                         ProtobufCBinaryData *key,
                         ProtobufCBinaryData *value,
                         struct msg_pos *keyname_start_pos,
                         bool allow_empty_bucket)
{
    size_t keynamelen = 0;
    uint8_t* data = NULL;

    rstatus_t status = NC_OK;
    if (keyname_start_pos->ptr == NULL) {
        /* skip command label */
        if ((status = redis_get_next_string(r, NULL, keyname_start_pos,
                                            &keynamelen))
            != NC_OK) {
            return status;
        }
    }

    if(key) {
        /* may be in format:
         * key
         * bucket:key
         * datatype:bucket:key
         * */
        if ((status = redis_get_next_string(r,
                                            keyname_start_pos, keyname_start_pos,
                                            &keynamelen))
                != NC_OK) {
            return status;
        }

        if (keynamelen == 0) {
            return NC_ERROR;
        }

        data = nc_alloc(keynamelen + 1);
        if (data == NULL) {
            return NC_ENOMEM;
        }
        data[keynamelen] = 0;
        if ((status = msg_extract_from_pos_char((char *)data,
                        keyname_start_pos, keynamelen))
                != NC_OK) {
            nc_free(data);
        }

        /* extract datatype, bucket and key from line */
        nc_split_key_string(data, keynamelen, datatype, bucket, key);

        if (!allow_empty_bucket && bucket->len <= 0) {
            return NC_EBADREQ;
        }
    }

    if (value != NULL) {
        if ((status = redis_get_next_string(r,
                                            keyname_start_pos, keyname_start_pos,
                                            &value->len))
                != NC_OK) {
            nc_free(data);
            return status;
        }
        value->data = nc_alloc(value->len + 1);
        if (value->data == NULL) {
            nc_free(data);
            return NC_ENOMEM;
        }
        if ((status = msg_extract_from_pos_char((char*)value->data,
                                                 keyname_start_pos, value->len))
                != NC_OK) {
            nc_free(data);
            return status;
        }
    }

    return NC_OK;
}

/**.......................................................................
 * Pack the message into our mbufs
 */
rstatus_t
pack_message(struct msg *r, msg_type_t type, uint32_t msglen, uint8_t reqid,
             pb_pack_func func, const void *message, uint32_t bucketlen)
{
    rstatus_t status;
    uint32_t netlen = htonl(msglen + 1);

    /*
     * Construct the length of the whole message we will send to
     * riak. This is:
     *
     *   size of the msglen integer
     * + a byte for the message type
     * + the length of the pb-encoded message
     */

    uint32_t pbmsglen = sizeof(msglen) + 1 + msglen;

    /*
     * Insert the message length, request ID and the data payload.
     *
     * If the first message mbuf has enough room to contain the message,
     * just reuse it.  Else we have to allocate a contiguous buffer
     * large enough to format the PB request before packing it into the
     * message
     */

    struct mbuf* mbuf = STAILQ_FIRST(&r->mhdr);
    if (pbmsglen <= mbuf_size(mbuf)) {
        mbuf_rewind(mbuf);

        mbuf_copy(mbuf, (uint8_t*)&netlen, sizeof(netlen));
        mbuf_copy(mbuf, &reqid, 1);
        func(message, mbuf->last);

        mbuf->last += msglen;
    } else {
        uint8_t* buf = nc_alloc(msglen);

        func(message, buf);

        msg_rewind(r);

        if ((status = msg_copy(r, (uint8_t*)&netlen, sizeof(netlen))) != NC_OK) {
            nc_free(buf);
            return status;
        }

        if ((status = msg_copy(r, &reqid, 1)) != NC_OK) {
            nc_free(buf);
            return status;
        }

        if ((status = msg_copy(r, buf, msglen)) != NC_OK) {
            nc_free(buf);
            return status;
        }

        nc_free(buf);
    }

    /* Set bucketlen, but use existing keylen (-1 to calc it). Also leave key
     * offset as is. */
    msg_set_keypos(r, 0, 7, -1, bucketlen);

    r->mlen = pbmsglen;
    r->type = type;

    return NC_OK;
}

/**.......................................................................
 * Take a Redis GET request, and remap it to a PB message suitable for
 * sending to riak.
 */
rstatus_t
encode_pb_get_req(struct msg* r, struct conn* s_conn, msg_type_t type)
{
    return _encode_pb_get_req(r, s_conn, type, 0);
}

rstatus_t
_encode_pb_get_req(struct msg* r, struct conn* s_conn, msg_type_t type,
                   unsigned read_before_write)
{
    ASSERT(r != NULL);
    ASSERT(s_conn != NULL);

    rstatus_t status;

    RpbGetReq req = RPB_GET_REQ__INIT;
    struct msg_pos keyname_start_pos = msg_pos_init();

    if ((status = extract_bucket_key_value(r, &req.type, &req.bucket, &req.key,
                                           NULL, &keyname_start_pos, false))
        != NC_OK) {
        return status;
    }

    if (req.type.len > 0) {
        req.has_type = 1;
    }

    if (req.type.len > 0) {
        req.has_type = 1;
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

    req.has_deletedvclock =
            (opt->riak_deletedvclock != CONF_UNSET_NUM);
    if (req.has_deletedvclock) {
        req.deletedvclock = opt->riak_deletedvclock;
    }

    req.has_timeout = (opt->riak_timeout != CONF_UNSET_NUM);
    if (req.has_timeout) {
        req.timeout = opt->riak_timeout;
    }

    r->read_before_write = read_before_write;

    int type_and_bucket_len = ((req.type.len > 0) ? req.type.len + 1 : 0)
            + ((req.bucket.len > 0) ? req.bucket.len : 0);
    status = pack_message(r, type, rpb_get_req__get_packed_size(&req),
                          REQ_RIAK_GET, (pb_pack_func) rpb_get_req__pack, &req,
                          type_and_bucket_len);
    nc_free(req.type.data);

    return status;
}

/**.......................................................................
 * Take a Redis SET request, and remap it to a PB message suitable for
 * sending to riak.
 */
rstatus_t
encode_pb_put_req(struct msg* r, struct conn* s_conn, msg_type_t type)
{
    ASSERT(r != NULL);
    ASSERT(s_conn != NULL);

    rstatus_t status;

    RpbPutReq req = RPB_PUT_REQ__INIT;
    req.has_key = 1;
    RpbContent content = RPB_CONTENT__INIT;
    req.content = &content;

    struct msg_pos keyname_start_pos = msg_pos_init();
    if ((status = extract_bucket_key_value(r, &req.type, &req.bucket, &req.key,
                                           &req.content[0].value,
                                           &keyname_start_pos, false))
        != NC_OK) {
        return status;
    }

    if (req.type.len > 0) {
        req.has_type = 1;
    }

    if (req.type.len > 0) {
        req.has_type = 1;
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
        if ((req.content->value.data[0] == '{') &&
            (req.content->value.data[req.content->value.len - 1] == '}')) {
            req.content[0].content_type.len = 16; /*<< strlen("application/json")*/
            req.content[0].content_type.data = (uint8_t*)"application/json";
        } else if ((req.content->value.data[0] == '<') &&
            (req.content->value.data[req.content->value.len - 1] == '>')) {
            req.content[0].content_type.len = 15; /*<< strlen("application/xml")*/
            req.content[0].content_type.data = (uint8_t*)"application/xml";
        } else {
            req.content[0].content_type.len = 10; /*<< strlen("text/plain")*/
            req.content[0].content_type.data = (uint8_t*)"text/plain";
        }
    }

    /* Set the vclock, otherwise causing "sibling explosion" */
    if (r->has_vclock && r->vclock.len > 0) {
        req.has_vclock = (protobuf_c_boolean)1;
        req.vclock = r->vclock;
    }

    int type_and_bucket_len = ((req.type.len > 0) ? req.type.len + 1 : 0)
            + ((req.bucket.len > 0) ? req.bucket.len : 0);
    status = pack_message(r, type, rpb_put_req__get_packed_size(&req),
                          REQ_RIAK_PUT, (pb_pack_func) rpb_put_req__pack, &req,
                          type_and_bucket_len);
    nc_free(req.type.data);
    nc_free(req.content->value.data);
    return status;
}

/**.......................................................................
 * Take a Redis DEL request, and remap it to a PB message suitable for
 * sending to riak.
 */
rstatus_t
encode_pb_del_req(struct msg* r, struct conn* s_conn, msg_type_t type)
{
    ASSERT(r != NULL);
    ASSERT(s_conn != NULL);
    ASSERT(r->frag_owner != NULL);

    struct conn *c_conn = r->owner;
    struct context *ctx = conn_to_ctx(c_conn);

    rstatus_t status;
    struct server* server = (struct server*)(s_conn->owner);
    const struct server_pool* pool = (struct server_pool*)(server->owner);
    const struct backend_opt* opt = &pool->backend_opt;

    RpbDelReq req = RPB_DEL_REQ__INIT;
    struct msg_pos keyname_start_pos = msg_pos_init();
    uint32_t keys_number = 0;
    struct mbuf *mbuffirst = r->mhdr.stqh_first;
    while ((status = extract_bucket_key_value(r, &req.type, &req.bucket,
                                              &req.key, NULL,
                                              &keyname_start_pos, true))
           == NC_OK) {
        if (req.bucket.len > 0) {
            if (req.type.len > 0) {
                req.has_type = 1;
            }

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

            req.has_sloppy_quorum = (opt->riak_sloppy_quorum
                    != CONF_UNSET_NUM);
            if (req.has_sloppy_quorum) {
                req.sloppy_quorum = opt->riak_sloppy_quorum;
            }

            req.has_timeout = (opt->riak_timeout != CONF_UNSET_NUM);
            if (req.has_timeout) {
                req.timeout = opt->riak_timeout;
            }

            struct mbuf *mbuf = mbuf_get();
            if (mbuf == NULL) {
                nc_free(req.type.data);
                return NC_ENOMEM;
            }
            STAILQ_INSERT_HEAD(&r->mhdr, mbuf, next);

            int type_and_bucket_len =
                    ((req.type.len > 0) ? req.type.len + 1 : 0) + (
                            (req.bucket.len > 0) ? req.bucket.len : 0);
            status = pack_message(r, type, rpb_del_req__get_packed_size(&req),
                                  REQ_RIAK_DEL, (pb_pack_func)rpb_del_req__pack,
                                  &req, type_and_bucket_len);
            keys_number++;
        } else {
            status = add_pexpire_msg_key(ctx, c_conn, (char*)req.key.data,
                                         req.key.len, 0);
            r->integer++;
        }

        if (req.type.data != NULL) {
            nc_free(req.type.data);
        }

        if (status != NC_OK)
            return status;
    }

    /* Mark number of sub messages */
    r->nfrag = keys_number;
    r->frag_owner->integer += r->integer;

    /* remove last mbuf from message - it contains just list of keys */
    if (keys_number > 0) {
        while (mbuffirst->next.stqe_next) {
            mbuf_remove(&r->mhdr, mbuffirst->next.stqe_next);
        }
        mbuf_remove(&r->mhdr, mbuffirst);
        return NC_OK;
    } else if (r->integer > 0) {
        /* if no messages for riak, then answer on response with number of redis requests(bucketless keys) */
        struct conn *conn = r->frag_owner->owner;
        ASSERT(conn != NULL);

        /* remove subresponse of fragmented message */
        r->noreply = 1;
        r->frag_owner->nfrag_done = r->frag_owner->nfrag;

        status = event_add_out(ctx->evb, conn);

        /* prevent from forwarding
         * this return code also prevent message from putting into queue
         * that is why no need to clean up queue
         */
        return NC_EAGAIN;
    }
    return NC_ERROR;
}

/**.......................................................................
 * Parse a protobuf-encoded GET request into a request structure
 */
void
parse_pb_get_req(struct msg *r, uint32_t* len, uint8_t* msgid, RpbGetReq** req)
{
    ASSERT(r != NULL);
    ASSERT(len != NULL);
    ASSERT(msgid != NULL);
    ASSERT(req != NULL);

    struct mbuf* mbuf = STAILQ_FIRST(&r->mhdr);

    get_pb_msglen(r, len, msgid);

    /* Skip the message length */

    uint8_t* pos = mbuf->start + 4 + 1;

    *req = rpb_get_req__unpack(NULL, *len - 1, pos);
}

void
parse_pb_del_req(struct mbuf *mbuf, uint32_t* len, uint8_t* msgid,
                 RpbDelReq** req)
{
    ASSERT(mbuf != NULL);
    ASSERT(len != NULL);
    ASSERT(msgid != NULL);
    ASSERT(req != NULL);

    get_pb_mbuflen(mbuf, len, msgid);

    /* Skip the message length */

    uint8_t* pos = mbuf->start + 4 + 1;

    *req = rpb_del_req__unpack(NULL, *len - 1, pos);
}

/**.......................................................................
 * Parse a protobuf-encoded PUT request into a request structure
 */
void
parse_pb_put_req(struct msg *r, uint32_t* len, uint8_t* msgid, RpbPutReq** req)
{
    ASSERT(r != NULL);
    ASSERT(len != NULL);
    ASSERT(msgid != NULL);
    ASSERT(req != NULL);

    struct mbuf* mbuf = STAILQ_FIRST(&r->mhdr);

    get_pb_msglen(r, len, msgid);

    /* Skip the message length */

    uint8_t* pos = mbuf->start + 4 + 1;

    *req = rpb_put_req__unpack(NULL, *len - 1, pos);
}

/**.......................................................................
 * Add a PEXIPIRE message to a server's queue, parsing the key name and
 * value from a RIAK DEL request
 */
rstatus_t
add_pexpire_msg_riak(struct context *ctx, struct conn* c_conn, struct msg* msg)
{
    uint32_t len;
    uint8_t msgid;
    RpbDelReq* req = NULL;
    struct mbuf *mbuf;
    STAILQ_FOREACH(mbuf, &msg->peer->mhdr, next) {
        parse_pb_del_req(mbuf, &len, &msgid, &req);
        if (req == NULL)
            return NC_ENOMEM;

        const uint32_t delimiter_count = ((req->type.len > 0) ? 1 : 0)
                                         + ((req->bucket.len > 0) ? 1 : 0);
        const uint32_t keynamelen = req->type.len +
                                    req->bucket.len +
                                    req->key.len + delimiter_count;
        char keyname[keynamelen + 1];
        sprintf(keyname, "%.*s%s%.*s:%.*s",
                (int)req->type.len, req->type.data,
                (req->type.len > 0) ? ":" : "",
                (int)req->bucket.len, req->bucket.data,
                (int)req->key.len, req->key.data);

        add_pexpire_msg_key(ctx, c_conn, keyname, keynamelen, 0);
        nc_free(req);
    }
    return NC_OK;
}

/**.......................................................................
 * Add a SET message to a server's queue, parsing the key name and
 * value from a RIAK request/response pair
 */
rstatus_t
add_set_msg_riak(struct context *ctx, struct conn* c_conn, struct msg* msg)
{
    ASSERT(ctx != NULL);
    ASSERT(c_conn != NULL);

    ASSERT(msg != NULL);
    ASSERT(msg->type == MSG_RSP_REDIS_BULK);

    ASSERT(msg->peer != NULL);
    ASSERT(msg->peer->type == MSG_REQ_RIAK_GET);

    uint32_t len;
    uint8_t msgid;

    RpbGetReq* req = 0;
    parse_pb_get_req(msg->peer, &len, &msgid, &req);

    int delimiter_count = ((req->type.len > 0) ? 1 : 0)
                          + ((req->bucket.len > 0) ? 1 : 0);
    uint32_t keynamelen = req->type.len + req->bucket.len + req->key.len + delimiter_count;
    char keyname[keynamelen + 1];
    sprintf(keyname, "%.*s%s%.*s%s%.*s",
            (int)req->type.len, req->type.data,
            (req->type.len > 0) ? ":" : "",
            (int)req->bucket.len, req->bucket.data,
            (req->bucket.len > 0) ? ":" : "",
            (int)req->key.len, req->key.data);

    nc_free(req);

    struct msg_pos keyval_start_pos = msg_pos_init();
    size_t keyvallen = 0;

    rstatus_t status = NC_OK;
    if ((status = redis_get_next_string(msg, NULL, &keyval_start_pos,
                                        &keyvallen))
        != NC_OK)
        return status;

    return add_set_msg_key(ctx, c_conn, keyname, &keyval_start_pos, keyvallen);
}

/**.......................................................................
 * Extract a PB-encoded response out of the message buffer
 */
bool
extract_rsp(struct msg* r, uint32_t len, uint8_t* msgid, unpack_func func,
            void ** rpbresp)
{
    ASSERT(r != NULL);

    uint8_t* buf;
    uint32_t allocs = 0;

    /*
     * If the first message in this object fits into a single mbuf, then
     * we can just read from the first mbuf
     * Bu if not, we have to allocate a buffer into which we will copy
     * the message from multiple mbufs, to pass to
     * rpb_get_resp__unpack below
     */
    if (len + 4 > mbuf_data_size() && rpbresp) {
        allocs = r->mlen;
    }

    if (allocs) {
        buf = nc_alloc(allocs);
        if (msg_extract(r, buf, r->mlen) != NC_OK) {
            if (rpbresp) {
                *rpbresp = NULL;
            }
            return false;
        }
    } else {
        const struct mbuf* mbuf = STAILQ_FIRST(&r->mhdr);
        buf = mbuf->start;
    }

    /* Skip the message length */

    uint8_t* pos = buf + 4;

    /* Get the message id */

    *msgid = *(pos++);

    /* And unpack the PB response from the rest */

    if (rpbresp) {
        *rpbresp = func(NULL, len - 1, pos);
        if (allocs) {
            nc_free(buf);
        }
        return (*rpbresp) ? true : false;
    }
    return true;
}

/**.......................................................................
 * Extract a PB-encoded GET response out of the message buffer
 */
RpbGetResp*
extract_get_rsp(struct msg* r, uint32_t len, uint8_t* msgid)
{
    RpbGetResp* rpbresp;
    if (extract_rsp(r, len, msgid, (unpack_func)rpb_get_resp__unpack,
                (void*)&rpbresp) == false)
        return NULL;
    return rpbresp;
}

/**.......................................................................
 * Extract a PB-encoded PUT response out of the message buffer
 */
RpbPutResp*
extract_put_rsp(struct msg* r, uint32_t len, uint8_t* msgid)
{
    RpbPutResp* rpbresp;
    if (extract_rsp(r, len, msgid, (unpack_func)rpb_put_resp__unpack,
                (void*)&rpbresp) == false)
        return NULL;
    return rpbresp;
}

/**.......................................................................
 * Extract a PB-encoded DEL response out of the message buffer
 */
bool
extract_del_rsp(struct msg* r, uint32_t len, uint8_t* msgid)
{
    return extract_rsp(r, len, msgid, NULL, NULL);
}

/**.......................................................................
 * Re-pack a PB-formatted GET response for return to a Redis client
 */
rstatus_t
repack_get_rsp(struct msg* r, RpbGetResp* rpbresp)
{
    ASSERT(r != NULL);
    ASSERT(rpbresp != NULL);

    rstatus_t status = NC_OK;

    msg_rewind(r);
    r->mlen = 0;

    /* If this response has content, pack the key val */

    if (rpbresp->n_content > 0) {
        /*
         * If there are siblings, there will be more than one keyval
         * present.
         * 
         * For now, we return the last-modified sibling presented by Riak,
         * unless all have the same last_mod time, in which case we select
         * a sibling at random
         *
         * We do this rather than return an array of all values, since
         * Redis clients expect only a single string in response to a GET
         * command, and not an array.
         */
        unsigned iContent = choose_sibling(rpbresp);
        uint8_t* data = rpbresp->content[iContent]->value.data;
        uint32_t datalen = rpbresp->content[iContent]->value.len;

        /* Strip spurious quotes surrounding the value */

        if ((datalen > 1) && (data[0] == '\"')) {
            datalen -= 2;
            ++data;
        }

        uint32_t datalenndig = ndig(datalen);
        char fmtbuf[datalenndig + 3 + 1];
        sprintf(fmtbuf, "$%d\r\n", datalen);

        if ((status = msg_copy_char(r, fmtbuf, strlen(fmtbuf))) != NC_OK) {
            return status;
        }

        if ((status = msg_copy(r, data, datalen)) != NC_OK) {
            return status;
        }

        if ((status = msg_copy_char(r, CRLF, 2)) != NC_OK) {
            return status;
        }
    } else {
        if ((status = msg_copy_char(r, "$-1\r\n", 5)) != NC_OK) {
            return status;
        }
    }

    r->type = MSG_RSP_REDIS_BULK;

    return NC_OK;
}

/**.......................................................................
 * Re-pack a PB-formatted PUT response for return to a Redis client
 */
rstatus_t
repack_put_resp(struct msg* r, RpbPutResp* rpbresp)
{
    ASSERT(r != NULL);
    ASSERT(rpbresp != NULL);

    rstatus_t status = NC_OK;

    msg_rewind(r);
    r->mlen = 0;

    if ((status = msg_copy_char(r, "+OK\r\n", 5)) != NC_OK) {
        return status;
    }

    r->type = MSG_RSP_REDIS_STATUS;

    return NC_OK;
}

/**.......................................................................
 * Re-pack a PB-formatted DEL response for return to a Redis client
 */
rstatus_t
repack_del_resp(struct msg* r)
{
    ASSERT(r != NULL);
    rstatus_t status = NC_OK;

    uint8_t msgid;
    if (extract_del_rsp(r, 0, &msgid) == false) {
        return NC_ERROR;
    }

    msg_rewind(r);
    r->mlen = 0;

    if (msgid == RSP_RIAK_DEL) {
        r->integer = 1;
    }

    if ((status = msg_copy_char(r, "+OK\r\n", 5)) != NC_OK) {
        return status;
    }

    r->type = MSG_RSP_RIAK_INTEGER;

    return NC_OK;
}

/**.......................................................................
 * Repack a message -- for Riak responses, this repacks to the
 * equivalent Redis response
 */
rstatus_t
riak_repack(struct msg* r)
{
    /*
     * This method is only called after riak_parse_rsp has been
     * evaluated, so we know at this point that the mbuf contains a valid
     * message
     */

    RpbGetResp* rpb_get_resp = NULL;
    RpbPutResp* rpb_put_resp = NULL;
    DtUpdateResp* dt_update_resp = NULL;
    DtFetchResp* dt_fetch_resp = NULL;
    uint32_t len = 0;
    uint8_t msgid = RSP_RIAK_UNKNOWN;
    if (!get_pb_msglen(r, &len, &msgid)) {
        r->result = MSG_PARSE_ERROR;
        return NC_ERROR;
    }

    switch (msgid) {
    case RSP_RIAK_GET:
        rpb_get_resp = extract_get_rsp(r, len, &msgid);

        if (rpb_get_resp == NULL) {
            r->result = MSG_PARSE_ERROR;
            break;
        }

        msg_copy_vclock(r, rpb_get_resp->has_vclock, rpb_get_resp->vclock);

        if (r->peer != NULL) {
            msg_copy_vclock(r->peer, rpb_get_resp->has_vclock, rpb_get_resp->vclock);
        }

        if (repack_get_rsp(r, rpb_get_resp) != NC_OK) {
            r->result = MSG_PARSE_ERROR;
        }

        rpb_get_resp__free_unpacked(rpb_get_resp, NULL);
        break;

    case RSP_RIAK_PUT:
        rpb_put_resp = extract_put_rsp(r, len, &msgid);

        if (rpb_put_resp == NULL) {
            r->result = MSG_PARSE_ERROR;
            break;
        }

        if (repack_put_resp(r, rpb_put_resp) != NC_OK) {
            r->result = MSG_PARSE_ERROR;
        }

        rpb_put_resp__free_unpacked(rpb_put_resp, NULL);
        break;

    case RSP_RIAK_DEL:
    {
        if (!extract_del_rsp(r, len, &msgid)) {
            r->result = MSG_PARSE_ERROR;
            break;
        }

        if (repack_del_resp(r) != NC_OK) {
            r->result = MSG_PARSE_ERROR;
        }
    }
        break;

    case RSP_RIAK_UNKNOWN:
    {
        struct msg* pmsg = TAILQ_FIRST(&r->owner->omsg_q);
        if (pmsg->type == MSG_REQ_RIAK_SREM) {
            if (repack_dt_update_resp(r, dt_update_resp) != NC_OK) {
                r->result = MSG_PARSE_ERROR;
            }
        }
    }
        break;

    case RSP_RIAK_DT_UPDATE:
        dt_update_resp = extract_dt_update_rsp(r, len, &msgid);

        if (dt_update_resp == NULL) {
            r->result = MSG_PARSE_ERROR;
            break;
        }

        if (repack_dt_update_resp(r, dt_update_resp) != NC_OK) {
            r->result = MSG_PARSE_ERROR;
        }

        dt_update_resp__free_unpacked(dt_update_resp, NULL);
        break;

    case RSP_RIAK_DT_FETCH:
        dt_fetch_resp = extract_dt_fetch_rsp(r, len, &msgid);

        if (dt_fetch_resp == NULL) {
            r->result = MSG_PARSE_ERROR;
            break;
        }

        if (repack_dt_fetch_resp(r, dt_fetch_resp) != NC_OK) {
            r->result = MSG_PARSE_ERROR;
        }

        dt_fetch_resp__free_unpacked(dt_fetch_resp, NULL);
        break;

    default:
        break;
    }

    if (r->result == MSG_PARSE_ERROR) {
        return NC_ERROR;
    }
    return NC_OK;
}

/**.......................................................................
 * Choose an entry to return to Redis when multiple siblings are present
 */
unsigned
choose_sibling(RpbGetResp* rpbresp)
{
    if (rpbresp->n_content == 1)
        return 0;

    /* TODO: delve into vclock or dotted-version vector (dvv), currently using last_mod */

    if (rpbresp->content[0]->has_last_mod && rpbresp->content[0]
            ->has_last_mod_usecs) {
        return choose_last_modified_sibling(rpbresp);
    } else {
        return choose_random_sibling(rpbresp->n_content);
    }
}

/**.......................................................................
 * Return the index of the last modified sibling
 */
unsigned
choose_last_modified_sibling(RpbGetResp* rpbresp)
{
    unsigned iMaxLastMod = 0;
    double lastMod = 0, maxLastMod = 0;

    unsigned iSib = 0;
    unsigned nSib = rpbresp->n_content;

    for (iSib = 0; iSib < nSib; iSib++) {

        RpbContent* content = rpbresp->content[iSib];
        lastMod = content->last_mod + (double)(content->last_mod_usecs) / 1e6;

        if (iSib == 0 || lastMod > maxLastMod) {
            maxLastMod = lastMod;
            iMaxLastMod = iSib;
        }

    }

    unsigned indices[nSib];
    unsigned nIdent = 0;

    for (iSib = 0; iSib < nSib; iSib++) {
        RpbContent* content = rpbresp->content[iSib];
        lastMod = content->last_mod + (double)(content->last_mod_usecs) / 1e6;

        if (lastMod == maxLastMod) {
            indices[nIdent++] = iSib;
        }
    }

    return nIdent == 1 ? iMaxLastMod : indices[choose_random_sibling(nIdent)];
}

/**.......................................................................
 * Return a random index
 */
unsigned
choose_random_sibling(unsigned nSib)
{
    return (unsigned)((double)(rand()) / RAND_MAX * nSib);
}
