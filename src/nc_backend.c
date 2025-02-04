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

#include <sys/uio.h>

#include <nc_core.h>
#include <nc_proto.h>

bool process_frontend_rsp(struct context *ctx, struct conn *s_conn, struct msg* msg);
bool process_backend_rsp(struct context *ctx, struct conn *s_conn, struct msg* msg);

bool resend_to_backend(struct context *ctx, struct conn *s_conn, struct msg* msg);

bool forward_response(struct context *ctx, struct conn* c_conn, struct conn* s_conn,
                      struct msg* pmsg, struct msg* msg);

bool swallow_response(struct context *ctx, struct conn* c_conn, struct conn* s_conn,
                      struct msg* pmsg, struct msg* msg);

rstatus_t add_set_msg(struct context *ctx, struct conn* c_conn, struct msg* msg);
rstatus_t add_pexpire_msg(struct context *ctx, struct conn* c_conn, struct msg* msg);

void init_backend_resend_q(struct msg* msg);
void insert_in_backend_resend_q(struct msg* msg, struct server* server);

struct backend_enqueue_post_msg_param {
    struct context *ctx;
    struct conn *c_conn;
    struct conn *s_conn;
};
rstatus_t backend_enqueue_post_msg(void *elem /*struct msg *msg*/,
        void *data /*struct backend_enqueue_post_msg_param *prm*/);
rstatus_t backend_event_add_post_msg(
        struct backend_enqueue_post_msg_param *prm);

/**.......................................................................
 * A generic stub for backend processing of requests.
 *
 * Intercepts responses and checks for exceptional conditions.  Where
 * relevant, this function sends new messages to backend or frontend
 * servers.
 *
 * Returns true if processing was done, false if not (relevant to
 * whether or not rsp_filter forwards the response to the client on
 * exit from this function)
 */
bool
backend_process_rsp(struct context *ctx, struct conn *s_conn, struct msg* msg)
{
    if (msg_nbackend(msg) == 0) {
        return false;
    }

    struct server* server = (struct server*)s_conn->owner;

    if (server->backend) {
        return process_backend_rsp(ctx, s_conn, msg);
    } else {
        return process_frontend_rsp(ctx, s_conn, msg);
    }
}

/**.......................................................................
 * Simple resend of original peer message to the backend pool.
 *
 * Reuses the peer message without reallocation; just manipulates
 * the peer message to reset mbuf pointers.
 */
bool
resend_to_backend(struct context *ctx, struct conn *s_conn, struct msg* msg)
{
    rsp_get_peer(ctx, s_conn, msg);

    /*
     * Get a pointer to the client connection that originated the peer,
     * and set the peer message as its next message to send.
    */
    struct conn* c_conn = msg->peer->owner;
    struct msg* pmsg = msg->peer;

    c_conn->rmsg = pmsg;

    /*
     * Re-mark the peer as in-process, and re-initialize its buffer
     * pointer to the start of the first mbuf
     */

    pmsg->done = 0;
    pmsg->peer = NULL;

    msg_reset_pos(pmsg);

    /*
     * If the response initiating this resend was received from a
     * frontend server, initialize the resend queue now (if we are
     * resending a message from one backend server to another, the queue
     * will already have been initialized)
     */

    struct server* server = (struct server*)s_conn->owner;

    if (!server->backend) {
        init_backend_resend_q(msg);
    }

    /*
     * Sanity check that this message has a peer enqueued on the client
     * omsg_q.  Malformed Redis messages with extra terminator sequences
     * can cause this check to fail.
     */

    if (TAILQ_EMPTY(&c_conn->omsg_q)) {
        return true;
    }

    /*
     * Re-send the request to our backend servers, without removing the
     * original request from the client's message queue (which could
     * lead to responses that are out of order wrt to the requests that
     * generated them).  recv_done() with enqueue = false doesn't
     * re-enqueue the message, so the original request is left in the
     * order in which it was received
     */

    c_conn->recv_done(ctx, c_conn, pmsg, NULL, true, false);

    /*
     * Return the original message to the free msg pool, but only if it
     * was processed successfully (otherwise the original message will
     * be forwarded)
     *
     * We use the peer message's error value to indicate if resending
     * succeeded.
     *
     * For now anyway, if an error occurred while attempting to resend
     * this request to the backend servers, we return false, so that on
     * exit from rsp_filter, rsp_recv_done will just forward the
     * original reponse to the client.  (If in future we want to
     * propagate backend errors up to the redis client, we can use
     * pmsg->error and pmsg->err to indicate the type of error.  For
     * now, we just reset the error flag as if nothing happened)
     *
     * For this to work, we need to add the original (peer) request back
     * to the frontend server's omsgq -- ie, restore the state we were
     * in when the original response was received.  Note that we need to
     * insert it at the head of the omsg_q (the position it was in when
     * we entered this function), not at the tail.
     *
     * If however the message was successfully intercepted, we return
     * the message to the pool by calling msg_put()
     */

    if (pmsg->error) {
        pmsg->error = 0;
        TAILQ_INSERT_HEAD(&s_conn->omsg_q, pmsg, s_tqe);
        return false;
    } else {
        msg_put(msg);
        return true;
    }
}

/**.......................................................................
 * Get the number of backend servers present in the pool that owns
 * this message's connection
 */
uint32_t
msg_nbackend(struct msg* msg)
{
    struct server_pool* conn_pool = msg_get_server_pool(msg);
    return array_n(&conn_pool->backends.server_arr);
}

/**.......................................................................
 * Get the number of backend servers present in the pool that owns
 * this connection
 */
uint32_t
conn_nbackend(struct conn* conn)
{
    struct server_pool* conn_pool = (struct server_pool*)conn->owner;
    return array_n(&conn_pool->backends.server_arr);
}

/**.......................................................................
 * Get the type of backend servers present in the pool that owns
 * this message's connection
 */
connection_type_t
msg_backend_type(struct msg* msg)
{
    struct server_pool* conn_pool = msg_get_server_pool(msg);
    return conn_pool->backend_opt.type;
}

/**.......................................................................
 * Swallow the current message if it should not be forwarded to the client
 *
 * Return true if the message was processed (swallowed)
 *        false if not
 */
bool
swallow_response(struct context *ctx, struct conn* c_conn, struct conn* s_conn,
                 struct msg* pmsg, struct msg* msg)
{
    if (pmsg->swallow) {

        s_conn->swallow_msg(s_conn, pmsg, msg);
        s_conn->dequeue_outq(ctx, s_conn, pmsg);

        pmsg->done = 1;

        log_debug(LOG_INFO,
                  "swallow rsp %"PRIu64" len %"PRIu32" of req " "%"PRIu64" on s %d",
                  msg->id, msg->mlen, pmsg->id, s_conn->sd);

        rsp_put(msg);
        req_put(pmsg);

        return true;
    }

    return false;
}

/**.......................................................................
 * Backend processing of a response from a frontend server.
 *
 * Initiate comms with the backend server pool for various
 * commands here.
 *
 * Test case is GET path.  Others are currently unhandled.
 *
 * TODO: handle DEL for redis as backend
 * TODO: handle SET for redis as backend
 */
bool
process_frontend_rsp(struct context *ctx, struct conn *s_conn, struct msg* msg)
{
    struct msg* pmsg = TAILQ_FIRST(&msg->owner->omsg_q);
    switch (pmsg->type) {

    case MSG_REQ_REDIS_GET:
    case MSG_REQ_REDIS_SMEMBERS:
    case MSG_REQ_REDIS_SISMEMBER:
    case MSG_REQ_REDIS_SCARD:
        if (msg_nil(msg)) {
            return resend_to_backend(ctx, s_conn, msg);
        }
        break;

    case MSG_REQ_REDIS_SDIFF:
    case MSG_REQ_REDIS_SINTER:
    case MSG_REQ_REDIS_SUNION:
    case MSG_REQ_REDIS_SDIFFSTORE:
    case MSG_REQ_REDIS_SINTERSTORE:
    case MSG_REQ_REDIS_SUNIONSTORE:
        if (pmsg->nsubs == 0 && msg_nil(msg)) {
            switch(pmsg->type) {
            case MSG_REQ_REDIS_SDIFF:
                riak_sync_key(ctx, pmsg, MSG_REQ_RIAK_SDIFF, false);
                break;
            case MSG_REQ_REDIS_SINTER:
                riak_sync_key(ctx, pmsg, MSG_REQ_RIAK_SINTER, false);
                break;
            case MSG_REQ_REDIS_SUNION:
                riak_sync_key(ctx, pmsg, MSG_REQ_RIAK_SUNION, false);
                break;
            case MSG_REQ_REDIS_SDIFFSTORE:
                riak_sync_key(ctx, pmsg, MSG_REQ_RIAK_SDIFFSTORE, true);
                break;
            case MSG_REQ_REDIS_SINTERSTORE:
                riak_sync_key(ctx, pmsg, MSG_REQ_RIAK_SINTERSTORE, true);
                break;
            case MSG_REQ_REDIS_SUNIONSTORE:
                riak_sync_key(ctx, pmsg, MSG_REQ_RIAK_SUNIONSTORE, true);
                break;
            default:
                NOT_REACHED();
            }
        } else if (pmsg->nsubs == 0) {
                uint32_t res;
                switch(pmsg->type) {
                case MSG_REQ_REDIS_SDIFFSTORE:
                case MSG_REQ_REDIS_SINTERSTORE:
                case MSG_REQ_REDIS_SUNIONSTORE:
                    riak_synced_key(ctx, pmsg, msg, &res);
                    msg_rewind(msg);
                    msg_prepend_format(msg, ":%d\r\n", res);
                    /* no break */
                default:
                    forward_response(ctx, pmsg->owner, s_conn, pmsg, msg);
                }

//                struct msg *nmsg = TAILQ_FIRST(&s_conn->omsg_q);
//                while (nmsg) {
//                    msg_put(nmsg);
//                    TAILQ_REMOVE(&s_conn->omsg_q, nmsg, s_tqe);
//                    nmsg = TAILQ_FIRST(&s_conn->omsg_q);
//                    if (nmsg->s_tqe.tqe_next == NULL)
//                        break;
//                }

        } else {
            pmsg->nsubs--;
        }
        return true;

    default:
        break;
    }
    return false;
}

rstatus_t
backend_enqueue_post_msg(void *elem /*struct msg *msg*/,
        void *data /*struct backend_enqueue_post_msg_param *prm*/)
{
    struct msg *msg = (struct msg*)elem;
    struct backend_enqueue_post_msg_param *prm =
        (struct backend_enqueue_post_msg_param*)data;
    struct context *ctx = prm->ctx;
    struct conn *s_conn = prm->s_conn;
    struct conn *c_conn = prm->c_conn;
    struct msg* msgp = TAILQ_FIRST(&c_conn->omsg_q);
    rstatus_t status = NC_OK;
    ProtobufCBinaryData vclock;
    ASSERT(msgp != NULL);
    ASSERT(msg != NULL);

    if (msgp->has_vclock) {
        vclock = msgp->vclock;
    } else {
        vclock.len = 0;
    }
    msg_copy_vclock(msg, (protobuf_c_boolean)1, vclock);

    s_conn->dequeue_outq(ctx, s_conn, msgp);
    c_conn->dequeue_outq(ctx, c_conn, msgp);

    msg->noreply = 0;
    s_conn->req_remap(s_conn, msg);

    c_conn->enqueue_outq(ctx, c_conn, msg);
    s_conn->enqueue_inq(ctx, s_conn, msg);

    return status;
}

rstatus_t
backend_event_add_post_msg(struct backend_enqueue_post_msg_param *prm)
{
    rstatus_t status = NC_OK;
    struct context *ctx = prm->ctx;
    struct conn *c_conn = prm->c_conn;

    if (TAILQ_EMPTY(&c_conn->omsg_q)) {
        if ((status = event_add_out(ctx->evb, c_conn)) != NC_OK) {
            c_conn->err = errno;
            return status;
        }
    }

    return status;
}

/**.......................................................................
 * Process a response received from a backend server.
 */
bool
process_backend_rsp(struct context *ctx, struct conn *s_conn, struct msg* msg)
{
    struct msg* pmsg = TAILQ_FIRST(&s_conn->omsg_q);
    ASSERT(pmsg != NULL);
    struct conn* c_conn = pmsg->owner;

    switch (msg->type) {
    case MSG_RSP_REDIS_BULK:
        if (pmsg->read_before_write) {
            msg_copy_vclock(pmsg, msg->has_vclock, msg->vclock);

            struct backend_enqueue_post_msg_param prmp;
            prmp.ctx = ctx;
            prmp.c_conn = c_conn;
            prmp.s_conn = s_conn;

            array_each(pmsg->msgs_post, backend_enqueue_post_msg, &prmp);
            backend_event_add_post_msg(&prmp);

            pmsg->swallow = 1;
            pmsg->done = 1;

           /* further processing needed, so return false  */
           return false;
        } else if (!msg_nil(msg)) {
            forward_response(ctx, c_conn, s_conn, pmsg, msg);
            add_set_msg(ctx, c_conn, msg);
        } else {
            if (!backend_resend_q_empty(pmsg)) {
                swallow_response(ctx, c_conn, s_conn, pmsg, msg);
                if (!resend_to_backend(ctx, s_conn, msg)) {
                    return false;
                }
            } else {
                forward_response(ctx, c_conn, s_conn, pmsg, msg);
            }
        }
        break;

    case MSG_RSP_REDIS_STATUS:
        if(pmsg->nsubs > 0) {
            if(--pmsg->nsubs > 0) {
                break;
            }
        }
        forward_response(ctx, c_conn, s_conn, pmsg, msg);
        if(msg->peer) {
            add_pexpire_msg(ctx, c_conn, msg);
        }
        break;

    case MSG_RSP_RIAK_INTEGER:
        pmsg->frag_owner->integer += msg->integer;
        pmsg->nfrag_done++;
        if (pmsg->nfrag_done < pmsg->nfrag) {
            break;
        }
        pmsg->integer = pmsg->frag_owner->integer;
        forward_response(ctx, c_conn, s_conn, pmsg, msg);
        add_pexpire_msg(ctx, c_conn, msg);

        break;

    case MSG_RSP_REDIS_INTEGER:
    case MSG_RSP_REDIS_MULTIBULK:
        forward_response(ctx, c_conn, s_conn, pmsg, msg);
        break;

    default:
        break;
    }

    return true;
}

/**.......................................................................
 * Function to add a SET message to the server's queue, extracting key
 * name and value from the passed message
 */
rstatus_t
add_set_msg(struct context *ctx, struct conn* c_conn, struct msg* msg)
{
    ASSERT(msg != NULL);
    ASSERT(msg->peer != NULL);
    ASSERT(msg->type == MSG_RSP_REDIS_BULK);

    if (msg_nil(msg)) {
        return NC_OK;
    }

    switch (msg->peer->type) {
    case MSG_REQ_REDIS_GET:
        return add_set_msg_redis(ctx, c_conn, msg);

    case MSG_REQ_RIAK_GET:
        return add_set_msg_riak(ctx, c_conn, msg);

    default:
        break;
    }

    return NC_ERROR;
}

/**.......................................................................
 * Function add PEXPIRE message to the server's queue, extracting key
 * name from the passed message
 */
rstatus_t
add_pexpire_msg(struct context *ctx, struct conn* c_conn, struct msg* msg)
{
    ASSERT(msg != NULL);
    ASSERT(msg->peer != NULL);

    if (msg->peer->type == MSG_REQ_RIAK_DEL && !msg_nil(msg)) {
        add_pexpire_msg_riak(ctx, c_conn, msg);
    }

    return NC_OK;
}

/**.......................................................................
 * Function to add a PEXPIRE message to the server's queue, with explicit
 * keyname and expiration time
 */
rstatus_t
add_pexpire_msg_key(struct context *ctx, struct conn* c_conn, char* keyname,
                    uint32_t keynamelen, uint32_t timeout)
{
    const char pexipire_begin_proto[] = "*3\r\n$7\r\npexpire\r\n$%u\r\n";
    const char pexipire_finish_proto[] = "\r\n$%u\r\n%u\r\n";
    uint32_t ntime_dig = ndig(timeout);
    rstatus_t status;
    struct conn* s_conn = server_pool_conn_frontend(ctx, c_conn->owner,
                                                    (uint8_t*)keyname,
                                                    keynamelen,
                                                    NULL);

    char pexipire_begin[sizeof(pexipire_begin_proto) - 2 + ndig(keynamelen)];
    const uint32_t pexipire_begin_len = (uint32_t)sprintf(pexipire_begin,
                                                          pexipire_begin_proto,
                                                          keynamelen);
    ASSERT(pexipire_begin_len == sizeof(pexipire_begin) - 1);
    UNUSED(pexipire_begin_len);

    char pexipire_finish[sizeof(pexipire_finish_proto) - 4 + ndig(ntime_dig)
                         + ntime_dig];
    const uint32_t pexipire_finish_len = (uint32_t)sprintf(
            pexipire_finish, pexipire_finish_proto, ntime_dig, timeout);
    ASSERT(pexipire_finish_len == sizeof(pexipire_finish) - 1);
    UNUSED(pexipire_finish_len);

    struct msg* msg = msg_get(c_conn, true);
    if (msg == NULL) {
        c_conn->err = errno;
        return NC_ENOMEM;
    }

    if ((status = msg_copy_char(msg, pexipire_begin, sizeof(pexipire_begin) - 1)) != NC_OK) {
        msg_put(msg);
        return status;
    }

    if ((status = msg_copy_char(msg, keyname, keynamelen)) != NC_OK) {
        msg_put(msg);
        return status;
    }

    if ((status = msg_copy_char(msg, pexipire_finish,
                                sizeof(pexipire_finish) - 1))
        != NC_OK) {
        msg_put(msg);
        return status;
    }

    msg->swallow = 1;
    msg->type = MSG_REQ_HIDDEN;

    if (TAILQ_EMPTY(&s_conn->imsg_q)) {
        event_add_out(ctx->evb, s_conn);
    }

    s_conn->enqueue_inq(ctx, s_conn, msg);
    s_conn->need_auth = 0;

    return NC_OK;
}

/**.......................................................................
 * Function to add a SET message to the server's queue, with explicit
 * keyname and keyval pos
 */
rstatus_t
add_set_msg_key(struct context *ctx, struct conn* c_conn, char* keyname,
                struct msg_pos* keyval_start_pos, uint32_t keyvallen)
{
    uint32_t keynamelen = (uint32_t)strlen(keyname);
    struct conn* s_conn = server_pool_conn_frontend(ctx, c_conn->owner,
                                                    (uint8_t*)keyname,
                                                    keynamelen, NULL);

    ASSERT(!s_conn->client && !s_conn->proxy);

    struct server* server = (struct server*)s_conn->owner;
    struct server_pool* pool = (struct server_pool*)server->owner;

    /* TTL portion */
    uint32_t ttlfmtlen = 1;
    uint32_t ttlndig = 0;
    bool use_ttl = false;
    uint32_t ttl_ms = 0;

    ProtobufCBinaryData datatype;
    ProtobufCBinaryData bucket;
    ProtobufCBinaryData key;
    nc_split_key_string((uint8_t*) keyname, keynamelen, &datatype, &bucket, &key);
    int64_t sl_ttl_ms = server_pool_bucket_ttl(pool,
                                               datatype.data,
                                               (uint32_t)datatype.len,
                                               bucket.data,
                                               (uint32_t)bucket.len);

    if (sl_ttl_ms > 0) {
        use_ttl = true;
        ttl_ms = (uint32_t)sl_ttl_ms;
        ttlndig = ndig(ttl_ms); /* Number of digits in ttl_ms (length of the number we will write) */
        ttlfmtlen = ndig(ttlndig) + 1; /* Number of digits in ttlndig (length of the formatted ttlndig) */
    }

    uint32_t ttlstrlen = ttlfmtlen + ttlndig + 12;
    char ttlstr[ttlstrlen + 1];

    if (use_ttl) {
        sprintf(ttlstr, "$2\r\npx\r\n$%d\r\n%u\r\n", ttlndig, ttl_ms);
    }

    ASSERT(strlen(ttlstr) == ttlstrlen);

    /* Key portion */
    uint32_t keynamendig = ndig(keynamelen);
    uint32_t keynamefmtlen = keynamendig + 1;
    uint32_t keynamestrlen = keynamefmtlen + keynamelen + 4;
    char keynamestr[keynamestrlen + 1];
    sprintf(keynamestr, "$%d\r\n%s\r\n", keynamelen, keyname);

    ASSERT(strlen(keynamestr) == keynamestrlen);

    /* Value portion */
    uint32_t keyvalndig = ndig(keyvallen);
    uint32_t keyvalfmtlen = keyvalndig + 1;
    uint32_t keyvalstrlen = keyvalfmtlen + 2;
    char keyvalstr[keyvalstrlen + 1];

    sprintf(keyvalstr, "$%d\r\n", keyvallen);

    ASSERT(strlen(keyvalstr) == keyvalstrlen);

    struct msg* msg = msg_get(c_conn, true);
    if (msg == NULL) {
        c_conn->err = errno;
        return NC_ENOMEM;
    }

    rstatus_t status = NC_OK;
    if (use_ttl) {
        if ((status = msg_copy_char(msg, "*5\r\n$3\r\nset\r\n",
                                    strlen("*5\r\n$3\r\nset\r\n")))
            != NC_OK) {
            msg_put(msg);
            return status;
        }
    } else {
        if ((status = msg_copy_char(msg, "*3\r\n$3\r\nset\r\n",
                                    strlen("*3\r\n$3\r\nset\r\n")))
            != NC_OK) {
            msg_put(msg);
            return status;
        }
    }

    if ((status = msg_copy_char(msg, keynamestr, strlen(keynamestr))) != NC_OK) {
        msg_put(msg);
        return status;
    }

    if ((status = msg_copy_char(msg, keyvalstr, strlen(keyvalstr))) != NC_OK) {
        msg_put(msg);
        return status;
    }

    if ((status = msg_copy_from_pos(msg, keyval_start_pos, keyvallen)) != NC_OK) {
        msg_put(msg);
        return status;
    }

    if ((status = msg_copy_char(msg, "\r\n", 2)) != NC_OK) {
        msg_put(msg);
        return status;
    }

    if (use_ttl) {
        if ((status = msg_copy_char(msg, ttlstr, strlen(ttlstr))) != NC_OK) {
            msg_put(msg);
            return status;
        }
    }

    msg->swallow = 1;

    if (TAILQ_EMPTY(&s_conn->imsg_q)) {
        status = event_add_out(ctx->evb, s_conn);
    }

    s_conn->enqueue_inq(ctx, s_conn, msg);
    s_conn->need_auth = 0;

    return NC_OK;
}

/**.......................................................................
 * Forward a server response back to the client that originated the
 * request.
 *
 * Returns true if the response was forwarded
 *         false if the response was swallowed instead
 */
bool
forward_response(struct context *ctx, struct conn* c_conn, struct conn* s_conn,
                 struct msg* pmsg, struct msg* msg)
{
    if (!swallow_response(ctx, c_conn, s_conn, pmsg, msg)) {
        rsp_forward(ctx, s_conn, msg);
        return true;
    }

    return false;
}

/**.......................................................................
 * Initialize a message's backend resend queue
 */
void
init_backend_resend_q(struct msg* msg)
{
    msg->backend_resend_servers->nelem = 0;
}

/**.......................................................................
 * Insert a server in this message's queue of backend servers
 */
void
insert_in_backend_resend_q(struct msg* msg, struct server* server)
{
    struct server** elem = array_push(msg->backend_resend_servers);
    *elem = server;
}

/**.......................................................................
 * Return true if this message's backend server queue is empty
 */
bool
backend_resend_q_empty(struct msg* msg)
{
    return msg->backend_resend_servers->nelem == 0;
}

/**.......................................................................
 * Get the next backend server we can send to
 */
struct server*
get_next_backend_server(struct msg* msg, struct conn* c_conn, uint8_t* key,
                        uint32_t keylen)
{
    struct server* server = NULL;

    if (backend_resend_q_empty(msg)) {

        struct server_pool* pool = c_conn->owner;
        unsigned nserver = array_n(&pool->backends.server_arr);
        int nresend = 0;
        int64_t now = nc_usec_now();
        if (now < 0) {
            return NULL;
        }

        struct server* primary_server = servers_server(&pool->backends,
                                                       key, keylen);

        unsigned iserver;
        for (iserver = 0; iserver < nserver; iserver++) {
            struct server* resend_server = array_get(&pool->backends.server_arr, iserver);

            if (nresend == pool->backend_opt.max_resend - 1)
                break;

            if (pool->auto_eject_hosts && resend_server->next_retry > now)
                continue;

            if (resend_server != primary_server) {
                insert_in_backend_resend_q(msg, resend_server);
                ++nresend;
            }
        }

        return primary_server;
    }

    server = *((struct server**)array_pop(msg->backend_resend_servers));

    return server;
}
