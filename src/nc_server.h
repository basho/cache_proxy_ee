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

#ifndef _NC_SERVER_H_
#define _NC_SERVER_H_

#include <nc_core.h>

/*
 * server_pool is a collection of servers and their continuum. Each
 * server_pool is the owner of a single proxy connection and one or
 * more client connections. server_pool itself is owned by the current
 * context.
 *
 * Each server is the owner of one or more server connections. server
 * itself is owned by the server_pool.
 *
 *  +-------------+
 *  |             |<---------------------+
 *  |             |<------------+        |
 *  |             |     +-------+--+-----+----+--------------+
 *  |   pool 0    |+--->|          |          |              |
 *  |             |     | server 0 | server 1 | ...     ...  |
 *  |             |     |          |          |              |--+
 *  |             |     +----------+----------+--------------+  |
 *  +-------------+                                             //
 *  |             |
 *  |             |
 *  |             |
 *  |   pool 1    |
 *  |             |
 *  |             |
 *  |             |
 *  +-------------+
 *  |             |
 *  |             |
 *  .             .
 *  .    ...      .
 *  .             .
 *  |             |
 *  |             |
 *  +-------------+
 *            |
 *            |
 *            //
 */

typedef uint32_t (*hash_t)(const char *, size_t);

struct continuum {
    uint32_t index;  /* server index */
    uint32_t value;  /* hash value */
};

struct server {
    uint32_t           idx;           /* server index */
    struct server_pool *owner;        /* owner pool */

    struct string      pname;         /* name:port:weight (ref in conf_server) */
    struct string      name;          /* name (ref in conf_server) */
    uint16_t           port;          /* port */
    uint32_t           weight;        /* weight */
    int                family;        /* socket family */
    socklen_t          addrlen;       /* socket length */
    struct sockaddr    *addr;         /* socket address (ref in conf_server) */

    uint32_t           ns_conn_q;     /* # server connection */
    struct conn_tqh    s_conn_q;      /* server connection q */

    int64_t            next_retry;    /* next retry time in usec */
    uint32_t           failure_count; /* # consecutive failures */

    bool               backend;       /* is a backend or frontend server? */
};

struct servers {
    struct server_pool *owner;               /* owner pool */
    struct array       server_arr;               /* server[] */
    uint32_t           ncontinuum;           /* # continuum points */
    uint32_t           nserver_continuum;    /* # servers - live and dead on continuum (const) */
    struct continuum   *continuum;           /* continuum */
    uint32_t           nlive_server;         /* # live server */
    int64_t            next_rebuild;         /* next distribution rebuild time in usec */
};

struct bucket_prop {
    struct string       datatype;            /* datatype */
    struct string       bucket;              /* bucket */
    int64_t             ttl_ms;              /* port */
};

struct backend_opt {
    connection_type_t  type;                 /* The type of backend server */
    int                max_resend;           /* maximum number of backend servers we will resend to */
    int                riak_r;               /* Riak r val */
    int                riak_pr;              /* Riak pr val */
    int                riak_w;               /* Riak w val */
    int                riak_pw;              /* Riak pw val */
    int                riak_n;               /* Riak n val */
    int                riak_basic_quorum;    /* Riak basic_quorum val */
    int                riak_sloppy_quorum;   /* Riak sloppy_quorum val */
    int                riak_notfound_ok;     /* Riak notfound_ok */
    int                riak_deletedvclock;   /* Riak deletedvclock */
    int                riak_timeout;         /* Riak timeout */
    struct array       bucket_prop;          /* buckets properties */
};

struct server_pool {
    uint32_t           idx;                  /* pool index */
    struct context     *ctx;                 /* owner context */

    struct conn        *p_conn;              /* proxy connection (listener) */
    uint32_t           nc_conn_q;            /* # client connection */
    struct conn_tqh    c_conn_q;             /* client connection q */

    struct servers     frontends;           /* frontend servers list */

    /*  "Backends" are source-of-truth servers for which redis is used as a
     *  read-through cache. They are managed by the same system as redis servers
     *  and queries against them are handled in the same fashion. */
    struct servers     backends;            /* backend servers list */
    struct backend_opt backend_opt;         /* backend servers options */

    struct string      name;                 /* pool name (ref in conf_pool) */
    struct string      addrstr;              /* pool address (ref in conf_pool) */
    struct string      redis_auth;           /* redis_auth password */
    uint16_t           port;                 /* port */
    int                family;               /* socket family */
    socklen_t          addrlen;              /* socket length */
    struct sockaddr    *addr;                /* socket address (ref in conf_pool) */
    mode_t             perm;                 /* socket permission */
    int                dist_type;            /* distribution type (dist_type_t) */
    int                key_hash_type;        /* key hash type (hash_type_t) */
    hash_t             key_hash;             /* key hasher */
    struct string      hash_tag;             /* key hash tag (ref in conf_pool) */
    int                timeout;              /* timeout in msec */
    int                backlog;              /* listen backlog */
    int                redis_db;             /* redis database to connect to */
    uint32_t           client_connections;   /* maximum # client connection */
    uint32_t           server_connections;   /* maximum # server connection */
    int64_t            server_retry_timeout; /* server retry timeout in usec */
    uint32_t           server_failure_limit; /* server failure limit */
    int64_t            server_ttl_ms;        /* TTL for writes to the
                                              * frontend servers in ms
                                              * server_ttl_ms == 0
                                              * will be taken to mean
                                              * never */
    unsigned           auto_eject_hosts:1;   /* auto_eject_hosts? */
    unsigned           preconnect:1;         /* preconnect? */
    unsigned           redis:1;              /* redis? */
};

void server_ref(struct conn *conn, void *owner);
void server_unref(struct conn *conn);
int server_timeout(struct conn *conn);
bool server_active(struct conn *conn);
rstatus_t server_init(struct array *server, struct array *conf_server, struct server_pool *sp);
void server_deinit(struct array *server);
struct conn *server_conn(struct server *server);
rstatus_t server_connect(struct context *ctx, struct server *server, struct conn *conn);
void server_close(struct context *ctx, struct conn *conn);
void server_connected(struct context *ctx, struct conn *conn);
void server_ok(struct context *ctx, struct conn *conn);

uint32_t servers_idx(struct servers *servers, uint8_t *key, uint32_t keylen);
struct server *servers_server(struct servers *servers, uint8_t *key, uint32_t keylen);
struct conn *server_pool_conn_frontend(struct context *ctx, struct server_pool *pool, uint8_t *key,
                                       uint32_t keylen, struct server* input_server);
struct conn *server_pool_conn_backend(struct context *ctx, struct server_pool *pool, uint8_t *key,
                                      uint32_t keylen, struct server* input_server);

rstatus_t servers_run(struct servers *servers);
rstatus_t server_pool_preconnect(struct context *ctx);
void server_pool_disconnect(struct context *ctx);
rstatus_t server_pool_init(struct array *server_pool, struct array *conf_pool, struct context *ctx);
void server_pool_deinit(struct array *server_pool);

int64_t server_pool_bucket_ttl(struct server_pool *pool, uint8_t *datatype, uint32_t datatypelen,
                               uint8_t *bucket, uint32_t bucketlen);
void server_pool_bp_deinit(struct array *bpa);

#endif
