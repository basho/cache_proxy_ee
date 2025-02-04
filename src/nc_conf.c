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

#include <nc_core.h>
#include <nc_conf.h>
#include <nc_server.h>
#include <nc_util.h>
#include <proto/nc_proto.h>

char* conf_add_server_(struct conf *cf, struct command *cmd, void *conf, bool backend);

#define DEFINE_ACTION(_hash, _name) string(#_name),
static struct string hash_strings[] = {
    HASH_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_hash, _name) hash_##_name,
static hash_t hash_algos[] = {
    HASH_CODEC( DEFINE_ACTION )
    NULL
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_dist, _name) string(#_name),
static struct string dist_strings[] = {
    DIST_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

static struct command conf_commands[] = {
    { string("listen"),
      conf_set_listen,
      offsetof(struct conf_pool, listen) },

    { string("hash"),
      conf_set_hash,
      offsetof(struct conf_pool, hash) },

    { string("hash_tag"),
      conf_set_hashtag,
      offsetof(struct conf_pool, hash_tag) },

    { string("distribution"),
      conf_set_distribution,
      offsetof(struct conf_pool, distribution) },

    { string("timeout"),
      conf_set_num,
      offsetof(struct conf_pool, timeout) },

    { string("backlog"),
      conf_set_num,
      offsetof(struct conf_pool, backlog) },

    { string("client_connections"),
      conf_set_num,
      offsetof(struct conf_pool, client_connections) },

    { string("redis"),
      conf_set_bool,
      offsetof(struct conf_pool, redis) },

    { string("redis_auth"),
      conf_set_string,
      offsetof(struct conf_pool, redis_auth) },

    { string("redis_db"),
      conf_set_num,
      offsetof(struct conf_pool, redis_db) },

    { string("preconnect"),
      conf_set_bool,
      offsetof(struct conf_pool, preconnect) },

    { string("auto_eject_hosts"),
      conf_set_bool,
      offsetof(struct conf_pool, auto_eject_hosts) },

    { string("server_connections"),
      conf_set_num,
      offsetof(struct conf_pool, server_connections) },

    { string("server_retry_timeout"),
      conf_set_num,
      offsetof(struct conf_pool, server_retry_timeout) },

    { string("server_failure_limit"),
      conf_set_num,
      offsetof(struct conf_pool, server_failure_limit) },

    { string("server_ttl"),
      conf_set_server_ttl,
      offsetof(struct conf_pool, server_ttl_ms) },

    { string("servers"),
      conf_add_server,
      offsetof(struct conf_pool, server) },

    { string("backends"),
      conf_add_server_be,
      offsetof(struct conf_pool, server_be) },

    { string("buckets"),
      conf_add_bucket_prop,
      offsetof(struct conf_pool, bucket_prop) },

    { string("backend_type"),
      conf_set_backend_type,
      offsetof(struct conf_pool, backend_type) },

    { string("backend_max_resend"),
      conf_set_num,
      offsetof(struct conf_pool, backend_max_resend) },

    { string("backend_riak_r"),
      conf_set_num,
      offsetof(struct conf_pool, backend_riak_r) },

    { string("backend_riak_pr"),
      conf_set_num,
      offsetof(struct conf_pool, backend_riak_pr) },

    { string("backend_riak_w"),
      conf_set_num,
      offsetof(struct conf_pool, backend_riak_w) },

    { string("backend_riak_pw"),
      conf_set_num,
      offsetof(struct conf_pool, backend_riak_pw) },

    { string("backend_riak_n"),
      conf_set_num,
      offsetof(struct conf_pool, backend_riak_n) },

    { string("backend_riak_timeout"),
      conf_set_num,
      offsetof(struct conf_pool, backend_riak_timeout) },

    { string("backend_riak_basic_quorum"),
      conf_set_num,
      offsetof(struct conf_pool, backend_riak_basic_quorum) },

    { string("backend_riak_sloppy_quorum"),
      conf_set_num,
      offsetof(struct conf_pool, backend_riak_sloppy_quorum) },

    { string("backend_riak_notfound_ok"),
      conf_set_num,
      offsetof(struct conf_pool, backend_riak_notfound_ok) },

    { string("backend_riak_deletedvclock"),
      conf_set_num,
      offsetof(struct conf_pool, backend_riak_deletedvclock) },

    null_command
};

static void
conf_server_init(struct conf_server *cs)
{
    string_init(&cs->pname);
    string_init(&cs->name);
    cs->port = 0;
    cs->weight = 0;

    memset(&cs->info, 0, sizeof(cs->info));

    cs->valid = 0;
    cs->backend = 0;

    log_debug(LOG_VVERB, "init conf server %p", cs);
}

static void
conf_server_deinit(struct conf_server *cs)
{
    string_deinit(&cs->pname);
    string_deinit(&cs->name);
    cs->valid = 0;
    log_debug(LOG_VVERB, "deinit conf server %p", cs);
}

rstatus_t
conf_server_each_transform(void *elem, void *data)
{
    struct conf_server *cs = elem;
    struct array *server = data;
    struct server *s;

    ASSERT(cs->valid);

    s = array_push(server);
    ASSERT(s != NULL);

    s->idx = array_idx(server, s);
    s->owner = NULL;

    s->pname = cs->pname;
    s->name = cs->name;
    s->port = (uint16_t)cs->port;
    s->weight = (uint32_t)cs->weight;

    s->family = cs->info.family;
    s->addrlen = cs->info.addrlen;
    s->addr = (struct sockaddr *)&cs->info.addr;

    s->ns_conn_q = 0;
    TAILQ_INIT(&s->s_conn_q);

    s->next_retry = 0LL;
    s->failure_count = 0;

    s->backend = cs->backend;

    log_debug(LOG_VERB, "transform to server %"PRIu32" '%.*s'",
              s->idx, s->pname.len, s->pname.data);

    return NC_OK;
}

static rstatus_t
conf_pool_init(struct conf_pool *cp, struct string *name)
{
    rstatus_t status;

    string_init(&cp->name);

    string_init(&cp->listen.pname);
    string_init(&cp->listen.name);
    string_init(&cp->redis_auth);
    cp->listen.port = 0;
    memset(&cp->listen.info, 0, sizeof(cp->listen.info));
    cp->listen.valid = 0;

    cp->hash = CONF_UNSET_HASH;
    string_init(&cp->hash_tag);
    cp->distribution = CONF_UNSET_DIST;

    cp->timeout = CONF_UNSET_NUM;
    cp->backlog = CONF_UNSET_NUM;

    cp->client_connections = CONF_UNSET_NUM;

    cp->redis = CONF_UNSET_NUM;
    cp->redis_db = CONF_UNSET_NUM;
    cp->preconnect = CONF_UNSET_NUM;
    cp->auto_eject_hosts = CONF_UNSET_NUM;
    cp->server_connections = CONF_UNSET_NUM;
    cp->server_retry_timeout = CONF_UNSET_NUM;
    cp->server_failure_limit = CONF_UNSET_NUM;
    cp->server_ttl_ms = CONF_UNSET_NUM;

    cp->backend_type = CONN_UNKNOWN;
    cp->backend_max_resend = CONF_UNSET_NUM;
    cp->backend_riak_r = CONF_UNSET_NUM;
    cp->backend_riak_pr = CONF_UNSET_NUM;
    cp->backend_riak_w = CONF_UNSET_NUM;
    cp->backend_riak_pw = CONF_UNSET_NUM;
    cp->backend_riak_n = CONF_UNSET_NUM;
    cp->backend_riak_basic_quorum = CONF_UNSET_NUM;
    cp->backend_riak_sloppy_quorum = CONF_UNSET_NUM;
    cp->backend_riak_notfound_ok = CONF_UNSET_NUM;
    cp->backend_riak_deletedvclock = CONF_UNSET_NUM;
    cp->backend_riak_timeout = CONF_UNSET_NUM;

    array_null(&cp->server);

    cp->valid = 0;

    status = string_duplicate(&cp->name, name);
    if (status != NC_OK) {
        return status;
    }

    status = array_init(&cp->server, CONF_DEFAULT_SERVERS,
                        sizeof(struct conf_server));
    if (status != NC_OK) {
        string_deinit(&cp->name);
        return status;
    }

    status = array_init(&cp->server_be, CONF_DEFAULT_SERVERS,
                        sizeof(struct conf_server));
    if (status != NC_OK) {
        string_deinit(&cp->name);
        array_deinit(&cp->server);
        return status;
    }

    status = array_init(&cp->bucket_prop, CONF_DEFAULT_BUCKETS,
                        sizeof(struct bucket_prop));
    if (status != NC_OK) {
        string_deinit(&cp->name);
        array_deinit(&cp->server);
        array_deinit(&cp->server_be);
        return status;
    }

    log_debug(LOG_VVERB, "init conf pool %p, '%.*s'", cp, name->len, name->data);

    return NC_OK;
}

static void
conf_pool_deinit(struct conf_pool *cp)
{
    string_deinit(&cp->name);

    string_deinit(&cp->listen.pname);
    string_deinit(&cp->listen.name);

    if (cp->redis_auth.len > 0) {
        string_deinit(&cp->redis_auth);
    }

    while (array_n(&cp->server) != 0) {
        conf_server_deinit(array_pop(&cp->server));
    }

    while (array_n(&cp->server_be) != 0) {
        conf_server_deinit(array_pop(&cp->server_be));
    }

    while (array_n(&cp->bucket_prop) != 0) {
        struct bucket_prop *bp = array_pop(&cp->bucket_prop);
        string_deinit(&bp->bucket);
        string_deinit(&bp->datatype);
    }

    array_deinit(&cp->server);
    array_deinit(&cp->server_be);
    array_deinit(&cp->bucket_prop);

    log_debug(LOG_VVERB, "deinit conf pool %p", cp);
}

rstatus_t
conf_pool_each_transform(void *elem, void *data)
{
    rstatus_t status;
    struct conf_pool *cp = elem;
    struct array *server_pool = data;
    struct server_pool *sp;

    ASSERT(cp->valid);

    sp = array_push(server_pool);
    ASSERT(sp != NULL);

    sp->idx = array_idx(server_pool, sp);
    sp->ctx = NULL;

    sp->p_conn = NULL;
    sp->nc_conn_q = 0;
    TAILQ_INIT(&sp->c_conn_q);

    array_null(&sp->frontends.server_arr);
    sp->frontends.owner = sp;
    sp->frontends.ncontinuum = 0;
    sp->frontends.nserver_continuum = 0;
    sp->frontends.continuum = NULL;
    sp->frontends.nlive_server = 0;
    sp->frontends.next_rebuild = 0LL;

    array_null(&sp->backends.server_arr);
    sp->backends.owner = sp;
    sp->backends.ncontinuum = 0;
    sp->backends.nserver_continuum = 0;
    sp->backends.continuum = NULL;
    sp->backends.nlive_server = 0;
    sp->backends.next_rebuild = 0LL;

    sp->name = cp->name;
    sp->addrstr = cp->listen.pname;
    sp->port = (uint16_t)cp->listen.port;

    sp->family = cp->listen.info.family;
    sp->addrlen = cp->listen.info.addrlen;
    sp->addr = (struct sockaddr *)&cp->listen.info.addr;
    sp->perm = cp->listen.perm;

    sp->key_hash_type = cp->hash;
    sp->key_hash = hash_algos[cp->hash];
    sp->dist_type = cp->distribution;
    sp->hash_tag = cp->hash_tag;

    sp->redis = cp->redis ? 1 : 0;
    sp->redis_auth = cp->redis_auth;
    sp->redis_db = cp->redis_db;
    sp->timeout = cp->timeout;
    sp->backlog = cp->backlog;

    sp->client_connections = (uint32_t)cp->client_connections;

    sp->server_connections = (uint32_t)cp->server_connections;
    sp->server_retry_timeout = (int64_t)cp->server_retry_timeout * 1000LL;
    sp->server_failure_limit = (uint32_t)cp->server_failure_limit;
    sp->server_ttl_ms = (uint32_t)cp->server_ttl_ms;
    sp->auto_eject_hosts = cp->auto_eject_hosts ? 1 : 0;
    sp->preconnect = cp->preconnect ? 1 : 0;

    status = server_init(&sp->frontends.server_arr, &cp->server, sp);
    if (status != NC_OK) {
        return status;
    }

    /*
    * Check the number of backend servers before initialization.
    * Unlike frontend servers, we allow this to be zero, since we
    * don't require backing servers at all.
    */

    sp->backend_opt.type = cp->backend_type;
    sp->backend_opt.max_resend = cp->backend_max_resend;
    sp->backend_opt.riak_r = cp->backend_riak_r;
    sp->backend_opt.riak_pr = cp->backend_riak_pr;
    sp->backend_opt.riak_w = cp->backend_riak_w;
    sp->backend_opt.riak_pw = cp->backend_riak_pw;
    sp->backend_opt.riak_n = cp->backend_riak_n;
    sp->backend_opt.riak_basic_quorum = cp->backend_riak_basic_quorum;
    sp->backend_opt.riak_sloppy_quorum = cp->backend_riak_sloppy_quorum;
    sp->backend_opt.riak_notfound_ok = cp->backend_riak_notfound_ok;
    sp->backend_opt.riak_deletedvclock = cp->backend_riak_deletedvclock;
    sp->backend_opt.riak_timeout = cp->backend_riak_timeout;
    array_null(&sp->backend_opt.bucket_prop);
    /* move buckets properties */
    uint32_t nbucket_prop = array_n(&cp->bucket_prop);
    if (nbucket_prop > 0) {
        array_init(&sp->backend_opt.bucket_prop, nbucket_prop, sizeof(struct bucket_prop));
        uint32_t i, nelem;
        for (i = 0, nelem = array_n(&cp->bucket_prop); i < nelem; i++) {
            struct bucket_prop *bp = array_get(&cp->bucket_prop, i);
            struct bucket_prop *nbp = array_push(&sp->backend_opt.bucket_prop);
            if (bp->ttl_ms == CONF_UNSET_NUM) {
                nbp->ttl_ms = cp->server_ttl_ms;
            } else {
                nbp->ttl_ms = bp->ttl_ms;
            }
            nbp->datatype = bp->datatype;
            nbp->bucket = bp->bucket;
        }
    }
    ASSERT(array_n(&sp->backend_opt.bucket_prop) == nbucket_prop);

    uint32_t nbackend_server = array_n(&cp->server_be);

    if (nbackend_server > 0) {
      status = server_init(&sp->backends.server_arr, &cp->server_be, sp);
      if (status != NC_OK) {
        return status;
      }
    }

    log_debug(LOG_VERB, "transform to pool %"PRIu32" '%.*s'", sp->idx,
              sp->name.len, sp->name.data);

    return NC_OK;
}

static void
conf_dump(struct conf *cf)
{
    uint32_t i, j, npool, nserver, nbackend_server, nbucket_prop;
    struct conf_pool *cp;
    struct string *s;
    struct bucket_prop *bp;

    npool = array_n(&cf->pool);
    if (npool == 0) {
        return;
    }

    log_debug(LOG_VVERB, "%"PRIu32" pools in configuration file '%s'", npool,
              cf->fname);

    for (i = 0; i < npool; i++) {
        cp = array_get(&cf->pool, i);

        log_debug(LOG_VVERB, "%.*s", cp->name.len, cp->name.data);
        log_debug(LOG_VVERB, "  listen: %.*s",
                  cp->listen.pname.len, cp->listen.pname.data);
        log_debug(LOG_VVERB, "  timeout: %d", cp->timeout);
        log_debug(LOG_VVERB, "  backlog: %d", cp->backlog);
        log_debug(LOG_VVERB, "  hash: %d", cp->hash);
        log_debug(LOG_VVERB, "  hash_tag: \"%.*s\"", cp->hash_tag.len,
                  cp->hash_tag.data);
        log_debug(LOG_VVERB, "  distribution: %d", cp->distribution);
        log_debug(LOG_VVERB, "  client_connections: %d",
                  cp->client_connections);
        log_debug(LOG_VVERB, "  redis: %d", cp->redis);
        log_debug(LOG_VVERB, "  preconnect: %d", cp->preconnect);
        log_debug(LOG_VVERB, "  auto_eject_hosts: %d", cp->auto_eject_hosts);
        log_debug(LOG_VVERB, "  server_connections: %d",
                  cp->server_connections);
        log_debug(LOG_VVERB, "  server_retry_timeout: %d",
                  cp->server_retry_timeout);
        log_debug(LOG_VVERB, "  server_failure_limit: %d",
                  cp->server_failure_limit);

        nserver = array_n(&cp->server);
        log_debug(LOG_VVERB, "  servers: %"PRIu32"", nserver);

        for (j = 0; j < nserver; j++) {
            s = array_get(&cp->server, j);
            log_debug(LOG_VVERB, "    %.*s", s->len, s->data);
        }

        nbackend_server = array_n(&cp->server_be);
        log_debug(LOG_VVERB, "  backend servers: %"PRIu32"", nbackend_server);

        for (j = 0; j < nbackend_server; j++) {
            s = array_get(&cp->server_be, j);
            log_debug(LOG_VVERB, "    %.*s", s->len, s->data);
        }

        nbucket_prop = array_n(&cp->bucket_prop);
        log_debug(LOG_VVERB, "  buckets properties: %"PRIu32"", nbucket_prop);
        for (j = 0; j < nbucket_prop; j++) {
            bp = array_get(&cp->bucket_prop, j);
            log_debug(LOG_VVERB, "    %.*s:%.*s ttl:%"PRIi64" ms",
                      bp->datatype.len, bp->datatype.data,
                      bp->bucket.len, bp->bucket.data,
                      bp->ttl_ms);
        }
    }
}

static rstatus_t
conf_yaml_init(struct conf *cf)
{
    int rv;

    ASSERT(!cf->valid_parser);

    rv = fseek(cf->fh, 0L, SEEK_SET);
    if (rv < 0) {
        log_error("conf: failed to seek to the beginning of file '%s': %s",
                  cf->fname, strerror(errno));
        return NC_ERROR;
    }

    rv = yaml_parser_initialize(&cf->parser);
    if (!rv) {
        log_error("conf: failed (err %d) to initialize yaml parser",
                  cf->parser.error);
        return NC_ERROR;
    }

    yaml_parser_set_input_file(&cf->parser, cf->fh);
    cf->valid_parser = 1;

    return NC_OK;
}

static void
conf_yaml_deinit(struct conf *cf)
{
    if (cf->valid_parser) {
        yaml_parser_delete(&cf->parser);
        cf->valid_parser = 0;
    }
}

static rstatus_t
conf_token_next(struct conf *cf)
{
    int rv;

    ASSERT(cf->valid_parser && !cf->valid_token);

    rv = yaml_parser_scan(&cf->parser, &cf->token);
    if (!rv) {
        log_error("conf: failed (err %d) to scan next token", cf->parser.error);
        return NC_ERROR;
    }
    cf->valid_token = 1;

    return NC_OK;
}

static void
conf_token_done(struct conf *cf)
{
    ASSERT(cf->valid_parser);

    if (cf->valid_token) {
        yaml_token_delete(&cf->token);
        cf->valid_token = 0;
    }
}

static rstatus_t
conf_event_next(struct conf *cf)
{
    int rv;

    ASSERT(cf->valid_parser && !cf->valid_event);

    rv = yaml_parser_parse(&cf->parser, &cf->event);
    if (!rv) {
        log_error("conf: failed (err %d) to get next event", cf->parser.error);
        return NC_ERROR;
    }
    cf->valid_event = 1;

    return NC_OK;
}

static void
conf_event_done(struct conf *cf)
{
    if (cf->valid_event) {
        yaml_event_delete(&cf->event);
        cf->valid_event = 0;
    }
}

static rstatus_t
conf_push_scalar(struct conf *cf)
{
    rstatus_t status;
    struct string *value;
    uint8_t *scalar;
    uint32_t scalar_len;

    scalar = cf->event.data.scalar.value;
    scalar_len = (uint32_t)cf->event.data.scalar.length;

    log_debug(LOG_VVERB, "push '%.*s'", scalar_len, scalar);

    value = array_push(&cf->arg);
    if (value == NULL) {
        return NC_ENOMEM;
    }
    string_init(value);

    status = string_copy(value, scalar, scalar_len);
    if (status != NC_OK) {
        array_pop(&cf->arg);
        return status;
    }

    return NC_OK;
}

static void
conf_pop_scalar(struct conf *cf)
{
    struct string *value;

    value = array_pop(&cf->arg);
    log_debug(LOG_VVERB, "pop '%.*s'", value->len, value->data);
    string_deinit(value);
}

static rstatus_t
conf_handler(struct conf *cf, void *data)
{
    struct command *cmd;
    struct string *key, *value;
    uint32_t narg;

    if (array_n(&cf->arg) == 1) {
        value = array_top(&cf->arg);
        log_debug(LOG_VVERB, "(1) conf handler on '%.*s'", value->len, value->data);
        return conf_pool_init(data, value);
    }

    narg = array_n(&cf->arg);
    value = array_get(&cf->arg, narg - 1);
    key = array_get(&cf->arg, narg - 2);

    log_debug(LOG_VVERB, "(2) conf handler on %.*s: %.*s", key->len, key->data,
              value->len, value->data);

    for (cmd = conf_commands; cmd->name.len != 0; cmd++) {
        char *rv = CONF_OK;

        if (string_compare(key, &cmd->name) != 0) {
            continue;
        }

        rv = cmd->set(cf, cmd, data);
        if (rv != CONF_OK) {
            log_error("conf: directive \"%.*s\" %s", key->len, key->data, rv);
            return NC_ERROR;
        }

        return NC_OK;
    }

    log_error("conf: directive \"%.*s\" is unknown", key->len, key->data);

    return NC_ERROR;
}

static rstatus_t
conf_begin_parse(struct conf *cf)
{
    rstatus_t status;
    bool done;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(cf->depth == 0);

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    done = false;
    do {
        status = conf_event_next(cf);
        if (status != NC_OK) {
            return status;
        }

        log_debug(LOG_VVERB, "next begin event %d", cf->event.type);

        switch (cf->event.type) {
        case YAML_STREAM_START_EVENT:
        case YAML_DOCUMENT_START_EVENT:
            break;

        case YAML_MAPPING_START_EVENT:
            ASSERT(cf->depth < CONF_MAX_DEPTH);
            cf->depth++;
            done = true;
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);

    } while (!done);

    return NC_OK;
}

static rstatus_t
conf_end_parse(struct conf *cf)
{
    rstatus_t status;
    bool done;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(cf->depth == 0);

    done = false;
    do {
        status = conf_event_next(cf);
        if (status != NC_OK) {
            return status;
        }

        log_debug(LOG_VVERB, "next end event %d", cf->event.type);

        switch (cf->event.type) {
        case YAML_STREAM_END_EVENT:
            done = true;
            break;

        case YAML_DOCUMENT_END_EVENT:
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);
    } while (!done);

    conf_yaml_deinit(cf);

    return NC_OK;
}

static rstatus_t
conf_parse_core(struct conf *cf, void *data)
{
    rstatus_t status;
    bool done, leaf, new_pool;

    ASSERT(cf->sound);

    status = conf_event_next(cf);
    if (status != NC_OK) {
        return status;
    }

    log_debug(LOG_VVERB, "next event %d depth %"PRIu32" seq %d", cf->event.type,
              cf->depth, cf->seq);

    done = false;
    leaf = false;
    new_pool = false;

    switch (cf->event.type) {
    case YAML_MAPPING_END_EVENT:
        cf->depth--;
        if (cf->depth == 1) {
            conf_pop_scalar(cf);
        } else if (cf->depth == 0) {
            done = true;
        }
        break;

    case YAML_MAPPING_START_EVENT:
        cf->depth++;
        break;

    case YAML_SEQUENCE_START_EVENT:
        cf->seq = 1;
        break;

    case YAML_SEQUENCE_END_EVENT:
        conf_pop_scalar(cf);
        cf->seq = 0;
        break;

    case YAML_SCALAR_EVENT:
        status = conf_push_scalar(cf);
        if (status != NC_OK) {
            break;
        }

        /* take appropriate action */
        if (cf->seq) {
            /* for a sequence, leaf is at CONF_MAX_DEPTH or CONF_SEQ_DEPTH */
            ASSERT(cf->depth == CONF_MAX_DEPTH || cf->depth == CONF_SEQ_DEPTH);
            leaf = true;
        } else if (cf->depth == CONF_ROOT_DEPTH) {
            /* create new conf_pool */
            data = array_push(&cf->pool);
            if (data == NULL) {
                status = NC_ENOMEM;
                break;
           }
           new_pool = true;
        } else if (array_n(&cf->arg) == cf->depth + 1) {
            /* for {key: value}, leaf is at CONF_MAX_DEPTH or CONF_SEQ_DEPTH */
            ASSERT(cf->depth == CONF_MAX_DEPTH || cf->depth == CONF_SEQ_DEPTH);
            leaf = true;
        }
        break;

    default:
        NOT_REACHED();
        break;
    }

    conf_event_done(cf);

    if (status != NC_OK) {
        return status;
    }

    if (done) {
        /* terminating condition */
        return NC_OK;
    }

    if (leaf || new_pool) {
        status = conf_handler(cf, data);

        if (leaf) {
            conf_pop_scalar(cf);
            if (!cf->seq) {
                conf_pop_scalar(cf);
            }
        }

        if (status != NC_OK) {
            return status;
        }
    }

    return conf_parse_core(cf, data);
}

static rstatus_t
conf_parse(struct conf *cf)
{
    rstatus_t status;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(array_n(&cf->arg) == 0);

    status = conf_begin_parse(cf);
    if (status != NC_OK) {
        return status;
    }

    status = conf_parse_core(cf, NULL);
    if (status != NC_OK) {
        return status;
    }

    status = conf_end_parse(cf);
    if (status != NC_OK) {
        return status;
    }

    cf->parsed = 1;

    return NC_OK;
}

static struct conf *
conf_open(char *filename)
{
    rstatus_t status;
    struct conf *cf;
    FILE *fh;

    fh = fopen(filename, "r");
    if (fh == NULL) {
        log_error("conf: failed to open configuration '%s': %s", filename,
                  strerror(errno));
        return NULL;
    }

    cf = nc_alloc(sizeof(*cf));
    if (cf == NULL) {
        fclose(fh);
        return NULL;
    }

    status = array_init(&cf->arg, CONF_DEFAULT_ARGS, sizeof(struct string));
    if (status != NC_OK) {
        nc_free(cf);
        fclose(fh);
        return NULL;
    }

    status = array_init(&cf->pool, CONF_DEFAULT_POOL, sizeof(struct conf_pool));
    if (status != NC_OK) {
        array_deinit(&cf->arg);
        nc_free(cf);
        fclose(fh);
        return NULL;
    }

    cf->fname = filename;
    cf->fh = fh;
    cf->depth = 0;
    /* parser, event, and token are initialized later */
    cf->seq = 0;
    cf->valid_parser = 0;
    cf->valid_event = 0;
    cf->valid_token = 0;
    cf->sound = 0;
    cf->parsed = 0;
    cf->valid = 0;

    log_debug(LOG_VVERB, "opened conf '%s'", filename);

    return cf;
}

static rstatus_t
conf_validate_document(struct conf *cf)
{
    rstatus_t status;
    uint32_t count;
    bool done;

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    count = 0;
    done = false;
    do {
        yaml_document_t document;
        yaml_node_t *node;
        int rv;

        rv = yaml_parser_load(&cf->parser, &document);
        if (!rv) {
            log_error("conf: failed (err %d) to get the next yaml document",
                      cf->parser.error);
            conf_yaml_deinit(cf);
            return NC_ERROR;
        }

        node = yaml_document_get_root_node(&document);
        if (node == NULL) {
            done = true;
        } else {
            count++;
        }

        yaml_document_delete(&document);
    } while (!done);

    conf_yaml_deinit(cf);

    if (count != 1) {
        log_error("conf: '%s' must contain only 1 document; found %"PRIu32" "
                  "documents", cf->fname, count);
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
conf_validate_tokens(struct conf *cf)
{
    rstatus_t status;
    bool done, error;
    int type;

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    done = false;
    error = false;
    do {
        status = conf_token_next(cf);
        if (status != NC_OK) {
            return status;
        }
        type = cf->token.type;

        switch (type) {
        case YAML_NO_TOKEN:
            error = true;
            log_error("conf: no token (%d) is disallowed", type);
            break;

        case YAML_VERSION_DIRECTIVE_TOKEN:
            error = true;
            log_error("conf: version directive token (%d) is disallowed", type);
            break;

        case YAML_TAG_DIRECTIVE_TOKEN:
            error = true;
            log_error("conf: tag directive token (%d) is disallowed", type);
            break;

        case YAML_DOCUMENT_START_TOKEN:
            error = true;
            log_error("conf: document start token (%d) is disallowed", type);
            break;

        case YAML_DOCUMENT_END_TOKEN:
            error = true;
            log_error("conf: document end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_SEQUENCE_START_TOKEN:
            error = true;
            log_error("conf: flow sequence start token (%d) is disallowed", type);
            break;

        case YAML_FLOW_SEQUENCE_END_TOKEN:
            error = true;
            log_error("conf: flow sequence end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_MAPPING_START_TOKEN:
            error = true;
            log_error("conf: flow mapping start token (%d) is disallowed", type);
            break;

        case YAML_FLOW_MAPPING_END_TOKEN:
            error = true;
            log_error("conf: flow mapping end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_ENTRY_TOKEN:
            error = true;
            log_error("conf: flow entry token (%d) is disallowed", type);
            break;

        case YAML_ALIAS_TOKEN:
            error = true;
            log_error("conf: alias token (%d) is disallowed", type);
            break;

        case YAML_ANCHOR_TOKEN:
            error = true;
            log_error("conf: anchor token (%d) is disallowed", type);
            break;

        case YAML_TAG_TOKEN:
            error = true;
            log_error("conf: tag token (%d) is disallowed", type);
            break;

        case YAML_BLOCK_SEQUENCE_START_TOKEN:
        case YAML_BLOCK_MAPPING_START_TOKEN:
        case YAML_BLOCK_END_TOKEN:
        case YAML_BLOCK_ENTRY_TOKEN:
            break;

        case YAML_KEY_TOKEN:
        case YAML_VALUE_TOKEN:
        case YAML_SCALAR_TOKEN:
            break;

        case YAML_STREAM_START_TOKEN:
            break;

        case YAML_STREAM_END_TOKEN:
            done = true;
            log_debug(LOG_VVERB, "conf '%s' has valid tokens", cf->fname);
            break;

        default:
            error = true;
            log_error("conf: unknown token (%d) is disallowed", type);
            break;
        }

        conf_token_done(cf);
    } while (!done && !error);

    conf_yaml_deinit(cf);

    return !error ? NC_OK : NC_ERROR;
}

static rstatus_t
conf_validate_structure(struct conf *cf)
{
    rstatus_t status;
    int type, depth, seq;
    uint32_t i, count[CONF_MAX_DEPTH + 1];
    bool done, error;

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    done = false;
    error = false;
    seq = false;
    depth = 0;
    for (i = 0; i < CONF_MAX_DEPTH + 1; i++) {
        count[i] = 0;
    }

    /*
     * Validate that the configuration conforms roughly to the following
     * yaml tree structure:
     *
     * keyx:
     *   key1: value1
     *   key2: value2
     *   seq:
     *     - elem1
     *     - elem2
     *     - elem3
     *   key3: value3
     *
     * keyy:
     *   key1: value1
     *   key2: value2
     *   seq:
     *     - elem1
     *       subkey1: value1
     *       subkey2: value1
     *     - elem2
     *     - elem3
     *   key3: value3
     */
    do {
        status = conf_event_next(cf);
        if (status != NC_OK) {
            return status;
        }

        type = cf->event.type;

        log_debug(LOG_VVERB, "next event %d depth %d seq %d", type, depth, seq);

        switch (type) {
        case YAML_STREAM_START_EVENT:
        case YAML_DOCUMENT_START_EVENT:
            break;

        case YAML_DOCUMENT_END_EVENT:
            break;

        case YAML_STREAM_END_EVENT:
            done = true;
            break;

        case YAML_MAPPING_START_EVENT:
            if (depth == CONF_ROOT_DEPTH && count[depth] != 1) {
                error = true;
                log_error("conf: '%s' has more than one \"key:value\" at depth"
                          " %d", cf->fname, depth);
            } else if (depth >= CONF_MAX_DEPTH) {
                error = true;
                log_error("conf: '%s' has a depth greater than %d", cf->fname,
                          CONF_MAX_DEPTH);
            }
            depth++;
            break;

        case YAML_MAPPING_END_EVENT:
            if (depth == CONF_SEQ_DEPTH) {
                if (seq) {
                    seq = false;
                } else {
                    error = true;
                    log_error("conf: '%s' missing sequence directive at depth "
                              "%d", cf->fname, depth);
                }
            }
            depth--;
            count[depth] = 0;
            break;

        case YAML_SEQUENCE_START_EVENT:
            if (seq > 2) {
                error = true;
                log_error("conf: '%s' has more than three sequence directives",
                          cf->fname);
            } else if (depth != CONF_MAX_DEPTH && depth != CONF_SEQ_DEPTH) {
                error = true;
                log_error("conf: '%s' has sequence at depth %d instead of %d",
                          cf->fname, depth, CONF_MAX_DEPTH);
            } else if (count[depth] != 1) {
                error = true;
                log_error("conf: '%s' has invalid \"key:value\" at depth %d",
                          cf->fname, depth);
            }
            seq++;
            break;

        case YAML_SEQUENCE_END_EVENT:
            ASSERT(depth == CONF_MAX_DEPTH || depth == CONF_SEQ_DEPTH);
            count[depth] = 0;
            break;

        case YAML_SCALAR_EVENT:
            if (depth == 0) {
                error = true;
                log_error("conf: '%s' has invalid empty \"key:\" at depth %d",
                          cf->fname, depth);
            } else if (depth == CONF_ROOT_DEPTH && count[depth] != 0) {
                error = true;
                log_error("conf: '%s' has invalid mapping \"key:\" at depth %d",
                          cf->fname, depth);
            } else if ((depth == CONF_MAX_DEPTH || depth == CONF_SEQ_DEPTH) && count[depth] == 2) {
                /* found a "key: value", resetting! */
                count[depth] = 0;
            }
            count[depth]++;
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);
    } while (!done && !error);

    conf_yaml_deinit(cf);

    return !error ? NC_OK : NC_ERROR;
}

static rstatus_t
conf_pre_validate(struct conf *cf)
{
    rstatus_t status;

    status = conf_validate_document(cf);
    if (status != NC_OK) {
        return status;
    }

    status = conf_validate_tokens(cf);
    if (status != NC_OK) {
        return status;
    }

    status = conf_validate_structure(cf);
    if (status != NC_OK) {
        return status;
    }

    cf->sound = 1;

    return NC_OK;
}

static int
conf_server_name_cmp(const void *t1, const void *t2)
{
    const struct conf_server *s1 = t1, *s2 = t2;

    return string_compare(&s1->name, &s2->name);
}

static int
conf_pool_name_cmp(const void *t1, const void *t2)
{
    const struct conf_pool *p1 = t1, *p2 = t2;

    return string_compare(&p1->name, &p2->name);
}

static int
conf_pool_listen_cmp(const void *t1, const void *t2)
{
    const struct conf_pool *p1 = t1, *p2 = t2;

    return string_compare(&p1->listen.pname, &p2->listen.pname);
}

static rstatus_t
conf_validate_server(struct conf *cf, struct conf_pool *cp)
{
    uint32_t i, nserver;
    bool valid;

    nserver = array_n(&cp->server);
    if (nserver == 0) {
        log_error("conf: pool '%.*s' has no servers", cp->name.len,
                  cp->name.data);
        return NC_ERROR;
    }

    /*
     * Disallow duplicate servers - servers with identical "host:port:weight"
     * or "name" combination are considered as duplicates. When server name
     * is configured, we only check for duplicate "name" and not for duplicate
     * "host:port:weight"
     */
    array_sort(&cp->server, conf_server_name_cmp);
    for (valid = true, i = 0; i < nserver - 1; i++) {
        struct conf_server *cs1, *cs2;

        cs1 = array_get(&cp->server, i);
        cs2 = array_get(&cp->server, i + 1);

        if (string_compare(&cs1->name, &cs2->name) == 0) {
            log_error("conf: pool '%.*s' has servers with same name '%.*s'",
                      cp->name.len, cp->name.data, cs1->name.len,
                      cs1->name.data);
            valid = false;
            break;
        }
    }
    if (!valid) {
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
conf_validate_pool(struct conf *cf, struct conf_pool *cp)
{
    rstatus_t status;

    ASSERT(!cp->valid);
    ASSERT(!string_empty(&cp->name));

    if (!cp->listen.valid) {
        log_error("conf: directive \"listen:\" is missing");
        return NC_ERROR;
    }

    /* set default values for unset directives */

    if (cp->distribution == CONF_UNSET_DIST) {
        cp->distribution = CONF_DEFAULT_DIST;
    }

    if (cp->hash == CONF_UNSET_HASH) {
        cp->hash = CONF_DEFAULT_HASH;
    }

    if (cp->timeout == CONF_UNSET_NUM) {
        cp->timeout = CONF_DEFAULT_TIMEOUT;
    }

    if (cp->backlog == CONF_UNSET_NUM) {
        cp->backlog = CONF_DEFAULT_LISTEN_BACKLOG;
    }

    cp->client_connections = CONF_DEFAULT_CLIENT_CONNECTIONS;

    if (cp->redis == CONF_UNSET_NUM) {
        cp->redis = CONF_DEFAULT_REDIS;
    }

    if (cp->redis_db == CONF_UNSET_NUM) {
        cp->redis_db = CONF_DEFAULT_REDIS_DB;
    }

    if (cp->preconnect == CONF_UNSET_NUM) {
        cp->preconnect = CONF_DEFAULT_PRECONNECT;
    }

    if (cp->auto_eject_hosts == CONF_UNSET_NUM) {
        cp->auto_eject_hosts = CONF_DEFAULT_AUTO_EJECT_HOSTS;
    }

    if (cp->server_connections == CONF_UNSET_NUM) {
        cp->server_connections = CONF_DEFAULT_SERVER_CONNECTIONS;
    } else if (cp->server_connections == 0) {
        log_error("conf: directive \"server_connections:\" cannot be 0");
        return NC_ERROR;
    }

    if (cp->server_retry_timeout == CONF_UNSET_NUM) {
        cp->server_retry_timeout = CONF_DEFAULT_SERVER_RETRY_TIMEOUT;
    }

    if (cp->server_failure_limit == CONF_UNSET_NUM) {
        cp->server_failure_limit = CONF_DEFAULT_SERVER_FAILURE_LIMIT;
    }

    if (cp->server_ttl_ms == CONF_UNSET_NUM) {
        cp->server_ttl_ms = CONF_DEFAULT_SERVER_TTL_MS;
    }

    if (cp->backend_type == CONN_UNKNOWN) {
        cp->backend_type = CONF_DEFAULT_BACKEND_TYPE;
    }

    if (cp->backend_max_resend == CONF_UNSET_NUM) {
        cp->backend_max_resend = CONF_DEFAULT_BACKEND_MAX_RESEND;
    } else if (cp->backend_max_resend == 0) {
        cp->backend_max_resend = 1;
    }

    status = conf_validate_server(cf, cp);
    if (status != NC_OK) {
        return status;
    }

    cp->valid = 1;

    return NC_OK;
}

static rstatus_t
conf_post_validate(struct conf *cf)
{
    rstatus_t status;
    uint32_t i, npool;
    bool valid;

    ASSERT(cf->sound && cf->parsed);
    ASSERT(!cf->valid);

    npool = array_n(&cf->pool);
    if (npool == 0) {
        log_error("conf: '%.*s' has no pools", cf->fname);
        return NC_ERROR;
    }

    /* validate pool */
    for (i = 0; i < npool; i++) {
        struct conf_pool *cp = array_get(&cf->pool, i);

        status = conf_validate_pool(cf, cp);
        if (status != NC_OK) {
            return status;
        }
    }

    /* disallow pools with duplicate listen: key values */
    array_sort(&cf->pool, conf_pool_listen_cmp);
    for (valid = true, i = 0; i < npool - 1; i++) {
        struct conf_pool *p1, *p2;

        p1 = array_get(&cf->pool, i);
        p2 = array_get(&cf->pool, i + 1);

        if (string_compare(&p1->listen.pname, &p2->listen.pname) == 0) {
            log_error("conf: pools '%.*s' and '%.*s' have the same listen "
                      "address '%.*s'", p1->name.len, p1->name.data,
                      p2->name.len, p2->name.data, p1->listen.pname.len,
                      p1->listen.pname.data);
            valid = false;
            break;
        }
    }
    if (!valid) {
        return NC_ERROR;
    }

    /* disallow pools with duplicate names */
    array_sort(&cf->pool, conf_pool_name_cmp);
    for (valid = true, i = 0; i < npool - 1; i++) {
        struct conf_pool *p1, *p2;

        p1 = array_get(&cf->pool, i);
        p2 = array_get(&cf->pool, i + 1);

        if (string_compare(&p1->name, &p2->name) == 0) {
            log_error("conf: '%s' has pools with same name %.*s'", cf->fname,
                      p1->name.len, p1->name.data);
            valid = false;
            break;
        }
    }
    if (!valid) {
        return NC_ERROR;
    }

    return NC_OK;
}

struct conf *
conf_create(char *filename)
{
    rstatus_t status;
    struct conf *cf;

    cf = conf_open(filename);
    if (cf == NULL) {
        return NULL;
    }

    /* validate configuration file before parsing */
    status = conf_pre_validate(cf);
    if (status != NC_OK) {
        goto error;
    }

    /* parse the configuration file */
    status = conf_parse(cf);
    if (status != NC_OK) {
        goto error;
    }

    /* validate parsed configuration */
    status = conf_post_validate(cf);
    if (status != NC_OK) {
        goto error;
    }

    conf_dump(cf);

    fclose(cf->fh);
    cf->fh = NULL;

    return cf;

error:
    fclose(cf->fh);
    cf->fh = NULL;
    conf_destroy(cf);
    return NULL;
}

void
conf_destroy(struct conf *cf)
{
    while (array_n(&cf->arg) != 0) {
        conf_pop_scalar(cf);
    }
    array_deinit(&cf->arg);

    while (array_n(&cf->pool) != 0) {
        conf_pool_deinit(array_pop(&cf->pool));
    }
    array_deinit(&cf->pool);

    nc_free(cf);
}

char *
conf_set_string(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    uint8_t *p;
    struct string *field, *value;

    p = conf;
    field = (struct string *)(p + cmd->offset);

    if (field->data != CONF_UNSET_PTR) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    status = string_duplicate(field, value);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    return CONF_OK;
}

char *
conf_set_listen(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    struct string *value;
    struct conf_listen *field;
    uint8_t *p, *name;
    uint32_t namelen;

    p = conf;
    field = (struct conf_listen *)(p + cmd->offset);

    if (field->valid == 1) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    status = string_duplicate(&field->pname, value);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    if (value->data[0] == '/') {
        uint8_t *q, *start, *perm;

        /* parse "socket_path permissions" from the end */
        p = value->data + value->len -1;
        start = value->data;
        q = nc_strrchr(p, start, ' ');
        if (q == NULL) {
            /* no permissions field, so use defaults */
            name = value->data;
            namelen = value->len;
        } else {
            perm = q + 1;

            p = q - 1;
            name = start;
            namelen = (uint32_t)(p - start + 1);

            errno = 0;
            field->perm = (mode_t)strtol((char *)perm, NULL, 8);
            if (errno || field->perm > 0777) {
                return "has an invalid file permission in \"socket_path permission\" format string";
            }
        }
    } else {
        uint8_t *q, *start, *port;
        uint32_t portlen;

        /* parse "hostname:port" from the end */
        p = value->data + value->len - 1;
        start = value->data;
        q = nc_strrchr(p, start, ':');
        if (q == NULL) {
            return "has an invalid \"hostname:port\" format string";
        }

        port = q + 1;
        portlen = (uint32_t)(p - port + 1);

        p = q - 1;

        name = start;
        namelen = (uint32_t)(p - start + 1);

        field->port = nc_atoi(port, portlen);
        if (field->port < 0 || !nc_valid_port(field->port)) {
            return "has an invalid port in \"hostname:port\" format string";
        }
    }

    status = string_copy(&field->name, name, namelen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    status = nc_resolve(&field->name, field->port, &field->info);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    field->valid = 1;

    return CONF_OK;
}

char *
conf_add_server_(struct conf *cf, struct command *cmd, void *conf, bool backend)
{
    rstatus_t status;
    struct array *a;
    struct string *value;
    struct conf_server *field;
    uint8_t *p, *q, *start;
    uint8_t *pname, *addr, *port, *weight, *name;
    uint32_t k, delimlen, pnamelen, addrlen, portlen, weightlen, namelen;
    struct string address;
    char delim[] = " ::";

    string_init(&address);
    p = conf;
    a = (struct array *)(p + cmd->offset);

    field = array_push(a);
    if (field == NULL) {
        return CONF_ERROR;
    }

    conf_server_init(field);

    field->backend = backend;

    value = array_top(&cf->arg);

    /* parse "hostname:port:weight [name]" or "/path/unix_socket:weight [name]" from the end */
    p = value->data + value->len - 1;
    start = value->data;
    addr = NULL;
    addrlen = 0;
    weight = NULL;
    weightlen = 0;
    port = NULL;
    portlen = 0;
    name = NULL;
    namelen = 0;

    delimlen = value->data[0] == '/' ? 2 : 3;

    for (k = 0; k < sizeof(delim); k++) {
        q = nc_strrchr(p, start, delim[k]);
        if (q == NULL) {
            if (k == 0) {
                /*
                 * name in "hostname:port:weight [name]" format string is
                 * optional
                 */
                continue;
            }
            break;
        }

        switch (k) {
        case 0:
            name = q + 1;
            namelen = (uint32_t)(p - name + 1);
            break;

        case 1:
            weight = q + 1;
            weightlen = (uint32_t)(p - weight + 1);
            break;

        case 2:
            port = q + 1;
            portlen = (uint32_t)(p - port + 1);
            break;

        default:
            NOT_REACHED();
        }

        p = q - 1;
    }

    if (k != delimlen) {
        return "has an invalid \"hostname:port:weight [name]\"or \"/path/unix_socket:weight [name]\" format string";
    }

    pname = value->data;
    pnamelen = namelen > 0 ? value->len - (namelen + 1) : value->len;
    status = string_copy(&field->pname, pname, pnamelen);
    if (status != NC_OK) {
        array_pop(a);
        return CONF_ERROR;
    }

    addr = start;
    addrlen = (uint32_t)(p - start + 1);

    field->weight = nc_atoi(weight, weightlen);
    if (field->weight < 0) {
        return "has an invalid weight in \"hostname:port:weight [name]\" format string";
    } else if (field->weight == 0) {
        return "has a zero weight in \"hostname:port:weight [name]\" format string";
    }

    if (value->data[0] != '/') {
        field->port = nc_atoi(port, portlen);
        if (field->port < 0 || !nc_valid_port(field->port)) {
            return "has an invalid port in \"hostname:port:weight [name]\" format string";
        }
    }

    if (name == NULL) {
        /*
         * To maintain backward compatibility with libmemcached, we don't
         * include the port as the part of the input string to the consistent
         * hashing algorithm, when it is equal to 11211.
         */
        if (field->port == CONF_DEFAULT_KETAMA_PORT) {
            name = addr;
            namelen = addrlen;
        } else {
            name = addr;
            namelen = addrlen + 1 + portlen;
        }
    }

    status = string_copy(&field->name, name, namelen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    status = string_copy(&address, addr, addrlen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    status = nc_resolve(&address, field->port, &field->info);
    if (status != NC_OK) {
        string_deinit(&address);
        return CONF_ERROR;
    }

    string_deinit(&address);
    field->valid = 1;

    return CONF_OK;
}

char * 
conf_add_server(struct conf *cf, struct command *cmd, void *conf)
{
    return conf_add_server_(cf, cmd, conf, false);
}

char *
conf_add_server_be(struct conf *cf, struct command *cmd, void *conf)
{
    return conf_add_server_(cf, cmd, conf, true);
}

char *
conf_add_bucket_prop(struct conf *cf, struct command *cmd, void *conf)
{
    typedef enum {
        BPR_NONE,
        BPR_TTL
    } BP_READSTATE;

    const struct string ttl_str = string("ttl");
    struct array *a;
    struct string value;
    struct bucket_prop *field;
    struct string *name;

    a = (struct array *)((uint8_t *)conf + cmd->offset);
    name = array_top(&cf->arg);
    if (name->len == 0) {
        return CONF_ERROR;
    }

    field = array_push(a);
    if (field == NULL) {
        return CONF_ERROR;
    }

    if (!nc_parse_datatype_bucket(name->data, name->len, &field->datatype,
                                  &field->bucket)) {
        array_pop(a);
        return CONF_ERROR;
    }
    // Init default values for fields
    field->ttl_ms = CONF_UNSET_NUM;

    bool done = false;
    bool error = false;
    BP_READSTATE state = BPR_NONE;
    do {
        conf_event_done(cf);
        conf_event_next(cf);
        switch (cf->event.type) {
        case YAML_MAPPING_END_EVENT:
            cf->depth--;
            conf_event_done(cf);
            done = true;
            break;
        case YAML_SCALAR_EVENT:
            value.data = cf->event.data.scalar.value;
            value.len = (uint32_t)cf->event.data.scalar.length;
            switch (state) {
            case BPR_NONE:
                if (string_compare(&value, &ttl_str) == 0) {
                    state = BPR_TTL;
                } else if (value.len) {
                    error = true;
                }
                break;
            case BPR_TTL:
                error = !nc_read_ttl_value(&value, &field->ttl_ms);
                state = BPR_NONE;
                break;
            }
            break;
        default:
            break;
        }
    } while (!done && !error);

    if (error) {
        array_pop(a);
        return CONF_ERROR;
    }
    return CONF_OK;
}

char*
conf_set_server_ttl(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    int64_t *np;

    struct string *value;

    p = conf;
    np = (int64_t *)(p + cmd->offset);

    if (*np != CONF_UNSET_NUM) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    return nc_read_ttl_value(value, np) ? CONF_OK : CONF_ERROR;
}

char *
conf_set_num(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    int num, *np;
    struct string *value;

    p = conf;
    np = (int *)(p + cmd->offset);

    if (*np != CONF_UNSET_NUM) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    num = nc_atoi(value->data, value->len);
    if (num < 0) {
        return "is not a number";
    }

    *np = num;

    return CONF_OK;
}

char *
conf_set_bool(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    int *bp;
    struct string *value, true_str, false_str;

    p = conf;
    bp = (int *)(p + cmd->offset);

    if (*bp != CONF_UNSET_NUM) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);
    string_set_text(&true_str, "true");
    string_set_text(&false_str, "false");

    if (string_compare(value, &true_str) == 0) {
        *bp = 1;
    } else if (string_compare(value, &false_str) == 0) {
        *bp = 0;
    } else {
        return "is not \"true\" or \"false\"";
    }

    return CONF_OK;
}

char *
conf_set_hash(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    hash_type_t *hp;
    struct string *value, *hash;

    p = conf;
    hp = (hash_type_t *)(p + cmd->offset);

    if (*hp != CONF_UNSET_HASH) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    for (hash = hash_strings; hash->len != 0; hash++) {
        if (string_compare(value, hash) != 0) {
            continue;
        }

        *hp = (hash_type_t)(hash - hash_strings);

        return CONF_OK;
    }

    return "is not a valid hash";
}

char *
conf_set_distribution(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    dist_type_t *dp;
    struct string *value, *dist;

    p = conf;
    dp = (dist_type_t *)(p + cmd->offset);

    if (*dp != CONF_UNSET_DIST) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    for (dist = dist_strings; dist->len != 0; dist++) {
        if (string_compare(value, dist) != 0) {
            continue;
        }

        *dp = (dist_type_t)(dist - dist_strings);

        return CONF_OK;
    }

    return "is not a valid distribution";
}

char *
conf_set_hashtag(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    uint8_t *p;
    struct string *field, *value;

    p = conf;
    field = (struct string *)(p + cmd->offset);

    if (field->data != CONF_UNSET_PTR) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    if (value->len != 2) {
        return "is not a valid hash tag string with two characters";
    }

    status = string_duplicate(field, value);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    return CONF_OK;
}

char *
conf_set_backend_type(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    connection_type_t* bp;
    struct string *value, *backend;

    p = conf;
    bp = (connection_type_t *)(p + cmd->offset);

    value = array_top(&cf->arg);

    for (backend = connection_strings; backend->len != 0; backend++)
    {
        if (string_compare(value, backend) != 0) {
            continue;
        }

        *bp = (connection_type_t)(backend - connection_strings);

        return CONF_OK;
    }

    return "is not a valid backend type";
}

static bool
conf_write_key_value_string(yaml_emitter_t *emitter, const char *key,
                            const struct string *value)
{
    yaml_event_t event;
    if (value->len == 0 || value->data == NULL) {
        return true;
    }

    /* write key */
    if (!yaml_scalar_event_initialize(&event, NULL, NULL, (yaml_char_t *)key,
                                      (int)nc_strlen(key), 1, 0,
                                      YAML_PLAIN_SCALAR_STYLE)) {
        log_error("conf: failed to init scalar event");
         return false;
    }
    if (!yaml_emitter_emit(emitter, &event)) {
        log_error("conf: failed to write yaml event");
        return false;
    }
    /* write value */
    if (!yaml_scalar_event_initialize(&event, NULL, NULL, value->data,
                                      (int)value->len, 1, 0,
                                      YAML_PLAIN_SCALAR_STYLE)) {
        log_error("conf: failed to init scalar event");
         return false;
    }
    if (!yaml_emitter_emit(emitter, &event)) {
        log_error("conf: failed to write yaml event");
        return false;
    }

    return true;
}

static bool
conf_write_key_value_int(yaml_emitter_t *emitter, const char *key, int value)
{
    uint8_t intbuf[16];
    if (value == CONF_UNSET_NUM) {
        return true;
    }
    const uint32_t len = (uint32_t)sprintf((char *)intbuf, "%i", value);
    struct string str = {len, intbuf};
    return conf_write_key_value_string(emitter, key, &str);
}

static bool
conf_write_key_value_bool(yaml_emitter_t *emitter, const char *key, bool value)
{
    const static struct string truestr = string("true");
    const static struct string falsestr = string("false");
    const struct string *b = value ? &truestr : &falsestr;
    return conf_write_key_value_string(emitter, key, b);
}

static bool
conf_write_key_value_time(yaml_emitter_t *emitter, const char *key,
                          int64_t time)
{
    struct string str;
    if (!nc_ttl_value_to_string(&str, time)) {
        return false;
    }
    bool res = conf_write_key_value_string(emitter, key, &str);
    string_deinit(&str);
    return res;
}

static bool
conf_write_buckets_props(yaml_emitter_t *emitter, const char *name,
                         struct array *bpa)
{
    uint32_t i;
    bool res = true;
    yaml_event_t event;
    if (array_n(bpa) == 0) {
        return true;
    }
    /* write name */
    if (!yaml_scalar_event_initialize(&event, NULL, NULL, (yaml_char_t *)name,
                                      (int)nc_strlen(name), 1, 0,
                                      YAML_PLAIN_SCALAR_STYLE)) {
        log_error("conf: failed to init scalar event");
        return false;
    }
    if (!yaml_emitter_emit(emitter, &event)) {
        log_error("conf: failed to write yaml event");
        return false;
    }
    /* write list */
    if (!yaml_sequence_start_event_initialize(&event, NULL, NULL, 1,
                                              YAML_BLOCK_SEQUENCE_STYLE)) {
        log_error("conf: failed to initialize yaml sequence");
        return false;
    }
    if (!yaml_emitter_emit(emitter, &event)) {
        log_error("conf: failed to write yaml event");
        return false;
    }
    for (i = 0; i < array_n(bpa); i++) {
        struct bucket_prop *bp = array_get(bpa, i);
        char bucketname[bp->datatype.len + bp->bucket.len + 2];
        int len = sprintf(bucketname, "%.*s:%.*s", bp->datatype.len,
                          bp->datatype.data, bp->bucket.len,
                          bp->bucket.data);
        /* start bucket properties list */
        if (!yaml_mapping_start_event_initialize(&event, NULL, NULL, 1,
                                                 YAML_BLOCK_MAPPING_STYLE)) {
            log_error("conf: failed to initialize yaml mapping");
            res = false;
            break;
        }
        if (!yaml_emitter_emit(emitter, &event)) {
            log_error("conf: failed to write yaml event");
            res = false;
            break;
        }

        /* write bucket name */
        if (!yaml_scalar_event_initialize(&event, NULL, NULL,
                                          (yaml_char_t *)bucketname, len,
                                          1, 0, YAML_PLAIN_SCALAR_STYLE)) {
            log_error("conf: failed to init scalar event");
            res = false;
            break;
        }
        if (!yaml_emitter_emit(emitter, &event)) {
            log_error("conf: failed to write yaml event");
            res = false;
            break;
        }
        /* empty value for bucket name */
        if (!yaml_scalar_event_initialize(&event, NULL, NULL,
                                          (yaml_char_t *)"", 0,
                                          1, 0, YAML_PLAIN_SCALAR_STYLE)) {
            log_error("conf: failed to init scalar event");
            res = false;
            break;
        }
        if (!yaml_emitter_emit(emitter, &event)) {
            log_error("conf: failed to write yaml event");
            res = false;
            break;
        }

        /* write each bucket props */
        conf_write_key_value_time(emitter, "ttl", bp->ttl_ms);

        /* close bucket properties list */
        if (!yaml_mapping_end_event_initialize(&event)) {
            log_error("conf: failed to end yaml mapping");
            res = false;
            break;
        }
        if (!yaml_emitter_emit(emitter, &event)) {
            log_error("conf: failed to write yaml event");
            res = false;
            break;
        }
    }
    /* close list */
    if (!yaml_sequence_end_event_initialize(&event)) {
        log_error("conf: failed to end yaml sequence");
        return false;
    }
    if (!yaml_emitter_emit(emitter, &event)) {
        log_error("conf: failed to write yaml event");
        return false;
    }
    return res;
}

static bool
conf_write_servers(yaml_emitter_t *emitter, const char *name,
                   struct servers *servers)
{
    uint32_t i;
    bool res = true;
    yaml_event_t event;
    if (array_n(&servers->server_arr) == 0) {
        return true;
    }
    /* write name */
    if (!yaml_scalar_event_initialize(&event, NULL, NULL, (yaml_char_t *)name,
                                      (int)nc_strlen(name), 1, 0,
                                      YAML_PLAIN_SCALAR_STYLE)) {
        log_error("conf: failed to init scalar event");
        return false;
    }
    if (!yaml_emitter_emit(emitter, &event)) {
        log_error("conf: failed to write yaml event");
        return false;
    }
    /* write list */
    if (!yaml_sequence_start_event_initialize(&event, NULL, NULL, 1,
                                              YAML_BLOCK_SEQUENCE_STYLE)) {
        log_error("conf: failed to initialize yaml sequence");
        return false;
    }
    if (!yaml_emitter_emit(emitter, &event)) {
        log_error("conf: failed to write yaml event");
        return false;
    }

    for (i = 0; i < array_n(&servers->server_arr); i++) {
        struct server *server = array_get(&servers->server_arr, i);
        if (!yaml_scalar_event_initialize(&event, NULL, NULL,
                                          (yaml_char_t *)server->pname.data,
                                          (int)server->pname.len, 1, 0,
                                          YAML_PLAIN_SCALAR_STYLE)) {
            log_error("conf: failed to init scalar event");
            res = false;
            break;
        }
        if (!yaml_emitter_emit(emitter, &event)) {
            log_error("conf: failed to write yaml event");
            res = false;
            break;
        }
    }
    /* close list */
    if (!yaml_sequence_end_event_initialize(&event)) {
        log_error("conf: failed to end yaml sequence");
        return false;
    }
    if (!yaml_emitter_emit(emitter, &event)) {
        log_error("conf: failed to write yaml event");
        return false;
    }
    return res;
}

static bool
conf_write_pool(yaml_emitter_t *emitter, struct server_pool *pool)
{
    bool res;
    yaml_event_t event;

    /* write pool name */
    if (!yaml_scalar_event_initialize(&event, NULL, NULL, pool->name.data,
                                      (int)pool->name.len, 1, 0,
                                      YAML_PLAIN_SCALAR_STYLE)) {
        log_error("conf: failed to init scalar event");
         return false;
    }
    if (!yaml_emitter_emit(emitter, &event)) {
        log_error("conf: failed to write yaml event");
        return false;
    }
    if (!yaml_mapping_start_event_initialize(&event, NULL, NULL, 1,
                                             YAML_BLOCK_MAPPING_STYLE)) {
        log_error("conf: failed to initialize yaml mapping");
        return false;
    }
    if (!yaml_emitter_emit(emitter, &event)) {
        log_error("conf: failed to write yaml event");
        return false;
    }

    /* write pool properties */
    res = conf_write_key_value_string(emitter, "listen", &pool->addrstr);
    if(res) {
       if (pool->key_hash_type != (int)CONF_UNSET_HASH) {
           res = conf_write_key_value_string(emitter, "hash",
                                           &hash_strings[pool->key_hash_type]);
       }
    }
    if(res) {
        res = conf_write_key_value_string(emitter, "hash_tag",
                                          &pool->hash_tag);
    }
    if(res) {
        if (pool->dist_type != (int)CONF_UNSET_DIST) {
            res = conf_write_key_value_string(emitter, "distribution",
                                              &dist_strings[pool->dist_type]);
        }
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "timeout", pool->timeout);
    }
    if(res) {
       res = conf_write_key_value_int(emitter, "backlog", pool->backlog);
    }
    if(res) {
       res = conf_write_key_value_int(emitter, "client_connections",
                                      (int)pool->client_connections);
    }
    if(res) {
       res = conf_write_key_value_bool(emitter, "redis", pool->redis);
    }
    if(res) {
        res = conf_write_key_value_string(emitter, "redis_auth",
                                          &pool->redis_auth);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "redis_db", pool->redis_db);
    }
    if(res) {
       res = conf_write_key_value_bool(emitter, "preconnect",
                                       pool->preconnect);
    }
    if(res) {
       res = conf_write_key_value_bool(emitter, "auto_eject_hosts",
                                       pool->auto_eject_hosts);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "server_connections",
                                       (int)pool->server_connections);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "server_retry_timeout",
                                       (int)pool->server_retry_timeout / 1000);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "server_failure_limit",
                                       (int)pool->server_failure_limit);
    }
    if(res) {
        res = conf_write_key_value_time(emitter, "server_ttl",
                                        pool->server_ttl_ms);
    }
    if(res) {
        res = conf_write_buckets_props(emitter, "buckets",
                                       &pool->backend_opt.bucket_prop);
    }
    if(res) {
        res = conf_write_servers(emitter, "servers", &pool->frontends);
    }
    if(res) {
        res = conf_write_servers(emitter, "backends", &pool->backends);
    }

    if(res) {
        if (pool->backend_opt.type != CONN_UNKNOWN) {
            res = conf_write_key_value_string(emitter, "backend_type",
                                  &connection_strings[pool->backend_opt.type]);
        }
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "backend_max_resend",
                                       pool->backend_opt.max_resend);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "backend_riak_r",
                                       pool->backend_opt.riak_r);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "backend_riak_pr",
                                       pool->backend_opt.riak_pr);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "backend_riak_w",
                                       pool->backend_opt.riak_w);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "backend_riak_pw",
                                       pool->backend_opt.riak_pw);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "backend_riak_n",
                                       pool->backend_opt.riak_n);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "backend_riak_timeout",
                                       pool->backend_opt.riak_timeout);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "backend_riak_basic_quorum",
                                       pool->backend_opt.riak_basic_quorum);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "backend_riak_sloppy_quorum",
                                       pool->backend_opt.riak_sloppy_quorum);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "backend_riak_notfound_ok",
                                       pool->backend_opt.riak_notfound_ok);
    }
    if(res) {
        res = conf_write_key_value_int(emitter, "backend_riak_deletedvclock",
                                       pool->backend_opt.riak_deletedvclock);
    }

    /* close pool record */
    if (!yaml_mapping_end_event_initialize(&event)) {
        log_error("conf: failed to end yaml mapping");
        return false;
    }
    if (!yaml_emitter_emit(emitter, &event)) {
        log_error("conf: failed to write yaml event");
        return false;
    }
    return res;
}

bool
conf_save_to_file(const char *filename, struct array *pools)
{
    FILE *fh;
    uint32_t i;
    yaml_emitter_t emitter;
    yaml_event_t event;
    bool res;

    /* init yaml writter */
    if (!yaml_emitter_initialize(&emitter)) {
        log_error("conf: failed to initialize yaml emitter");
        return false;
    }
    fh = fopen(filename, "w");
    if (fh == NULL) {
        log_error("conf: failed to write configuration");
        return false;
    }
    yaml_emitter_set_output_file(&emitter, fh);
    res = false;
    if (!yaml_stream_start_event_initialize(&event, YAML_UTF8_ENCODING)) {
        log_error("conf: failed to init yaml stream");
    } else if (!yaml_emitter_emit(&emitter, &event)) {
        log_error("conf: failed to write yaml event");
    } else if (!yaml_document_start_event_initialize(&event, NULL, NULL,
                                                     NULL, 1)) {
        log_error("conf: failed to init yaml document");
    } else if (!yaml_emitter_emit(&emitter, &event)) {
        log_error("conf: failed to write yaml event");
    } else if (!yaml_mapping_start_event_initialize(&event, NULL, NULL, 1,
                                              YAML_BLOCK_MAPPING_STYLE)) {
       log_error("conf: failed to init yaml mapping");
    } else if (!yaml_emitter_emit(&emitter, &event)) {
       log_error("conf: failed to write yaml event");
    } else {
        res = true;
    }
    if (!res) {
        yaml_emitter_delete(&emitter);
        fclose(fh);
        return false;
    }

    /* store each pool */
    for (i = 0; i < array_n(pools); i++) {
        struct server_pool *pool = array_get(pools, i);
        if (!conf_write_pool(&emitter, pool)) {
            yaml_emitter_flush(&emitter);
            yaml_emitter_delete(&emitter);
            fclose(fh);
            return false;
        }
    }

    /* deinit yaml writter */
    res = false;
    if (!yaml_mapping_end_event_initialize(&event)) {
        log_error("conf: failed to end yaml mapping");
    } else if (!yaml_emitter_emit(&emitter, &event)) {
        log_error("conf: failed to write yaml event");
    } else if (!yaml_document_end_event_initialize(&event, 1)) {
        log_error("conf: failed to end yaml document");
    } else if (!yaml_emitter_emit(&emitter, &event)) {
        log_error("conf: failed to write yaml event");
    } else if (!yaml_stream_end_event_initialize(&event)) {
        log_error("conf: failed to end yaml stream");
    } else if (!yaml_emitter_emit(&emitter, &event)) {
        log_error("conf: failed to write yaml event");
    } else if (!yaml_emitter_flush(&emitter)) {
        log_error("conf: failed to flush yaml");
    } else {
        res = true;
    }
    yaml_emitter_delete(&emitter);
    fclose(fh);
    return res;
}
