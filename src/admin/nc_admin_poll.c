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
#include <pthread.h>
#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

#include <nc_core.h>
#include <nc_log.h>
#include <nc_util.h>
#include <nc_string.h>
#include <nc_server.h>

#include <nc_admin.h>
#include <nc_admin_poll.h>
#include <nc_admin_connection.h>

static pthread_mutex_t poll_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t array_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint8_t *service_key;
static uint8_t service_key_len;

struct update_item {
    struct server_pool *pool;
    struct array bucket_props;
};

struct array update_items;

static void
get_abs_time(struct timespec *abs_time)
{
#ifdef __MACH__ // OS X doesn't implement clock_gettime,
  clock_serv_t clock_srv;
  mach_timespec_t m_abs_time;
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &clock_srv);
  clock_get_time(clock_srv, &m_abs_time);
  mach_port_deallocate(mach_task_self(), clock_srv);
  abs_time->tv_sec = m_abs_time.tv_sec;
  abs_time->tv_nsec = m_abs_time.tv_nsec;
#else
  clock_gettime(CLOCK_REALTIME, abs_time);
#endif
}

static void
nc_admin_poll_default_bp(struct bucket_prop *bp, struct server_pool *pool)
{
    ASSERT(bp);
    ASSERT(pool);
    bp->bucket.len = 0;
    bp->bucket.data = NULL;
    bp->datatype.len = 0;
    bp->datatype.data = NULL;
    bp->ttl_ms = pool->server_ttl_ms;
}

static bool
nc_admin_poll_bucket_props(int sock, uint8_t *bucket, struct bucket_prop *bp)
{
    uint32_t i = 0;
    while (ALLOWED_PROPERTIES[i][0]) {
        RpbGetResp *prop = nc_admin_connection_get_bucket_prop(sock, (char *)bucket,
                                                               ALLOWED_PROPERTIES[i]);
        if (prop == NULL) {
            return false;
        }
        if (prop->n_content > 0) {
            if (prop->content[0]->value.len > 0) {
                if (nc_c_strequ(ALLOWED_PROPERTIES[i], "ttl")) {
                    struct string value;
                    value.data = prop->content[0]->value.data;
                    value.len = prop->content[0]->value.len;
                    if (!nc_read_ttl_value(&value, &bp->ttl_ms)) {
                        nc_free(prop);
                        return false;
                    }
                }
            }
        }
        nc_free(prop);
        i++;
    }
    uint32_t bucketlen = nc_strlen(bucket);
    if (!nc_parse_datatype_bucket(bucket, bucketlen, &bp->datatype,
                                  &bp->bucket)) {
        return false;
    }
    return true;
}

static bool
nc_admin_poll_buckets(int sock, struct array *bucket_props,
                      struct server_pool *pool)
{
    uint32_t i;
    DtFetchResp *bl = nc_admin_connection_list_buckets(sock);
    if (bl == NULL) {
        return false;
    }
    if (bl->value->n_set_value == 0) {
        array_null(bucket_props);
        nc_free(bl);
        return true;
    }
    array_init(bucket_props, bl->value->n_set_value, sizeof(struct bucket_prop));
    for (i = 0; i < bl->value->n_set_value; i++) {
        uint8_t bucket[bl->value->set_value[i].len + 1];
        sprintf((char *)bucket, "%.*s", (int)bl->value->set_value[i].len,
                bl->value->set_value[i].data);
        struct bucket_prop bp;
        nc_admin_poll_default_bp(&bp, pool);
        if (nc_admin_poll_bucket_props(sock, bucket, &bp)) {
            struct bucket_prop *n = array_push(bucket_props);
            *n = bp;
        }
    }
    nc_free(bl);
    if (array_n(bucket_props) == 0) {
        array_deinit(bucket_props);
        return false;
    }

    return true;
}

static void *
nc_admin_poll_loop(void *arg)
{
    int64_t current_revision = 0;
    int64_t revision;
    struct timespec abs_time;
    struct server *server;
    struct server_pool *pool = (struct server_pool *)arg;
    int sock = INVALID_SOCKET;

    for (;;) {
        /* sleep for POLL_TIMEOUT_SEC or exit when mutex is unlocked */
        get_abs_time(&abs_time);
        abs_time.tv_sec += POLL_TIMEOUT_SEC;
        if (pthread_mutex_timedlock(&poll_mutex, &abs_time) == 0) {
            pthread_mutex_unlock(&poll_mutex);
            break;
        }

        if (sock == INVALID_SOCKET) {
            server = servers_server(&pool->backends, service_key,
                                    service_key_len);
            sock = nc_admin_connection_connect(server->addr, server->addrlen);
            if (sock == INVALID_SOCKET) {
                log_debug(LOG_DEBUG, "Failed to connect to '%.*s'",
                          server->name.len, server->name.data);
                continue;
            }
        }

        if (!nc_admin_connection_get_counter(sock, &revision)) {
            nc_admin_connection_disconnect(sock);
            sock = INVALID_SOCKET;
            continue;
        }

        if (revision != current_revision) {
            log_debug(LOG_DEBUG, "Update config from '%.*s'",
                      server->name.len, server->name.data);
            struct array bucket_props;
            if (nc_admin_poll_buckets(sock, &bucket_props, pool)) {
                current_revision = revision;
                pthread_mutex_lock(&array_mutex);
                struct update_item *item = array_push(&update_items);
                if (item == NULL) {
                    pthread_mutex_unlock(&array_mutex);
                    continue;
                }
                item->bucket_props = bucket_props;
                item->pool = pool;
                pthread_mutex_unlock(&array_mutex);
            } else {
                nc_admin_connection_disconnect(sock);
                sock = INVALID_SOCKET;
            }
        }
    }

    if (sock != INVALID_SOCKET) {
        nc_admin_connection_disconnect(sock);
    }
    log_debug(LOG_DEBUG, "Poll stopped for pool '%.*s'",
                          pool->name.len, pool->name.data);
    return NULL;
}

void
nc_admin_poll_start(struct context *ctx)
{
    struct server_pool *pool;
    uint32_t i;

    service_key = nc_alloc(RRA_COUNTER_DATATYPE_LEN + RRA_SERVICE_BUCKET_LEN
                           + RRA_SERVICE_KEY_LEN + 3);
    service_key_len = sprintf((char *)service_key, "%s:%s:%s",
                              RRA_COUNTER_DATATYPE, RRA_SERVICE_BUCKET,
                              RRA_SERVICE_KEY);
    array_init(&update_items, array_n(&ctx->pool), sizeof(struct update_item));

    pthread_mutex_trylock(&poll_mutex);
    for (i = 0; i < array_n(&ctx->pool); i++) {
        pool = array_get(&ctx->pool, i);
        pthread_t tid;
        if (pthread_create(&tid, NULL, nc_admin_poll_loop, pool)) {
            log_debug(LOG_ERR, "Failed to create poll thread for pool '%.*s'",
                      pool->name.len, pool->name.data);
        } else {
            log_debug(LOG_DEBUG, "Poll started for pool '%.*s'",
                                  pool->name.len, pool->name.data);
        }
    }
}

void
nc_admin_poll_stop(void)
{
    pthread_mutex_unlock(&poll_mutex);
    nc_free(service_key);
    while (array_n(&update_items) != 0) {
        struct update_item *item = array_pop(&update_items);
        server_pool_bp_deinit(&item->bucket_props);
    }
    array_deinit(&update_items);
}

bool
nc_admin_poll_sync(void)
{
    bool found = false;
    while (array_n(&update_items) != 0) {
        pthread_mutex_lock(&array_mutex);
        struct update_item *item = array_pop(&update_items);
        server_pool_bp_deinit(&item->pool->backend_opt.bucket_prop);
        item->pool->backend_opt.bucket_prop = item->bucket_props;
        pthread_mutex_unlock(&array_mutex);
        log_debug(LOG_DEBUG, "Update %d buckets props in pool '%.*s'",
                  array_n(&item->bucket_props), item->pool->name.len,
                  item->pool->name.data);
        found = true;
    }
    return found;
}
