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

#include <nc_core.h>
#include <nc_log.h>

#include <nc_admin.h>
#include <nc_admin_poll.h>
#include <nc_admin_connection.h>

static pthread_mutex_t poll_mutex = PTHREAD_MUTEX_INITIALIZER;

static void *
nc_admin_poll_loop(void *arg)
{
    int64_t current_revision = 0;
    int64_t revision;
    struct timespec abs_time;
    struct server *server;
    struct server_pool *pool = (struct server_pool *)arg;
    int sock = INVALID_SOCKET;
    uint8_t key[RRA_COUNTER_DATATYPE_LEN + RRA_SERVICE_BUCKET_LEN
                + RRA_SERVICE_KEY_LEN + 3];
    uint32_t keylen = sprintf((char *)key, "%s:%s:%s", RRA_COUNTER_DATATYPE,
                              RRA_SERVICE_BUCKET, RRA_SERVICE_KEY);

    /* usleep can be interrupted with any signal to thread */
    for (;;) {
        clock_gettime(CLOCK_REALTIME , &abs_time);
        abs_time.tv_sec += POLL_TIMEOUT_SEC;
        // sleep for POLL_TIMEOUT_SEC or exit when mutex is unlocked
        if (pthread_mutex_timedlock(&poll_mutex, &abs_time) != ETIMEDOUT) {
            pthread_mutex_unlock(&poll_mutex);
            break;
        }

        if (sock == INVALID_SOCKET) {
            server = servers_server(&pool->backends, key, keylen);
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
            current_revision = revision;
            // TODO do actual config update
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
}
