#ifndef _NC_ADMIN_CONNECTION_H_
#define _NC_ADMIN_CONNECTION_H_

#include <stdbool.h>

#include <proto/riak_kv.pb-c.h>
#include <proto/riak_dt.pb-c.h>

#define INVALID_SOCKET (-1)

int nc_admin_connection_connect(const char *host);
void nc_admin_connection_disconnect(int sock);
bool nc_admin_connection_set_bucket_prop(int sock, const char *bucket,
                                        const char *prop, const char *value);
RpbGetResp *nc_admin_connection_get_bucket_prop(int sock, const char *bucket,
                                                const char *prop);
DtFetchResp *nc_admin_connection_list_buckets(int sock);

#endif /* _NC_ADMIN_CONNECTION_H_ */
