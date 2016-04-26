#ifndef _NC_ADMIN_POLL_H_
#define _NC_ADMIN_POLL_H_

#define POLL_TIMEOUT_SEC 15

#include <nc_core.h>

void nc_admin_poll_start(struct context *ctx);
void nc_admin_poll_stop(void);
void nc_admin_poll_sync(void);
bool nc_admin_poll_buckets(int sock, struct array *bucket_props);

#endif /* _NC_ADMIN_POLL_H_ */
