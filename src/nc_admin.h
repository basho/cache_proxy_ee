#ifndef _NC_ADMIN_H_
#define _NC_ADMIN_H_

#define NC_ADMIN_OK 0
#define NC_ADMIN_ERROR 1

int nc_admin_command(const char *host, const char *command,
                     const char *arg1, const char *arg2,
                     const char *arg3);

#endif /* _NC_ADMIN_H_ */
