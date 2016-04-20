#ifndef _NC_ADMIN_H_
#define _NC_ADMIN_H_

#define NC_ADMIN_OK 0
#define NC_ADMIN_ERROR 1

#include <stddef.h>
#include <stdint.h>

extern const uint8_t RRA_DATATYPE[];
extern const size_t RRA_DATATYPE_LEN;
extern const uint8_t RRA_SET_DATATYPE[];
extern const size_t RRA_SET_DATATYPE_LEN;
extern const uint8_t RRA_COUNTER_DATATYPE[];
extern const size_t RRA_COUNTER_DATATYPE_LEN;
extern const uint8_t RRA_SERVICE_BUCKET[];
extern const size_t RRA_SERVICE_BUCKET_LEN;
extern const uint8_t RRA_SERVICE_KEY[];
extern const size_t RRA_SERVICE_KEY_LEN;
extern const char *ALLOWED_PROPERTIES[];

int nc_admin_command(const char *host, const char *command,
                     const char *arg1, const char *arg2,
                     const char *arg3);

#endif /* _NC_ADMIN_H_ */
