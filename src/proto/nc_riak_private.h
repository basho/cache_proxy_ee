#ifndef _NC_RIAK_H_
#define _NC_RIAK_H_

#include <proto/riak_kv.pb-c.h>
#include <proto/riak_dt.pb-c.h>

#define CONF_UNSET_NUM -1

typedef enum {
    REQ_RIAK_GET = 9,
    REQ_RIAK_PUT = 11,
    REQ_RIAK_DEL = 13,
    REQ_RIAK_LIST_BUCKETS = 15,
    REQ_RIAK_LIST_KEYS = 17,
    REQ_RIAK_COUNTER_UPDATE = 50,
    REQ_RIAK_COUNTER_GET = 52,
    REQ_RIAK_DT_FETCH = 80,
    REQ_RIAK_DT_UPDATE = 82
} riak_req_t;

typedef enum {
    RSP_RIAK_UNKNOWN = 0x0,
    RSP_RIAK_GET = 10,
    RSP_RIAK_PUT = 12,
    RSP_RIAK_DEL = 14,
    RSP_RIAK_LIST_BUCKETS = 16,
    RSP_RIAK_LIST_KEYS = 18,
    RSP_RIAK_COUNTER_UPDATE = 51,
    RSP_RIAK_COUNTER_GET = 53,
    RSP_RIAK_DT_FETCH = 81,
    RSP_RIAK_DT_UPDATE = 83
} riak_rsp_t;

typedef size_t (*pb_pack_func)(const void *message, uint8_t *out);
typedef void* (*unpack_func)(ProtobufCAllocator *allocator, size_t len, const uint8_t *data);

void parse_pb_get_req(struct msg *r, uint32_t* len, uint8_t* msgid, RpbGetReq** req);
void parse_pb_put_req(struct msg *r, uint32_t* len, uint8_t* msgid, RpbPutReq** req);

bool get_pb_msglen(struct msg* r, uint32_t* len, uint8_t* msgid);
bool get_pb_mbuflen(struct mbuf* mbuf, uint32_t* len, uint8_t* msgid);

rstatus_t encode_pb_get_req(struct msg* r, struct conn* s_conn, msg_type_t type);
rstatus_t _encode_pb_get_req(struct msg* r, struct conn* s_conn, msg_type_t type,
                   unsigned read_before_write);
rstatus_t encode_pb_put_req(struct msg* r, struct conn* s_conn, msg_type_t type);
rstatus_t encode_pb_del_req(struct msg* r, struct conn* s_conn, msg_type_t type);
rstatus_t encode_pb_sadd_req(struct msg* r, struct conn* s_conn, msg_type_t type);
rstatus_t encode_pb_srem_req(struct msg* r, struct conn* s_conn, msg_type_t type);
rstatus_t encode_pb_smembers_req(struct msg* r, struct conn* s_conn, msg_type_t type);
rstatus_t encode_pb_sismember_req(struct msg* r, struct conn* s_conn, msg_type_t type);
rstatus_t encode_pb_scard_req(struct msg* r, struct conn* s_conn, msg_type_t type);

RpbGetResp* extract_get_rsp(struct msg* r, uint32_t len, uint8_t* msgid);
RpbPutResp* extract_put_rsp(struct msg* r, uint32_t len, uint8_t* msgid);
bool extract_del_rsp(struct msg* r, uint32_t len, uint8_t* msgid);
DtUpdateResp* extract_dt_update_rsp(struct msg* r, uint32_t len, uint8_t* msgid);
DtFetchResp* extract_dt_fetch_rsp(struct msg* r, uint32_t len, uint8_t* msgid);

rstatus_t repack_get_rsp(struct msg* r, RpbGetResp* rpbresp);
rstatus_t repack_dt_update_resp(struct msg* r, DtUpdateResp* dtresp);
rstatus_t repack_dt_fetch_resp(struct msg* r, DtFetchResp* dtresp);


bool extract_rsp(struct msg* r, uint32_t len, uint8_t* msgid, unpack_func func,
            void ** rpbresp);
rstatus_t extract_bucket_key_value(struct msg *r,
                                   ProtobufCBinaryData *type,
                                   ProtobufCBinaryData *bucket,
                                   ProtobufCBinaryData *key,
                                   ProtobufCBinaryData *value,
                                   struct msg_pos *keyname_start_pos,
                                   bool allow_empty_bucket);

rstatus_t pack_message(struct msg *r, msg_type_t type, uint32_t msglen,
                       uint8_t reqid, pb_pack_func func, const void *message,
                       uint32_t bucketlen);

/*
 * Sibling resolution functions
 */

unsigned choose_sibling(RpbGetResp* rpbresp);
unsigned choose_last_modified_sibling(RpbGetResp* rpbresp);
unsigned choose_random_sibling(unsigned nSib);

#endif /* _NC_RIAK_H_ */
