#!/usr/bin/env python
#coding: utf-8

from riak_common import *
import riak
import redis
import time

def assert_equal_approximately(v1, v2):
    # 10 percent assertion
    d = abs(v1 - v2) / 0.1
    assert d <= abs(v1) and d <= abs(v2)

def assert_stat(redis_commands, riak_commands):
    time.sleep(1) # wait until statistics ready
    stat = nc._info_dict()
    max_connections = 2 + len(riak_cluster.node_names()) + len(all_redis)
    assert stat['curr_connections'] <= max_connections
    assert_equal(stat['service'], 'nutcracker');
    assert_equal_approximately(stat['timestamp'], time.time())
    assert stat['total_connections'] <= max_connections
    assert stat['uptime'] < 60
    if redis_commands > 0 or riak_commands > 0:
        assert_equal(stat[CLUSTER_NAME]['client_connections'], 1)
    else:
        assert_equal(stat[CLUSTER_NAME]['client_connections'], 0)
    assert_equal(stat[CLUSTER_NAME]['client_eof'], 0)
    assert_equal(stat[CLUSTER_NAME]['client_err'], 0)
    assert_equal(stat[CLUSTER_NAME]['forward_error'], 0)
    assert_equal(stat[CLUSTER_NAME]['fragments'], 0)
    assert_equal(stat[CLUSTER_NAME]['server_ejects'], 0)
    redis_stat = dict()
    riak_stat = dict()
    total_stat = dict()
    for k in ['in_queue', 'in_queue_bytes', 'out_queue', 'out_queue_bytes',
              'request_bytes', 'requests', 'response_bytes', 'responses',
              'server_connections', 'server_ejected_at', 'server_eof', 
              'server_err', 'server_timedout']:
        redis_stat[k] = 0
        riak_stat[k] = 0
        total_stat[k] = 0
    for n, d in stat[CLUSTER_NAME].items():
        if type(d) == dict:
            for k, v in d.items():
                total_stat[k] += v
                if 'redis' in n:
                    redis_stat[k] += v
                else:
                    riak_stat[k] += v

    assert total_stat['server_connections'] <= max_connections
    assert_equal(total_stat['server_ejected_at'], 0)
    assert_equal(total_stat['server_eof'], 0)
    assert_equal(total_stat['server_err'], 0)
    assert_equal(total_stat['server_timedout'], 0)

    assert_equal(redis_stat['in_queue'], 0)
    assert_equal(redis_stat['in_queue_bytes'], 0)
    assert_equal(redis_stat['out_queue'], 0)
    assert_equal(redis_stat['out_queue_bytes'], 0)
    assert_equal_approximately(redis_stat['request_bytes'],
                               59 * redis_commands + 28 * riak_commands)
    assert_equal(redis_stat['requests'], 2 * redis_commands + riak_commands)
    assert_equal_approximately(redis_stat['response_bytes'],
                               16 * redis_commands)
    assert_equal(redis_stat['responses'], 2 * redis_commands)

    assert_equal(riak_stat['in_queue'], 0)
    assert_equal(riak_stat['in_queue_bytes'], 0)
    assert_equal(riak_stat['out_queue'], 0)
    assert_equal(riak_stat['out_queue_bytes'], 0)
    assert_equal_approximately(riak_stat['request_bytes'], 38 * riak_commands)
    assert_equal(riak_stat['requests'], 2 * riak_commands)
    assert_equal_approximately(riak_stat['response_bytes'], 16 * riak_commands)
    assert_equal(riak_stat['responses'], 2 * riak_commands)

def test_nc_stat():
    tets_len = 20
    (_, _, nutcracker, _) = getconn()
    nc.stop() #reset counters
    nc.start()
    assert_stat(0, 0)

    kv = {'kkk-%s' % i :'vvv-%s' % i for i in range(tets_len)}
    for k, v in kv.items():
        nutcracker.set(k, v)
        nutcracker.get(k)
    assert_stat(tets_len, 0)

    bkv = {'bbb:kkk-%s' % i :'vvv-%s' % i for i in range(tets_len)}
    for k, v in bkv.items():
        nutcracker.set(k, v)
    time.sleep(3) # wait until data expire in frontend
    for k, v in bkv.items():
        nutcracker.get(k)
    assert_stat(tets_len, tets_len)

def test_bucket_prop_ttl():
    (_, _, nutcracker, redis) = getconn()
    # TODO: strangely localhost did not work on OSX, investigate why the
    # `nutcracker admin` fails to resolve localhost correctly.
    riak_node = '127.0.0.1:%d' % riak_cluster.port()

    buckets_ttl = [['bucket1', 2], ['bucket2', 4],
                   ['bucket3', 8], ['bucket4', 10]]
    value = 'data'
    max_ttl = 0
    for bt in buckets_ttl:
        assert_equal(nc.admin(riak_node, 'set-bucket-prop default:%s ttl %ds' % (bt[0], bt[1])), 0)
        if bt[1] > max_ttl:
            max_ttl = bt[1]

    # TODO: should read back value using nc.admin get-bucket-prop to test the
    # basic CRUD of the `nutcracker admin` set of commands
    time.sleep(20) # sleep until centralized config is synced

    # NOTE: the following actions+assertions test whether bucket-specific ttl
    # is in effect
    for bt in buckets_ttl:
        key = '%s:key' % bt[0]
        nutcracker.set(key, value)
        nutcracker.get(key)
    start_time = time.time()

    while (time.time() - start_time) <  (max_ttl + 2):
        for bt in buckets_ttl:
            key = '%s:key' % bt[0]
            if bt[1] < (time.time() - start_time):
                assert_equal(redis.get(key), None)
            else:
                assert_equal(redis.get(key), value)
        time.sleep(3)
