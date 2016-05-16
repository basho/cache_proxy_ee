#!/usr/bin/env python
#coding: utf-8

import os
import sys
import redis
import riak

PWD = os.path.dirname(os.path.realpath(__file__))
WORKDIR = os.path.join(PWD,'../')
sys.path.append(os.path.join(WORKDIR,'lib/'))
sys.path.append(os.path.join(WORKDIR,'conf/'))

import conf

from server_modules import *
from server_modules_redis import *
from server_modules_riak import *
from server_modules_nutcracker import *
from utils import *
from all_redis import *

# NOTE: such naive thread-per-request implementation as opposed to thread pool
# doesn't scale far enough for load testing, but here we are testing concurrent
# requests intentionally, to help establish max concurrent supported.
riak_multi_n = getenv('T_RIAK_MULTI', 2, int)
riak_many_n = getenv('T_RIAK_MANY', 50, int)

CLUSTER_NAME = 'ntest2'
nc_verbose = getenv('T_VERBOSE', 5, int)
mbuf = getenv('T_MBUF', 512, int)
large = getenv('T_LARGE', 1000, int)

all_redis_for_feature_testing = [
        RedisServer('127.0.0.1', 2210, '/tmp/r/redis-2210/', CLUSTER_NAME, 'redis-2210')
        ]
all_redis = all_redis_for_feature_testing #<< alias used w/i feature testing

all_redis_for_partition_testing = [
        RedisServer('127.0.0.1', 2410, '/tmp/r/redis-2410/', CLUSTER_NAME, 'redis-2410'),
        RedisServer('127.0.0.1', 2411, '/tmp/r/redis-2411/', CLUSTER_NAME, 'redis-2411'),
        RedisServer('127.0.0.1', 2412, '/tmp/r/redis-2412/', CLUSTER_NAME, 'redis-2412'),
        ]

riak_cluster_for_feature_testing = RiakCluster([
                            ('devA', 5200)
                            ])
riak_cluster = riak_cluster_for_feature_testing #<< alias used w/i feature testing

# NOTE: intentionally not overlapping node name prefix w/ feature testing nodes
riak_cluster_for_partition_testing = RiakCluster([
                            ('devPA', 5400),
                            ('devPB', 5401),
                            ('devPC', 5402)
                            ])

nc_for_feature_testing = NutCracker('127.0.0.1', 4210, '/tmp/r/nutcracker-4210',
        CLUSTER_NAME, all_redis_for_feature_testing, mbuf=mbuf,
        verbose=nc_verbose, riak_cluster=riak_cluster_for_feature_testing,
        auto_eject=True)
nc = nc_for_feature_testing

nc_for_partition_testing = NutCracker('127.0.0.1', 4210, '/tmp/r/nutcracker-4410',
        CLUSTER_NAME, all_redis_for_partition_testing, mbuf=mbuf,
        verbose=nc_verbose, riak_cluster=riak_cluster_for_partition_testing,
        auto_eject=True)

def cluster_setup():
    print 'setup(mbuf=%s, verbose=%s)' %(mbuf, nc_verbose)
    _cluster_setup(nc_for_feature_testing,
            riak_cluster_for_feature_testing,
            all_redis_for_feature_testing)

def cluster_setup_for_partition():
    # NOTE: function name must not contain test or it will be discovered as a test
    print 'setup(mbuf=%s, verbose=%s)' %(mbuf, nc_verbose)
    _cluster_setup(nc_for_partition_testing,
            riak_cluster_for_partition_testing,
            all_redis_for_partition_testing)

def _cluster_setup(lnc, lriak, lredis):
    lriak.deploy()
    lriak.start()
    for r in lredis + [lriak, lnc]:
        r.clean()
        r.deploy()
        r.stop()
        r.start()
    lriak.ensure_string_dt()
    lriak.ensure_set_dt()

def cluster_teardown():
    _cluster_teardown(nc_for_feature_testing,
            riak_cluster_for_feature_testing,
            all_redis_for_feature_testing)

def cluster_teardown_for_partition():
    # NOTE: function name must not contain test or it will be discovered as a test
    _cluster_teardown(nc_for_partition_testing,
            riak_cluster_for_partition_testing,
            all_redis_for_partition_testing)

def _cluster_teardown(lnc, lriak, lredis):
    for r in [lnc, lriak] + lredis:
        if not r._alive():
            print('%s was not alive at teardown' % r)
        r.stop()

def getconn(bucket_type = 'default', testing_type = 'feature'):
    if testing_type == 'partition':
        lredis = all_redis_for_partition_testing
        lriak = riak_cluster_for_partition_testing
        lnc = nc_for_partition_testing
    else:
        lredis = all_redis_for_feature_testing
        lriak = riak_cluster_for_feature_testing
        lnc = nc_for_feature_testing

    for r in lredis:
        c = redis.Redis(r.host(), r.port())
        c.flushdb()

    riak_client = riak.RiakClient(pb_port = lriak.port(), protocol = 'pbc')
    riak_bucket = riak_client.bucket_type(bucket_type).bucket('test')

    nutcracker = redis.Redis(lnc.host(), lnc.port())
    r = AllRedis(lredis)

    return (riak_client, riak_bucket, nutcracker, r)

def ensure_siblings_bucket_properties(testing_type = 'feature'):
    (riak_client, riak_bucket, nutcracker, redis) = getconn(testing_type = testing_type)
    bucket_properties = riak_bucket.get_properties()
    if not bucket_properties['allow_mult'] or bucket_properties['last_write_wins']:
        riak_bucket.set_property('allow_mult', True)
        riak_bucket.set_property('last_write_wins', False)
        bucket_properties_readback = riak_bucket.get_properties()
        assert_equal(True, bucket_properties_readback['allow_mult'])
        assert_equal(False, bucket_properties_readback['last_write_wins'])

def create_siblings(key, testing_type = 'feature'):
    ensure_siblings_bucket_properties()
    value = distinct_value()
    value2 = distinct_value()
    assert_not_equal(value, value2)
    (_riak_client, riak_bucket, _nutcracker, _redis) = getconn(testing_type = testing_type)
    riak_object = riak_bucket.get(key)
    riak_object2 = riak_bucket.get(key)
    riak_object.data = value
    riak_object.store()
    riak_object2.data = value2
    riak_object2.store()
    riak_object_readback = riak_bucket.get(key)
    assert_not_equal('[]', siblings_str(riak_object_readback))

def siblings_str(riak_object):
    return str([ str(content.data) for content in riak_object.siblings ])

def shutdown_redis_nodes(num):
    lredis = all_redis_for_partition_testing
    assert(num <= len(lredis))
    for i in range(0, num):
        lredis[i].stop()

def restore_redis_nodes():
    for r in all_redis_for_partition_testing:
        r.start()

def shutdown_riak_nodes(num):
    lriak = riak_cluster_for_partition_testing
    node_names = lriak.node_names()
    assert(num <= len(node_names))
    if num > 0:
        lriak.shutdown(node_names[-num:])

def restore_riak_nodes():
    riak_cluster_for_partition_testing.restore()
