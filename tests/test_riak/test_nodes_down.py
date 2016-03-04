#!/usr/bin/env python
#coding: utf-8

from riak_common import *
import riak
import redis

def _create_delete(key_count, redis_to_shutdown, riak_to_shutdown):
    (riak_client, riak_bucket, nutcracker, redis) = getconn()
    shutdown_redis_nodes(redis_to_shutdown)
    shutdown_riak_nodes(riak_to_shutdown)

    keys = [ distinct_key() for i in range(0, key_count)]
    nc_keys = []

    for i, key in enumerate(keys):
        if i % 2 == 0:
            nc_keys += [ key ]
        else:
            nc_keys += [ nutcracker_key(key) ]

    cleanup_func = lambda : nutcracker.delete(*nc_keys)
    retry_write(cleanup_func)

    keys_created = 0
    for i, key in enumerate(nc_keys):
        write_func = lambda: nutcracker.set(key, key)
        keys_created += retry_write(write_func)
    assert_equal(len(nc_keys), keys_created)

    for i, key in enumerate(nc_keys):
        read_func = lambda: nutcracker.get(key)
        read_value = retry_read(read_func)
        assert_equal(key, read_value)

    delete_func = lambda : nutcracker.delete(*nc_keys)
    del_response = retry_write(delete_func)
    assert_equal(len(nc_keys), del_response)
# TODO
#    failed_to_delete = 0
#    for i, key in enumerate(nc_keys):
#        read_func = lambda: nutcracker.get(nutcracker_key(key))
#        riak_read_func = lambda : riak_bucket.get(key)
#        cached_value = retry_read_notfound_ok(read_func)
#        if None != cached_value:
#            failed_to_delete += 1
#            continue
#        riak_object = retry_read_notfound_ok(riak_read_func)
#        if None != riak_object.data:
#            failed_to_delete += 1
#            continue
#    assert_equal(0, failed_to_delete)
    restore_redis_nodes()
    restore_riak_nodes()

def test_happy_path():
    _create_delete(riak_many_n, 0, 0)

def test_one_redis_node_down():
    _create_delete(riak_many_n, 1, 0)

def test_two_redis_nodes_down():
    _create_delete(riak_many_n, 2, 0)

#TODO, shutdown_riak_nodes throw an error
def _test_one_riak_node_down():
    _create_delete(riak_many_n, 0, 1)

def _test_two_riak_nodes_down():
    _create_delete(riak_many_n, 0, 2)
