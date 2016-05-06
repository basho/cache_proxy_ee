#!/usr/bin/env python
#coding: utf-8

from riak_common import *
import riak
import redis
import numbers

def _set_expecting_numeric_response(nutcracker, key, value):
    response = nutcracker.set(key, value)
    if not isinstance(response, numbers.Number):
        raise Exception('Expected nutcracker.set to return a number, but got: {0}'.format(response))
    return response

def _create_delete_run(key_count, riak_client, riak_bucket, nutcracker, redis):
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
        write_func = lambda: _set_expecting_numeric_response(nutcracker, key, key)
        keys_created += retry_write(write_func)

    assert_equal(len(nc_keys), keys_created)

    for i, key in enumerate(nc_keys):
        read_func = lambda: nutcracker.get(key)
        read_value = retry_read(read_func)
        assert_equal(key, read_value)

    delete_func = lambda : nutcracker.delete(*nc_keys)
    del_response = retry_write(delete_func)
    assert_equal(len(nc_keys), del_response)

    failed_to_delete = 0
    for i, key in enumerate(nc_keys):
        riak_read_func = lambda : riak_bucket.get(key)
        riak_object = retry_read_notfound_ok(riak_read_func)
        if None != riak_object.data:
            failed_to_delete += 1
            continue
    assert_equal(0, failed_to_delete)

def _create_delete(key_count, redis_to_shutdown, riak_to_shutdown):
    (riak_client, riak_bucket, nutcracker, redis) = getconn(testing_type='partition')
    try:
        shutdown_redis_nodes(redis_to_shutdown)
        shutdown_riak_nodes(riak_to_shutdown)
        _create_delete_run(key_count, riak_client, riak_bucket, nutcracker, redis)
    except:
        raise
    finally:
        restore_redis_nodes()
        restore_riak_nodes()

def test_happy_path():
    _create_delete(riak_many_n, 0, 0)

def test_one_redis_node_down():
    _create_delete(riak_many_n, 1, 0)

def test_two_redis_nodes_down():
    _create_delete(riak_many_n, 2, 0)

def test_one_riak_node_down():
    _create_delete(riak_many_n, 0, 1)

def test_two_riak_nodes_down():
    _create_delete(riak_many_n, 0, 2)
