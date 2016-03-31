#!/usr/bin/env python
#coding: utf-8

from riak_common import *
import riak
import time
import redis

def test_write_through_create():
    _test_write_through_create()

def test_write_through_create_with_dtype():
    _test_write_through_create(bucket_type = 'strings')

def _test_write_through_create(bucket_type = 'default'):
    (riak_client, riak_bucket, nutcracker, redis) = getconn(bucket_type)
    key = distinct_key()
    value = distinct_key()
    nc_key = nutcracker_key(key, riak_bucket)
    write_func = lambda : nutcracker.set(nc_key, value)
    read_func = lambda : nutcracker.get(nc_key)
    redis_delete_func = lambda : redis.delete(nc_key)
    redis_read_func = lambda : redis.get(nc_key)
    riak_read_func = lambda : riak_bucket.get(key)
    riak_object = retry_read_notfound_ok(riak_read_func)
    riak_delete_func = lambda : riak_object.delete()
    wrote = retry_write(riak_delete_func)
    assert_not_exception(wrote)
    wrote = retry_write(redis_delete_func)
    assert_not_exception(wrote)
    wrote = retry_write(write_func)
    assert_not_exception(wrote)
    # cache was invalidated
    assert_equal(None, retry_read_notfound_ok(redis_read_func))
    # riak was written
    riak_object_readback = retry_read_notfound_ok(riak_read_func)
    assert_equal(value, riak_object_readback.data)
    # and a read-through yields the value
    value_readback = retry_read_notfound_ok(read_func)
    assert_equal(value, value_readback)

def test_write_through_bucketless_create():
    (riak_client, riak_bucket, nutcracker, redis) = getconn()
    key = distinct_key()
    value = distinct_key()
    write_func = lambda : nutcracker.set(key, value)
    read_func = lambda : nutcracker.get(key)
    redis_delete_func = lambda : redis.delete(key)
    redis_read_func = lambda : redis.get(key)
    riak_read_func = lambda : riak_bucket.get(key)
    riak_object = retry_read_notfound_ok(riak_read_func)
    wrote = retry_write(redis_delete_func)
    assert_not_exception(wrote)
    wrote = retry_write(write_func)
    assert_not_exception(wrote)
    # redis as persistent store was written
    assert_equal(value, retry_read_notfound_ok(redis_read_func))
    # riak was NOT written
    riak_object_readback = retry_read_notfound_ok(riak_read_func)
    assert_equal(None, riak_object_readback.data)
    # and a read of redis as persistent store yields the value
    value_readback = retry_read_notfound_ok(read_func)
    assert_equal(value, value_readback)

def test_write_through_update():
    _test_write_through_update()

def test_write_through_update_with_dtype():
    _test_write_through_update(bucket_type = 'strings')

def _test_write_through_update(bucket_type = 'default'):
    (riak_client, riak_bucket, nutcracker, redis) = getconn(bucket_type)
    key = distinct_key()
    nc_key = nutcracker_key(key, riak_bucket)
    value = distinct_value()
    create_siblings(key)
    write_func = lambda : nutcracker.set(nc_key, value)
    read_func = lambda : nutcracker.get(nc_key)
    riak_read_func = lambda : riak_bucket.get(key)
    for _ in range(0, 3):
        wrote = retry_write(write_func)
        assert_not_exception(wrote)
    value_readback = retry_read_notfound_ok(read_func)
    assert_equal(value, value_readback)
    # w/o a sibling resolution strategy, 'Object in conflict' will result if
    # Cache Proxy is causing "sibling explosion"
    riak_object_readback = retry_read_notfound_ok(riak_read_func)
    assert_equal(value, riak_object_readback.data)
    assert_equal(1, len(riak_object_readback.siblings))

def test_delete_single():
    _delete(1)

def test_delete_single_with_dtype():
    _delete(1, bucket_type = 'strings')

def test_delete_multi():
    _delete(riak_multi_n)

def test_delete_many():
    _delete(riak_many_n)

def _delete(key_count, bucket_type = 'default'):
    (riak_client, riak_bucket, nutcracker, redis) = getconn(bucket_type)
    keys = [ distinct_key() for i in range(0, key_count)]
    nc_keys = [ nutcracker_key(key, riak_bucket) for key in keys ]

    for i, key in enumerate(keys):
        riak_read_func = lambda : riak_bucket.get(key)
        riak_object = retry_read_notfound_ok(riak_read_func)
        riak_object.data = distinct_key()
        riak_write_func = lambda : riak_object.store()
        wrote = retry_write(riak_write_func)
        assert_not_exception(wrote)
    delete_func = lambda : nutcracker.delete(*nc_keys)
    del_response = retry_write(delete_func)
    assert_equal(len(keys), del_response)
    failed_to_delete = 0
    for i, key in enumerate(keys):
        read_func = lambda: nutcracker.get(nutcracker_key(key))
        riak_read_func = lambda : riak_bucket.get(key)
        cached_value = retry_read_notfound_ok(read_func)
        if None != cached_value:
            failed_to_delete += 1
            continue
        riak_object = retry_read_notfound_ok(riak_read_func)
        if None != riak_object.data:
            failed_to_delete += 1
            continue
    assert_equal(0, failed_to_delete)

# TODO: add a test for an empty del, a manual test of this hung the client
# connection and had a message stuck in the listen message queue.

def test_delete_bucketless_single():
    _delete_bucketless(1)

def test_delete_bucketless_multi():
    _delete_bucketless(riak_multi_n)

def test_delete_bucketless_many():
    _delete_bucketless(riak_many_n)

def _delete_bucketless(key_count):
    (riak_client, riak_bucket, nutcracker, redis) = getconn()
    keys = [ distinct_key() for i in range(0, key_count)]

    keys_created = 0
    for i, key in enumerate(keys):
        write_func = lambda: nutcracker.set(key, i)
        keys_created += retry_write(write_func)
    assert_equal(len(keys), keys_created)

    delete_func = lambda : nutcracker.delete(*keys)
    del_response = retry_write(delete_func)
    assert_equal(len(keys), del_response)
    failed_to_delete = 0
    for i, key in enumerate(keys):
        read_func = lambda: nutcracker.get(key)
        cached_value = retry_read_notfound_ok(read_func)
        if None != cached_value:
            failed_to_delete += 1
            continue
    assert_equal(0, failed_to_delete)

def test_delete_mixed_bucketless_at_edges_multi():
    _delete_mixed_bucketless_at_edges(riak_multi_n)

def test_delete_mixed_bucketless_at_edges_many():
    _delete_mixed_bucketless_at_edges(riak_many_n)

def _delete_mixed_bucketless_at_edges(key_count):
    bucketless_func = lambda (i): i <= 0 or i >= key_count
    _delete_multi_mixed(key_count, bucketless_func)

def test_delete_mixed_bucketless_striped_multi():
    _delete_mixed_bucketless_striped(riak_multi_n)

def test_delete_mixed_bucketless_striped_many():
    _delete_mixed_bucketless_striped(riak_many_n)

def _delete_mixed_bucketless_striped(key_count):
    bucketless_func = lambda (i): i % 2 == 0
    _delete_multi_mixed(key_count, bucketless_func)

def test_delete_mixed_bucketed_at_edges_multi():
    _delete_mixed_bucketed_at_edges(riak_multi_n)

def test_delete_mixed_bucketed_at_edges_many():
    _delete_mixed_bucketed_at_edges(riak_many_n)

def _delete_mixed_bucketed_at_edges(key_count):
    bucketless_func = lambda (i): i > 0 or i < key_count
    _delete_multi_mixed(key_count, bucketless_func)

def _delete_multi_mixed(key_count, bucketless_func):
    (riak_client, riak_bucket, nutcracker, redis) = getconn()
    keys = [ distinct_key() for i in range(0, key_count)]
    nc_keys = []
    for i, key in enumerate(keys):
        if bucketless_func(i):
            nc_keys += [ key ]
        else:
            nc_keys += [ nutcracker_key(key) ]

    for i, key in enumerate(keys):
        riak_read_func = lambda : riak_bucket.get(key)
        riak_object = retry_read_notfound_ok(riak_read_func)
        riak_object.data = distinct_key()
        if bucketless_func(i):
            riak_write_func = lambda : riak_object.delete()
        else:
            riak_write_func = lambda : riak_object.store()
        wrote = retry_write(riak_write_func)
        assert_not_exception(wrote)
    delete_func = lambda : nutcracker.delete(*nc_keys)
    del_response = retry_write(delete_func)
    assert_equal(len(keys), del_response)
    for i, key in enumerate(keys):
        read_func = lambda: nutcracker.get(nutcracker_key(key))
        cached_value = retry_read_notfound_ok(read_func)
        assert_equal(None, cached_value)
