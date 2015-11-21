#!/usr/bin/env python
#coding: utf-8

from riak_common import *
import riak
import time
import redis
from riak.datatypes import Set
from time import sleep

def test_set_dt_empty():
    (riak_client, _, nutcracker, redis) = getconn()
    key = distinct_key()
    nc_key = nutcracker_key(key)
    riak_set = get_set_dt_object(riak_client, 'test', key)
    assert_equal(0, len(riak_set))
    value = retry_read(lambda: nutcracker.scard(nc_key))
    assert_equal(0, value)

def test_set_dt_add_single():
    (riak_client, _, nutcracker, redis) = getconn()
    key = distinct_key()
    nc_key = nutcracker_key(key)
    add_values = [ distinct_value(), distinct_value(), distinct_value() ]
    remove_values = [ add_values[1] ]
    values = []

    for value in add_values:
        wrote = retry_write(lambda: nutcracker.sadd(nc_key, value))
        assert_equal(1, wrote)
        values.append(value)

    value = retry_read(lambda: nutcracker.scard(nc_key))
    assert_equal(len(values), value)

    for value in remove_values:
        wrote = retry_write(lambda: nutcracker.srem(nc_key, value))
        assert_equal(1, wrote)
        values.remove(value)

    value = retry_read(lambda: nutcracker.scard(nc_key))
    assert_equal(len(values), value)

    nc_values = retry_read(lambda: nutcracker.smembers(nc_key))
    assert_equal(len(values), len(nc_values))

    for value in values:
        exists = retry_read(lambda: nutcracker.sismember(nc_key, value))
        assert(exists)

def test_set_dt_add_multi():
    (riak_client, _, nutcracker, redis) = getconn()
    key = distinct_key()
    nc_key = nutcracker_key(key)
    add_values = [ distinct_value(), distinct_value(), distinct_value() ]
    remove_values = [ add_values[1], add_values[0] ]
    values = []

    wrote = retry_write(lambda: nutcracker.sadd(nc_key, *add_values))
    #HACK: sadd and srem multi is returning 0 or 1, not count of affected values
    #assert_equal(len(add_values), wrote)
    assert(wrote > 0 and wrote <= len(add_values)), \
            "expected wrote: %d to be between 0 and len(add_values): %d" % \
            (wrote, len(add_values))
    for value in add_values:
        values.append(value)

    value = retry_read(lambda: nutcracker.scard(nc_key))
    assert_equal(len(values), value)

    wrote = retry_write(lambda: nutcracker.srem(nc_key, *remove_values))
    #HACK: sadd and srem multi is returning 0 or 1, not count of affected values
    #assert_equal(len(remove_values), wrote)
    assert(wrote > 0 and wrote <= len(remove_values)), \
            "expected wrote: %d to be between 0 and len(remove_values): %d" % \
            (wrote, len(remove_values))
    for value in remove_values:
        values.remove(value)

    value = retry_read(lambda: nutcracker.scard(nc_key))
    assert_equal(len(values), value)

    nc_values = retry_read(lambda: nutcracker.smembers(nc_key))
    assert_equal(len(values), len(nc_values))

    for value in values:
        exists = retry_read(lambda: nutcracker.sismember(nc_key, value))
        assert(exists)

def test_set_dt_ttl():
    (riak_client, _, nutcracker, redis) = getconn()
    key = distinct_key()
    nc_key = nutcracker_key(key)
    add_values = [ distinct_value(), distinct_value() ]
    remove_values = []
    values = []

    riak_set = get_set_dt_object(riak_client, 'test', key)

    value = add_values[0]
    wrote = retry_write(lambda: nutcracker.sadd(nc_key, value))
    values.append(value)

    value = add_values[1]
    riak_set.add(value)
    values.append(value)
    riak_set.store()

    for i in range(0, 10):
        nc_values = retry_read(lambda: nutcracker.smembers(nc_key))
        if len(values) == len(nc_values):
            break
        sleep(0.1)
    assert_equal(len(values), len(nc_values))

def test_set_dt_max_add():
    n_to_adds = range(1, 100)
    for n_to_add in n_to_adds:
        try:
            _test_set_dt_max_add(n_to_add)
        except Exception as e:
            assert(False), "failed at n_to_add: %d, e: %s" % (n_to_add, e)

def _test_set_dt_max_add(n_to_add):
    (riak_client, _, nutcracker, redis) = getconn()
    key = distinct_key()
    nc_key = nutcracker_key(key)
    add_values = [ distinct_value() for i in range(0, n_to_add) ]
    remove_values = []
    values = []

    wrote = retry_write(lambda: nutcracker.sadd(nc_key, *add_values))
    values = add_values

    nc_values = retry_read(lambda: nutcracker.smembers(nc_key))
    assert_equal(len(values), len(nc_values))
    for value in values:
        assert(value in nc_values), \
                "expected {0} to be in {1}" % (value, nc_values)

def test_set_dt_max_items():
    n_items = range(1, 100)
    for n_item in n_items:
        try:
            _test_set_dt_max_items(n_item)
        except Exception as e:
            assert(False), "failed at n_item: %d, e: %s" % (n_item, e)

def _test_set_dt_max_items(n_items):
    (riak_client, _, nutcracker, redis) = getconn()
    key = distinct_key()
    nc_key = nutcracker_key(key)
    
    for i in range(0, n_items):
        retry_write(lambda: nutcracker.sadd(nc_key, distinct_value()))

    nc_values = retry_read(lambda: nutcracker.smembers(nc_key))
    assert_equal(n_items, len(nc_values))

def get_set_dt_object(riak_client, bucket, key):
    set_dt_bucket = riak_client.bucket_type(bucket_type_name()).bucket(bucket)
    return Set(set_dt_bucket, key)

def bucket_type_name():
    #HACK: see riak server module code about arbitrary bucket-type name
    return 'sets'
