#!/usr/bin/env python
#coding: utf-8

from common import *

def test_mget_mset(kv=default_kv):
    r = getconn()

    def insert_by_pipeline():
        pipe = r.pipeline(transaction=False)
        for k, v in kv.items():
            pipe.set(k, v)
        pipe.execute()

    def insert_by_mset():
        ret = r.mset(**kv)

    #insert_by_mset() #only the mget-imporve branch support this
    try:
        insert_by_mset() #only the mget-imporve branch support this
    except:
        insert_by_pipeline()

    keys = kv.keys()

    #mget to check
    vals = r.mget(keys)
    for i, k in enumerate(keys):
        assert_equal(kv[k], vals[i])

    assert_equal(len(keys), r.delete(*keys) )

    #mget again
    vals = r.mget(keys)

    for i, k in enumerate(keys):
        assert_equal(None, vals[i])

def test_mget_mset_on_key_not_exist(kv=default_kv):
    r = getconn()

    def insert_by_pipeline():
        pipe = r.pipeline(transaction=False)
        for k, v in kv.items():
            pipe.set(k, v)
        pipe.execute()

    def insert_by_mset():
        ret = r.mset(**kv)

    try:
        insert_by_mset() #only the mget-imporve branch support this
    except:
        insert_by_pipeline()

    keys = kv.keys()
    keys2 = ['x-'+k for k in keys]
    keys = keys + keys2
    random.shuffle(keys)

    #mget to check
    vals = r.mget(keys)
    for i, k in enumerate(keys):
        if k in kv:
            assert_equal(kv[k], vals[i])
        else:
            assert_equal(None, vals[i])

    assert_equal(len(kv), r.delete(*keys) )

    #mget again
    vals = r.mget(keys)

    for i, k in enumerate(keys):
        assert_equal(None, vals[i])

def test_mget_mset_large():
    for cnt in range(171, large, 171):
        kv = {'kkk-%s' % i :'vvv-%s' % i for i in range(cnt)}
        test_mget_mset(kv)

def test_mget_special_key(cnt=5):
    #key length = 512-48-1
    kv = {}
    for i in range(cnt):
        k = 'kkk-%s' % i
        k = k + 'x'*(512-48-1-len(k))
        kv[k] = 'vvv'

    test_mget_mset(kv)

def test_mget_special_key_2(cnt=5):
    #key length = 512-48-2
    kv = {}
    for i in range(cnt):
        k = 'kkk-%s' % i
        k = k + 'x'*(512-48-2-len(k))
        kv[k] = 'vvv'*9

    test_mget_mset(kv)

def test_mget_on_backend_down():
    #one backend down

    r = redis.Redis(nc.host(), nc.port())
    assert_equal(None, r.get('key-2'))
    assert_equal(None, r.get('key-1'))

    all_redis[0].stop()

    actual_fails = 0
    for i in range(1, len(all_redis)):
        try:
            assert_fail('Connection refused|reset by peer|Broken pipe', r.mget, 'key-1')
            assert_fail('Connection refused|reset by peer|Broken pipe', r.get, 'key-1')
        except:
            actual_fails += 1
    assert_not_equal(0, actual_fails)

    keys = ['key-1', 'key-2', 'kkk-3']
    actual_fails = 0
    for i in range(1, len(all_redis)):
        try:
            assert_fail('Connection refused|reset by peer|Broken pipe', r.mget, *keys)
        except:
            actual_fails += 1
    assert_not_equal(0, actual_fails)

    #all backend down
    for i in range(1, len(all_redis)):
        all_redis[i].stop()

    r = redis.Redis(nc.host(), nc.port())

    assert_fail('Connection refused|reset by peer|Broken pipe', r.mget, 'key-1')
    assert_fail('Connection refused|reset by peer|Broken pipe', r.mget, 'key-2')

    keys = ['key-1', 'key-2', 'kkk-3']
    assert_fail('Connection refused|reset by peer|Broken pipe', r.mget, *keys)

    for r in all_redis:
        r.start()

def test_mset_on_backend_down():
    all_redis[0].stop()
    r = redis.Redis(nc.host(),nc.port())

    actual_fails = 0
    for i in range(1, len(all_redis)):
        try:
            assert_fail('Connection refused|Broken pipe',r.mset,default_kv)
        except:
            actual_fails += 1
    assert_not_equal(0, actual_fails)

    for i in range(1, len(all_redis)):
        all_redis[i].stop()

    assert_fail('Connection refused|Broken pipe',r.mset,default_kv)

    for r in all_redis:
        r.start()

def test_mget_pipeline():
    r = getconn()

    pipe = r.pipeline(transaction=False)
    for k,v in default_kv.items():
        pipe.set(k,v)
    keys = default_kv.keys()
    pipe.mget(keys)
    kv = {}
    for i in range(large):
        kv['kkk-%s' % i] = os.urandom(100)
    for k,v in kv.items():
        pipe.set(k,v)
    for k in kv.keys():
        pipe.get(k)
    rst = pipe.execute()

    #print rst
    #check the result
    keys = default_kv.keys()

    #mget to check
    vals = r.mget(keys)
    for i, k in enumerate(keys):
        assert_equal(kv[k], vals[i])

    assert_equal(len(keys), r.delete(*keys))

    #mget again
    vals = r.mget(keys)

    for i, k in enumerate(keys):
        assert_equal(None, vals[i])

def test_multi_delete_normal():
    r = getconn()

    kvs = { 'key-%i' % i: 'val-%i' % i for i in range(100) }
    keys = kvs.keys()

    r.mset(kvs)
    vals = r.mget(keys)
    assert_equal(len(kvs), reduce(lambda agg, it: agg + (it != None), vals, 0))

    assert_equal(len(kvs), r.delete(*keys))

    vals = r.mget(keys)
    assert_equal(100, reduce(lambda agg, it: agg + (it == None), vals, 0))

def test_multi_delete_on_readonly():
    all_redis[0].slaveof(all_redis[1].args['host'], all_redis[1].args['port'])

    r = redis.Redis(nc.host(), nc.port())

    keys = ['key-1', 'key-2', 'key-3']

    actual_fails = 0
    for key in keys:
        for i in range(0, len(all_redis)):
            try:
                # got "READONLY You can't write against a read only slave"
                assert_fail('READONLY|Invalid', r.delete, key)
            except:
                actual_fails += 1
        assert_not_equal(0, actual_fails)

    actual_fails = 0
    for i in range(0, len(all_redis)):
        try:
            assert_fail('READONLY|Invalid', r.delete, *keys)     # got "Invalid argument"
        except:
            actual_fails += 1
    assert_not_equal(0, actual_fails)

def test_multi_delete_on_backend_down():
    #one backend down
    all_redis[0].stop()
    r = redis.Redis(nc.host(), nc.port())

    keys = ['key-1', 'key-2', 'kkk-3']
    # count the fails, should not attempt a match on the number of fails because config of
    # fault tolerance should be another test
    for i in range(0, len(all_redis)):
        actual_fails = 0
        for key in keys:
            try:
                assert_fail('Connection refused|reset by peer|Broken pipe', r.delete, key)
                assert_equal(None, r.get(key))
            except:
                actual_fails += 1
        assert_not_equal(0, actual_fails)

    # if the downed backend is ejected, this assertion will not hold
    actual_fails = 0
    for i in range(0, len(all_redis)):
        try:
            assert_fail('Connection refused|reset by peer|Broken pipe', r.delete, *keys)
        except:
            actual_fails += 1
    assert_not_equal(0, actual_fails)

    #all backend down
    for i in range(1, len(all_redis)):
        all_redis[i].stop()

    r = redis.Redis(nc.host(), nc.port())

    for key in keys:
        assert_fail('Connection refused|reset by peer|Broken pipe', r.delete, key)

    assert_fail('Connection refused|reset by peer|Broken pipe', r.delete, *keys)

    for r in all_redis:
        r.start()

def test_multi_delete_20140525():
    r = getconn()

    cnt = 126
    keys = ['key-%s'%i for i in range(cnt)]
    pipe = r.pipeline(transaction=False)
    pipe.mget(keys)
    pipe.delete(*keys)
    pipe.execute()


