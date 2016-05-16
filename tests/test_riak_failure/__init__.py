#!/usr/bin/env python
#coding: utf-8
from riak_common import *

def setup():
    cluster_setup_for_partition()

def teardown():
    cluster_teardown_for_partition()
