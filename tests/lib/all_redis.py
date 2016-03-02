#!/usr/bin/env python
#coding: utf-8

import redis

class AllRedis:
    def __init__(self, all_redis):
        self.redise_servers = []
        for r in all_redis:
            self.redise_servers.append(redis.Redis(r.host(), r.port()))
            
    def delete(self, key):
        res = 0
        for r in self.redise_servers:
            res += r.delete(key)
        return res
            
    def get(self, key):
        for r in self.redise_servers:
            res = r.get(key)
            if res != None:
                return res
        return None
