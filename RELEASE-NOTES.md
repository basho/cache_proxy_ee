# Riak Redis Add-on 1.1.0 Release Notes

## New Features

### Write-around Cache for String Datatype

With existing support for read-through caching through GET, write-around caching
has been added through SET and DEL. Writes through the Cache Proxy invalidate
cache, providing an increase in cache coherence without being subject to a race
condition.

# Riak Redis Add-on 1.0.0 Release Notes

### Read-through Cache for String Datatype

Read-through caching through GET, reduces latency while also reducing the window
of time that the cache may not be coherent through a configurable time-to-live
(TTL).
