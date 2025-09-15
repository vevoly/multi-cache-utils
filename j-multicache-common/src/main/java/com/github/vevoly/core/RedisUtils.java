package com.github.vevoly.core;

import org.redisson.api.RLock;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface RedisUtils {

    <T> T get(String key);
    <R> List<R> getList(String key);
    boolean set(String key, Object value, Duration ttl);
    <T> void setList(String key, List<T> value, Duration ttl);


    // --- Set Operations ---
    <T> Set<T> sGet(String key);
    long sSet(String key, long time, Object... values);
    Set<String> sUnion(List<String> keys);
    Map<String, Object> sunionAndFindMisses(List<String> keys);

    // --- Hash Operations ---
    <HK, HV> HV hget(String key, HK hashKey);
    <HK, HV> Map<HK, HV> hmget(String key);
    boolean hset(String key, String item, Object value, long time);
    boolean hmset(String key, Map<String, Object> map, Duration ttl);

    // --- Key Operations ---
    void del(String... keys);

    // --- Lock Operations ---
    RLock getLock(String lockKey);
    boolean tryLock(String lockKey, long waitTime, long leaseTime, TimeUnit timeUnit);
    void unlock(String lockKey);
    boolean isRateLimit(String key, int maxCount, int windowSeconds);
}
