package com.github.vevoly.strategy.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.vevoly.enums.StorageType;
import com.github.vevoly.strategy.FieldBasedStorageStrategy;
import com.github.vevoly.strategy.RedisStorageStrategy;
import com.github.vevoly.core.CacheUtils;
import com.github.vevoly.core.RedisUtils;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.MapUtils;
import org.redisson.api.RBatch;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class HashStorageStrategy implements RedisStorageStrategy<Map<String, ?>>, FieldBasedStorageStrategy<Object> {

    private final ObjectMapper objectMapper;

    @Override
    public StorageType getStorageType() {
        return StorageType.HASH;
    }

    /**
     * 整体存取
     * @param redisUtils
     * @param key Redis 键
     * @param typeRef
     * @return
     */
    @Override
    public Map<String, ?> read(RedisUtils redisUtils, String key, TypeReference<Map<String, ?>> typeRef) {
        Map<String, ?> map = redisUtils.hmget(key);
        if (MapUtils.isEmpty(map)) return null;

        // 如果是标记空值
        if (map.size() == 1 && Boolean.TRUE.equals(map.get(CacheUtils.EMPTY_CACHE))) {
            return map;
        }
        return map;
    }

    @Override
    public <V> Map<String, CompletableFuture<Optional<V>>> readMulti(RBatch batch, List<String> keysToRead, TypeReference<V> typeRef) {
        return Map.of();
    }

    @Override
    public void write(RedisUtils redisUtils, String key, Map<String, ?> value, Duration ttl) {
        redisUtils.hmset(key, (Map<String, Object>) value, ttl);
    }

    @Override
    public void writeMulti(RBatch batch, Map<String, Map<String, ?>> dataToCache, Duration ttl) {

    }

    @Override
    public void writeMultiEmpty(RBatch batch, List<String> keysToMarkEmpty, Duration emptyTtl) {

    }

    /**
     * 字段级存取
     * @param redisUtils Redis 工具类
     * @param key        Redis 的主键
     * @param field      要获取的字段
     * @param fieldType  字段值的 Class 类型
     * @return
     */
    @Override
    public Object readField(RedisUtils redisUtils, String key, String field, Class<Object> fieldType) {
        Object rawValue = redisUtils.hget(key, field);

        // 检查空值标记
        if (rawValue instanceof String && CacheUtils.EMPTY_CACHE.equals(rawValue)) {
            return rawValue;
        }
        return objectMapper.convertValue(rawValue, fieldType);
    }

    @Override
    public void writeField(RedisUtils redisUtils, String key, String field, Object value, Duration ttl) {
        redisUtils.hset(key, field, value, ttl.toSeconds());
    }
}