package com.vevoly.multicache.strategy.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vevoly.multicache.enums.StorageType;
import com.vevoly.multicache.strategy.RedisStorageStrategy;
import com.vevoly.multicache.utils.CacheUtils;
import com.vevoly.multicache.core.RedisUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBatch;
import org.redisson.api.RFuture;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public class StringStorageStrategy implements RedisStorageStrategy<Object> {

    private final ObjectMapper objectMapper;

    @Override
    public StorageType getStorageType() {
        return StorageType.STRING;
    }

    @Override
    public Object read(RedisUtils redisUtils, String key, TypeReference<Object> typeRef) {
        Object rawValue = redisUtils.get(key);
        if (rawValue == null) {
            return null;
        }

        // 检查空值标记
        if (CacheUtils.isSpecialEmptyData(rawValue)) {
            return rawValue; // 是占位符，直接返回
        }

        try {
            return objectMapper.convertValue(rawValue, typeRef);
        } catch (Exception e) {
            log.error("Failed to convert cache value to {}. Key: {}, RawValue Type: {}",
                    typeRef.getType(), key, rawValue.getClass().getName(), e);
            return null;
        }
    }

    @Override
    public <V> Map<String, CompletableFuture<Optional<V>>> readMulti(RBatch batch, List<String> keysToRead, TypeReference<V> typeRef) {
        Map<String, CompletableFuture<Optional<V>>> finalFutures = new HashMap<>();
        JavaType targetType = objectMapper.constructType(typeRef.getType());

        for (String key : keysToRead) {

            // 1. 获取原始数据的Future
            RFuture<Object> rawFuture = batch.getBucket(key).getAsync();

            // 2. 链式处理：当原始Future完成后，执行类型转换
            CompletableFuture<Optional<V>> finalFuture = rawFuture.toCompletableFuture()
                    .thenApply(rawValue -> {
                        if (rawValue == null) {

                            // 返回空，去数据库查找
                            return null;
                        }
                        if (CacheUtils.isSpecialEmptyData(rawValue)) {
                            return Optional.empty();
                        }

                        // 将类型转换的逻辑封装在策略的异步流程中
                        V convertedValue = objectMapper.convertValue(rawValue, targetType);
                        return Optional.of(convertedValue);
                    });
            finalFutures.put(key, finalFuture);
        }
        return finalFutures;
    }

    @Override
    public void write(RedisUtils redisUtils, String key, Object value, Duration ttl) {

        // todo 空值的回种，因为value是null所以未生效
        redisUtils.set(key, value, ttl);
    }

    @Override
    public void writeMulti(RBatch batch, Map<String, Object> dataToCache, Duration ttl) {
        if (dataToCache == null) return;
        dataToCache.forEach((key, value) -> {
            batch.getBucket(key).setAsync(value, ttl);
        });
    }

    @Override
    public void writeMultiEmpty(RBatch batch, List<String> keysToMarkEmpty, Duration emptyTtl) {
        if (keysToMarkEmpty == null) return;
        String placeholder = CacheUtils.EMPTY_CACHE;

        keysToMarkEmpty.forEach(key -> {
            batch.getBucket(key).setAsync(placeholder, emptyTtl);
        });
    }
}
