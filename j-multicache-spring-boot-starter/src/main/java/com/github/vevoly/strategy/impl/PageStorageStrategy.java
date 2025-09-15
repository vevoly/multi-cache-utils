package com.github.vevoly.strategy.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.vevoly.enums.StorageType;
import com.github.vevoly.strategy.RedisStorageStrategy;
import com.github.vevoly.utils.CacheUtils;
import com.github.vevoly.core.RedisUtils;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBatch;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Page 策略，用于缓存Mybatis Page数据类型
 */
@Slf4j
@AllArgsConstructor
public class PageStorageStrategy implements RedisStorageStrategy<Object> {

    private final ObjectMapper objectMapper;

    private static final String EMPTY_MARK = CacheUtils.EMPTY_CACHE;

    @Override
    public StorageType getStorageType() {
        return StorageType.PAGE;
    }

    @Override
    public Object read(RedisUtils redisUtils, String key, TypeReference<Object> typeRef) {
        String json = redisUtils.get(key);
        if (!StringUtils.hasText(json)) return null;

        if (EMPTY_MARK.equals(json)) {
            return EMPTY_MARK;
        }

        try {

            // 使用 TypeReference 来正确反序列化包含泛型的 Page 对象
            return objectMapper.readValue(json, typeRef);
        } catch (Exception e) {
            log.error("PageStorageStrategy: Failed to deserialize JSON for key: {}. JSON: '{}'", key, json, e);
            return null;
        }
    }

    @Override
    public <V> Map<String, CompletableFuture<Optional<V>>> readMulti(RBatch batch, List<String> keysToRead, TypeReference<V> typeRef) {

		/*
		Page 类型的缓存不适合批量读取，因为每个 Page 的泛型都可能不同
		getMultiData 应该避免用于 Page 类型。如果非要实现，逻辑会非常复杂
		抛出异常，强制使用者用 getSingleData
		 */
        throw new UnsupportedOperationException("PageStorageStrategy does not support readMulti operation. Please use getSingleData for Page objects.");
    }

    @Override
    public void write(RedisUtils redisUtils, String key, Object value, Duration ttl) {
        try {
            String jsonValue = (value instanceof String) ? (String) value : objectMapper.writeValueAsString(value);
            redisUtils.set(key, jsonValue, ttl);
        } catch (Exception e) {
            log.error("PageStorageStrategy: Failed to serialize value for key: {}", key, e);
        }
    }

    @Override
    public void writeMulti(RBatch batch, Map<String, Object> dataToCache, Duration ttl) {
        throw new UnsupportedOperationException("PageStorageStrategy does not support writeMulti operation.");
    }

    @Override
    public void writeMultiEmpty(RBatch batch, List<String> keysToMarkEmpty, Duration emptyTtl) {
        throw new UnsupportedOperationException("PageStorageStrategy does not support writeMultiEmpty operation.");
    }
}