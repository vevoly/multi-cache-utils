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
import org.redisson.api.RFuture;
import org.redisson.api.RListAsync;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Slf4j
@AllArgsConstructor
public class ListStorageStrategy implements RedisStorageStrategy<List<?>> {

    private final ObjectMapper objectMapper;

    private static final String EMPTY_MARK = CacheUtils.EMPTY_CACHE;

    @Override
    public StorageType getStorageType() {
        return StorageType.LIST;
    }

    @Override
    public List<?> read(RedisUtils redisUtils, String key, TypeReference<List<?>> typeRef) {
        List<?> rawList = redisUtils.getList(key);
        if (CollectionUtils.isEmpty(rawList)) return null;

        // 如果是标记空值
        if (CacheUtils.isSpecialEmptyData(rawList)) {
            return rawList;
        }

        // 进行类型安全的转换
        return objectMapper.convertValue(rawList, typeRef);
    }

    @Override
    public <V> Map<String, CompletableFuture<Optional<V>>> readMulti(RBatch batch, List<String> keysToRead, TypeReference<V> typeRef) {
        Map<String, CompletableFuture<Optional<V>>> finalFutures = new HashMap<>();
        var targetType = objectMapper.constructType(typeRef.getType());

        for (String key : keysToRead) {

            // 1. 获取原始List的Future
            RFuture<List<Object>> rawFuture = batch.getList(key).readAllAsync();

            // 2. 链式处理：当原始List获取后，执行类型转换
            CompletableFuture<Optional<V>> finalFuture = rawFuture.toCompletableFuture()
                    .thenApply(rawList -> {

                        // Redis中没有这个Key
                        if (rawList == null || rawList.size() == 0) {
                            return null;
                        }

                        // 命中空值占位符
                        if (CacheUtils.isSpecialEmptyData(rawList)) {
                            return Optional.empty();
                        }

                        // 命中真实数据
                        try {

                            // 将 Redis 返回的 List<Object> (实际是List<String>) 反序列化为调用者期望的类型 V
                            // 如果 V 是 List<MyEntity>，这里就会得到一个 ArrayList<MyEntity>
                            V value = objectMapper.convertValue(rawList, targetType);

                            // 将反序列化后的结果 value，
                            // 用 Optional.of() 包装起来，以符合方法的返回类型。
                            return Optional.of(value);

                        } catch (Exception e) {
                            log.error("Failed to deserialize for key: {}. Error: {}", key, e.getMessage());

                            // 反序列化失败，也视为 MISS
                            return null;
                        }

                    });

            finalFutures.put(key, finalFuture);
        }
        return finalFutures;
    }

    @Override
    public void write(RedisUtils redisUtils, String key, List<?> value, Duration ttl) {
        redisUtils.setList(key, value, ttl);
    }

    @Override
    public void writeMulti(RBatch batch, Map<String, List<?>> dataToCache, Duration ttl) {
        if (dataToCache == null) return;
        dataToCache.forEach((key, valueList) -> {
            if (valueList != null && !valueList.isEmpty()) {
                RListAsync<Object> rList = batch.getList(key);
                rList.deleteAsync();
                rList.addAllAsync(valueList);
                rList.expireAsync(ttl);
            }
        });
    }

    @Override
    public void writeMultiEmpty(RBatch batch, List<String> keysToMarkEmpty, Duration emptyTtl) {
        if (keysToMarkEmpty == null || keysToMarkEmpty.isEmpty()) {
            return;
        }
        for (String key : keysToMarkEmpty) {
            RListAsync<Object> rList = batch.getList(key);
            rList.deleteAsync();
            rList.addAsync(EMPTY_MARK);
            rList.expireAsync(emptyTtl);
        }
    }
}