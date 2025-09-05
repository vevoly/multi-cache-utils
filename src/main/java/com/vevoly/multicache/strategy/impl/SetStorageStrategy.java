package com.vevoly.multicache.strategy.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vevoly.multicache.enums.StorageType;
import com.vevoly.multicache.strategy.RedisStorageStrategy;
import com.vevoly.multicache.utils.CacheUtils;
import com.vevoly.multicache.utils.RedisUtils;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.redisson.api.RBatch;
import org.redisson.api.RFuture;
import org.redisson.api.RSetAsync;
import org.redisson.client.codec.StringCodec;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
@AllArgsConstructor
public class SetStorageStrategy implements RedisStorageStrategy<Set<?>> {

    private final ObjectMapper objectMapper;
    private static final String EMPTY_MARK = CacheUtils.EMPTY_CACHE;

    @Override
    public StorageType getStorageType() {
        return StorageType.SET;
    }

    @Override
    public Set<?> read(RedisUtils redisUtils, String key, TypeReference<Set<?>> typeRef) {
        Set<Object> rawSet = redisUtils.sGet(key);
        if (CollectionUtils.isEmpty(rawSet)) return null;

        if (CacheUtils.isSpecialEmptyData(rawSet)) {
            return rawSet;
        }

        // 进行类型安全的转换
        return objectMapper.convertValue(rawSet, typeRef);
    }

    @Override
    public <V> Map<String, CompletableFuture<Optional<V>>> readMulti(RBatch batch, List<String> keysToRead, TypeReference<V> typeRef) {
        Map<String, CompletableFuture<Optional<V>>> finalFutures = new HashMap<>();
        JavaType targetType = objectMapper.constructType(typeRef.getType());

        for (String key : keysToRead) {

            // 1. 获取原始Set的Future
            RFuture<Set<Object>> rawFuture = batch.getSet(key).readAllAsync();

            // 2. 链式处理
            CompletableFuture<Optional<V>> finalFuture = rawFuture.toCompletableFuture()
                    .thenApply(rawSet -> {
                        if (rawSet == null || rawSet.isEmpty()) {
                            return null; // 代表 MISS
                        }

                        if (CacheUtils.isSpecialEmptyData(rawSet)) {
                            return Optional.empty(); // 代表 HIT_EMPTY
                        }

                        try {

                            // 将 Redis 返回的 Set<Object> 反序列化为调用者期望的类型 V (V 应该是 Set<T>)
                            V value = objectMapper.convertValue(rawSet, targetType);
                            return Optional.of(value);
                        } catch (Exception e) {
                            log.error("SetStorageStrategy: Failed to deserialize for key: {}. Error: {}", key, e.getMessage());
                            return null; // 反序列化失败，视为 MISS
                        }
                    });

            finalFutures.put(key, finalFuture);
        }
        return finalFutures;
    }

    @Override
    public void write(RedisUtils redisUtils, String key, Set<?> value, Duration ttl) {
        if (value == null || value.isEmpty()) {
            redisUtils.del(key); // 如果值为空，则删除key
            return;
        }

        // 将成员转换为 String 数组
        String[] stringMembers = value.stream()
                .map(String::valueOf)
                .toArray(String[]::new);

        redisUtils.sSet(key, ttl.toSeconds(), (Object[]) stringMembers);
    }

    @Override
    public void writeMulti(RBatch batch, Map<String, Set<?>> dataToCache, Duration ttl) {
        if (dataToCache == null || dataToCache.isEmpty()) {
            return;
        }

        dataToCache.forEach((key, valueSet) -> {
            if (valueSet != null && !valueSet.isEmpty()) {
                RSetAsync<String> rSet = batch.getSet(key, StringCodec.INSTANCE);

                // 1. 先删除旧的key，确保是覆盖写
                rSet.deleteAsync();

                // 2. 将传入的 Set<?> 的每个成员转换为 String
                Set<String> stringMembers = valueSet.stream()
                        .map(String::valueOf) // 对每个成员调用 String.valueOf()
                        .collect(Collectors.toSet());

                // 3. 将转换后的 Set<String> 写入Redis
                rSet.addAllAsync(stringMembers);
                rSet.expireAsync(ttl);
            }
        });
    }

    @Override
    public void writeMultiEmpty(RBatch batch, List<String> keysToMarkEmpty, Duration emptyTtl) {
        if (keysToMarkEmpty == null || keysToMarkEmpty.isEmpty()) {
            return;
        }
        for (String key : keysToMarkEmpty) {
            RSetAsync<String> rSet = batch.getSet(key, StringCodec.INSTANCE);

            // 先删除旧的，再添加空标记
            rSet.deleteAsync();
            rSet.addAsync(EMPTY_MARK);
            rSet.expireAsync(emptyTtl);
        }
    }

    /**
     * 重写 readUnion 方法以支持 SUNION 操作。
     */
    @Override
    public <E> UnionReadResult<E> readUnion(RedisUtils redisUtils, List<String> keys, TypeReference<Set<E>> typeRef) {
        if (CollectionUtils.isEmpty(keys)) {
            return UnionReadResult.empty();
        }

        try {

            // 1. 调用高效的 Lua 脚本方法
            Map<String, Object> rawResult = redisUtils.sunionAndFindMisses(keys);
            Set<String> rawUnionSet = (Set<String>) rawResult.get("unionResult");
            List<String> missedKeys = (List<String>) rawResult.get("missedKeys");

            // 2. 在转换前，对 rawUnionSet 进行空标记检查和处理
            if (CollectionUtils.isEmpty(rawUnionSet)) {

                // 并集结果是空的
                return new UnionReadResult<>(Collections.emptySet(), missedKeys);
            }

            if (CacheUtils.isSpecialEmptyData(rawUnionSet)) {

                // 并集结果本身就是一个“空标记集合”， 这种情况下，有效的并集结果也是一个空集
                return new UnionReadResult<>(Collections.emptySet(), missedKeys);
            }

            // 命中真实数据，可能混合了空标记
            Set<String> filteredSet = rawUnionSet.stream()
                    .filter(member -> !EMPTY_MARK.equals(member))
                    .collect(Collectors.toSet());

            // 2. 类型转换
            Set<E> unionResult = convertStringSetToTypedSet(rawUnionSet, typeRef);

            // 3. 封装并返回结果
            return new UnionReadResult<>(unionResult, missedKeys);

        } catch (Exception e) {
            log.error("SetStorageStrategy: Failed to read union for keys: {}. Error: {}", keys, e.getMessage(), e);

            // 在策略层遇到异常，可以返回空集合或向上抛出
            return new UnionReadResult<>(Collections.emptySet(), keys);
        }
    }

    /**
     * 将一个原始的字符串集合，安全地转换为一个指定泛型类型的集合。
     *
     * @param rawSet   从Redis获取的原始 Set<String>
     * @param typeRef  目标类型的 TypeReference，例如 new TypeReference<Set<Long>>() {}
     * @return 转换后的、类型安全的 Set<E>
     * @param <E> 目标集合中元素的类型
     */
    private <E> Set<E> convertStringSetToTypedSet(Set<String> rawSet, TypeReference<Set<E>> typeRef) {
        if (CollectionUtils.isEmpty(rawSet)) {
            return Collections.emptySet();
        }

        // 在进行任何类型转换之前，先过滤掉所有已知的空值标记字符串。
        Set<String> cleanSet = rawSet.stream()
                .filter(member -> !EMPTY_MARK.equals(member))
                .collect(Collectors.toSet());

        // 如果过滤后集合为空，说明原始集合只包含空标记或为空
        if (CollectionUtils.isEmpty(cleanSet)) {
            return Collections.emptySet();
        }

        // 现在，我们可以安全地对“干净”的 cleanSet 进行类型转换
        try {
            JavaType targetSetType = objectMapper.constructType(typeRef.getType());
            Class<?> elementClass = targetSetType.getContentType().getRawClass();

            if (Long.class.isAssignableFrom(elementClass)) {
                return (Set<E>) cleanSet.stream()
                        .map(Long::parseLong)
                        .collect(Collectors.toSet());
            }
            if (Integer.class.isAssignableFrom(elementClass)) {
                return (Set<E>) cleanSet.stream()
                        .map(Integer::parseInt)
                        .collect(Collectors.toSet());
            }
            if (String.class.isAssignableFrom(elementClass)) {
                return (Set<E>) cleanSet;
            }

            return objectMapper.convertValue(cleanSet, typeRef);

        } catch (NumberFormatException e) {
            log.error("SetStorageStrategy: Set member in Redis is not a valid number after filtering empty markers. Clean Set: {}", cleanSet, e);
            return Collections.emptySet();
        } catch (Exception e) {
            log.error("SetStorageStrategy: Generic type conversion failed for clean Set. Clean Set: {}", cleanSet, e);
            return Collections.emptySet();
        }
    }
}