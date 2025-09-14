package com.vevoly.multicache.strategy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.vevoly.multicache.enums.StorageType;
import com.vevoly.multicache.core.RedisUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.redisson.api.RBatch;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * redis策略
 * @param <T>
 */
public interface RedisStorageStrategy<T> {

    /**
     * 返回当前策略支持的存储类型。
     *
     * @return StorageTypeEnum
     */
    StorageType getStorageType();

    /**
     * 从 Redis 中读取数据
     *
     * @param key Redis 键
     * @return 查询到的数据
     */
    T read(RedisUtils redisUtils, String key, TypeReference<T> typeRef);

    /**
     * 异步批量读取并转换数据。
     * 此方法将读取和类型转换的完整逻辑封装起来。
     *
     * @param batch      Redisson的RBatch对象
     * @param keysToRead 需要读取的Redis Key列表
     * @param typeRef    目标类型的TypeReference，用于精确转换
     * @return 一个Map，Key是Redis Key，Value是包含了最终类型数据的CompletableFuture
     */
    <V> Map<String, CompletableFuture<Optional<V>>> readMulti(RBatch batch, List<String> keysToRead, TypeReference<V> typeRef);

    /**
     * 将数据写入 Redis
     *
     * @param key   Redis 键
     * @param value 要写入的数据
     * @param ttl   过期时间
     */
    void write(RedisUtils redisUtils, String key, T value, Duration ttl);

    /**
     * 批量将真实数据写入Redis。
     * 每个策略根据自己的类型来实现具体的存储方式。
     *
     * @param batch       Redisson的RBatch对象，用于pipeline操作
     * @param dataToCache Key是Redis的Key，Value是要缓存的Java对象
     * @param ttl         正常过期时间
     */
    void writeMulti(RBatch batch, Map<String, T> dataToCache, Duration ttl);

    /**
     * 批量写入空值占位符。
     *
     * @param batch           Redisson的RBatch对象
     * @param keysToMarkEmpty 需要标记为空的Redis Key列表
     * @param emptyTtl        空值占位符的短过期时间
     */
    void writeMultiEmpty(RBatch batch, List<String> keysToMarkEmpty, Duration emptyTtl);

    /**
     * 用于封装 readUnion 返回结果的包装类
     *
     * @param <E>
     */
    @Getter
    @AllArgsConstructor
    class UnionReadResult<E> {

        // L2中存在的keys计算出的并集
        private final Set<E> unionResult;

        // 在L2中不存在的keys
        private final List<String> missedKeys;

        public static <E> UnionReadResult<E> empty() {
            return new UnionReadResult<>(Collections.emptySet(), Collections.emptyList());
        }
    }

    /**
     * 从 Redis 读取多个集合的并集。
     * 默认实现为抛出不支持操作异常，只有 Set 策略需要实现它。
     *
     * @param redisUtils Redis 工具类
     * @param keys       要计算并集的集合的 Key 列表
     * @param typeRef    结果集的 TypeReference
     * @param <E>        包含结果集和未命中key集合
     * @return 计算出的并集
     */
    default <E> UnionReadResult<E> readUnion(RedisUtils redisUtils, List<String> keys, TypeReference<Set<E>> typeRef) {
        throw new UnsupportedOperationException(getStorageType() + " storage strategy does not support readUnion operation.");
    }

}