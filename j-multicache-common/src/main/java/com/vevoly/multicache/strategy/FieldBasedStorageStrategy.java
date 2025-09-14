package com.vevoly.multicache.strategy;



import com.vevoly.multicache.core.RedisUtils;

import java.time.Duration;

/**
 * 为支持字段级操作的存储策略（如 Redis Hash）定义的扩展接口
 */
public interface FieldBasedStorageStrategy<V> {

    /**
     * 从一个集合型缓存中读取单个字段的值。
     *
     * @param redisUtils Redis 工具类
     * @param key        Redis 的主键
     * @param field      要获取的字段
     * @param fieldType  字段值的 Class 类型
     * @return 字段对应的值
     */
    default V readField(RedisUtils redisUtils, String key, String field, Class<V> fieldType) {
        throw new UnsupportedOperationException("storage strategy does not support readField operation.");
    }

    /**
     * 向一个集合型缓存中写入单个字段的值。
     *
     * @param redisUtils Redis 工具类
     * @param key        Redis 的主键
     * @param field      要设置的字段
     * @param value      要设置的值
     * @param ttl        整个主键的过期时间
     */
    default void writeField(RedisUtils redisUtils, String key, String field, V value, Duration ttl) {
        throw new UnsupportedOperationException("storage strategy does not support writeField operation.");
    }

}

