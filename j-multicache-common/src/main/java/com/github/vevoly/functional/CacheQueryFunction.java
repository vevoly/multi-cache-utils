package com.github.vevoly.functional;

/**
 * 查询接口
 * @Author vevoly
 * @param <T>
 */
@FunctionalInterface
public interface CacheQueryFunction<T> {
    T query();
}
