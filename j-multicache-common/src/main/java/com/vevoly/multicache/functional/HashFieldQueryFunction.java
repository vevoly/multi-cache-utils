package com.vevoly.multicache.functional;

/**
 * 查询接口
 * @param <T>
 */
@FunctionalInterface
public interface HashFieldQueryFunction<T> {
    T query(String field);
}
