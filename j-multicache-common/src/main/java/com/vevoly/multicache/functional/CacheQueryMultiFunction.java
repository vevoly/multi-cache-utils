package com.vevoly.multicache.functional;

import java.util.List;
import java.util.Map;

/**
 * 查询接口
 * @param <K>
 * @param <V>
 */
@FunctionalInterface
public interface CacheQueryMultiFunction<K, V> {
    Map<K, V> query(List<K> missingKeys);
}
