package com.github.vevoly.core;

import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.data.redis.core.script.DigestUtils;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 项目工具类
 */
public class CacheUtils {

    public final static String EMPTY_CACHE = "CACHE_NULL_VALUE";

    public static String getCacheKey(String namespace, String... key) {
        if (key == null || key.length == 0) {
            return namespace;
        }
        return namespace + ":" + String.join(":", key);
    }

    public static String getCacheKey(String namespace, Long... key) {
        return getCacheKey(namespace,
                Arrays.stream(key).map(String::valueOf).toArray(String[]::new));
    }

    /**
     * 将一个 Key 的集合转换为一个稳定的 MD5 哈希值。
     */
    public static String getMd5Key(Collection<?> keyList) {
        if (CollectionUtils.isEmpty(keyList)) return " ";
        String sortedKeysString = keyList.stream()
                .filter(Objects::nonNull)
                .map(Object::toString)
                .sorted()
                .collect(Collectors.joining(","));
        return DigestUtils.sha1DigestAsHex(sortedKeysString);
    }

    public static String getMd5Key(String namespace, Collection<?> keyList) {
        String md5Key = getMd5Key(keyList);
        return getCacheKey(namespace, md5Key);
    }

    public static String getMd5Key(String namespace, String... key) {
        return getMd5Key(namespace, Arrays.asList(key));
    }

    /**
     * 判断是否为空值标记
     * @param result
     * @return
     */
    public static boolean isSpecialEmptyData(Object result) {
        if (result == null) {
            return false;
        }

        if (EMPTY_CACHE.equals(result)) {
            return true;
        }

        // 判断集合是否为只包含一个特殊标记的 List和 Set
        if (result instanceof Collection) {
            Collection<?> collection = (Collection<?>) result;
            if (collection.isEmpty()) {
                return true;
            }
            return collection.size() == 1 && EMPTY_CACHE.equals(collection.iterator().next());
        }

        // Map 和 HashMap
        if (result instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) result;
            if (map.isEmpty()) {
                return true;
            }
            return map.size() == 1 && Boolean.TRUE.equals(map.get(EMPTY_CACHE));
        }
        return false;
    }

    /**
     * 检查L1缓存key
     * @param key
     * @return
     */
    public static String checkL1Key(String key) {

        // todo 如果原项目中的 redis key与现有项目 key 的规则不符合，请在这里写检查代码，为了确保L1中不会出现原项目中的 redis key

        return key;
    }

    /**
     * 判断数据库查询结果是否为空
     * @param result
     * @return
     */
    public static boolean isResultEmptyFromDb(Object result) {
        if (result == null) return true;
        if (result instanceof Collection) return ((Collection<?>) result).isEmpty();
        if (result instanceof Map) return ((Map<?, ?>) result).isEmpty();
        return false;
    }

    /**
     * 将空标记转换为空数据
     * 用于数据返回
     * @param result
     * @return
     * @param <T>
     */
    public static <T> T specialEmptyData2EmptyData(T result) {
        if (EMPTY_CACHE.equals(result)) {
            return null;
        }

        // 判断集合是否为只包含一个特殊标记的 List和 Set
        if (result instanceof Collection) {
            Collection<?> collection = (Collection<?>) result;
            if (collection.isEmpty()) {
                return result;
            }
            if (collection.size() == 1 && EMPTY_CACHE.equals(collection.iterator().next())) {
                if (result instanceof List) {
                    return (T) List.of();
                }
                if (result instanceof Set) {
                    return (T) Set.of();
                }
            }
        }

        // Map 和 HashMap
        if (result instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) result;
            if (map.isEmpty()) {
                return result;
            }
            if (map.size() == 1 && Boolean.TRUE.equals(map.get(EMPTY_CACHE))) {
                return (T) Map.of();
            }
        }
        return null;
    }

    /**
     * 判断是否为空
     * @param result
     * @return
     * @param <T>
     */
    public static <T> boolean isEmpty(T result) {
        if (result == null) return true;
        if (EMPTY_CACHE.equals(result)) return true; // 特殊空值标记
        if (result instanceof Collection) return ((Collection<?>) result).isEmpty();
        if (result instanceof Map) return ((Map<?, ?>) result).isEmpty();
        return false;
    }

    /**
     * 根据类型创建一个空数据标记
     * @param typeRef	类型
     * @return
     * @param <T>
     */
    public static <T> T createEmptyData(TypeReference<T> typeRef) {

        Type type = typeRef.getType();

        // 获取Type的原始类型 (raw type)
        Class<?> rawType = null;
        if (type instanceof Class<?>) {
            rawType = (Class<?>) type;
        } else if (type instanceof ParameterizedType) {

            // 如果是一个参数化的类型 (List<String>, Map<String, Object>) 取它的原始类型 (List, Map)
            rawType = (Class<?>) ((ParameterizedType) type).getRawType();
        }

        if (rawType != null) {
            if (List.class.isAssignableFrom(rawType)) {
                return (T) Collections.singletonList(EMPTY_CACHE);
            }
            if (Set.class.isAssignableFrom(rawType)) {
                return (T) Collections.singleton(EMPTY_CACHE);
            }
            if (Map.class.isAssignableFrom(rawType)) {
                return (T) new String(EMPTY_CACHE);
            }
        }
        return (T) EMPTY_CACHE;
    }
}
