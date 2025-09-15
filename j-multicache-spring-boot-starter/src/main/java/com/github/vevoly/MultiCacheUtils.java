package com.github.vevoly;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.vevoly.config.MultiCacheProperties;
import com.github.vevoly.enums.StoragePolicy;
import com.github.vevoly.enums.StorageType;
import com.github.vevoly.functional.CacheQueryFunction;
import com.github.vevoly.functional.CacheQueryMultiFunction;
import com.github.vevoly.functional.HashFieldQueryFunction;
import com.github.vevoly.resolver.CacheConfigResolver;
import com.github.vevoly.strategy.FieldBasedStorageStrategy;
import com.github.vevoly.strategy.RedisStorageStrategy;
import com.github.vevoly.utils.CacheUtils;
import com.github.vevoly.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBatch;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.util.StopWatch;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class MultiCacheUtils {

    private final RedisUtils redisUtils;
    private final Executor asyncExecutor;
    private final ObjectMapper objectMapper;
    private final CacheConfigResolver configResolver;
    private final CaffeineCacheManager caffeineCacheManager;

    public static final String LOG_PREFIX = "[MultiCache] ";

    private Map<StorageType, RedisStorageStrategy<?>> strategyMap = new ConcurrentHashMap<>();

    public MultiCacheUtils(RedisUtils redisUtils,
                           @Qualifier("caffeineCacheManager") CaffeineCacheManager caffeineCacheManager,
                           List<RedisStorageStrategy<?>> strategies,
                           ObjectMapper objectMapper,
                           @Qualifier("asyncMultiCacheExecutor") Executor asyncExecutor,
                           CacheConfigResolver configResolver) {

        this.redisUtils = redisUtils;
        this.configResolver = configResolver;
        this.objectMapper = objectMapper;
        this.asyncExecutor = asyncExecutor;
        this.caffeineCacheManager = caffeineCacheManager;

        if (strategies != null) {
            this.strategyMap = strategies.stream()
                    .collect(Collectors.toMap(RedisStorageStrategy::getStorageType, Function.identity()));
        }
    }

    // =================================================================
    // ======================== 公共 API 方法 ==========================
    // =================================================================

    /**
     * 【方法一：整体存取】获取一个完整的数据实体。数据结构可以时 entity、list、set等
     * L1 -> L2 -> DB 完整缓存逻辑。
     * 例子：TenantInfoEntity getTenantInfo(String tenantId)
     *
     * @param key          Redis 键
     * @param typeRef      返回结果的 TypeReference，用于处理泛型
     * @param queryFunction 当缓存未命中时，用于从数据库查询的函数
     * @return 查询到的数据，或 null
     */
    public <T> T getSingleData(String key, TypeReference<T> typeRef, CacheQueryFunction<T> queryFunction) {
        return getSingleDataInternal(key, typeRef, queryFunction, StoragePolicy.L1_L2_DB);
    }

    /**
     * 【方法二：Redis存取】获取一个完整的数据实体。。数据结构可以时 entity、list、set等
     *  L2 -> DB 缓存逻辑
     *  例子请查看：getSystemNoticeMessageCount(Long userId, Long siteUserLevelId, String tenantId, Long noticeColumnsId)
     * @param key
     * @param typeRef
     * @param queryFunction
     * @return
     * @param <T>
     */
    public <T> T getSingleDataFromL2(String key, TypeReference<T> typeRef, CacheQueryFunction<T> queryFunction) {
        return getSingleDataInternal(key, typeRef, queryFunction, StoragePolicy.L2_DB);
    }

    /**
     * 【方法三：本地缓存存取】获取一个完整的数据实体。。数据结构可以时 entity、list、set等
     * 	L1 -> DB 缓存逻辑
     * 	不建议使用此方法，如果L1没有命中就会直接打到数据库，L1不缓存空值，因为L1不能单独设置空值缓存时间
     * 	应直接使用 L1 -> L2 -> DB 完整缓存逻辑。
     * @param key
     * @param typeRef
     * @param queryFunction
     * @return
     * @param <T>
     */
    public <T> T getSingleDataFromL1(String key, TypeReference<T> typeRef, CacheQueryFunction<T> queryFunction) {
        return getSingleDataInternal(key, typeRef, queryFunction, StoragePolicy.L1_DB);
    }

    /**
     * 【方法一：整体存取】根据ID列表，从多级缓存批量获取数据实体
     * 。数据结构可以时 entity、list、set等
     * 例子：
     * 一对一：multiCacheUtils.getMultiData(userIds, new TypeReference<UserEntity>() {}, ...)
     * 一对一的例子请查看：
     * 一对多：multiCacheUtils.getMultiData(noticeIdList, new TypeReference<List<TenantNoticePositionEntity>>() {}, ...)
     * 一对多的例子请查看： getNoticeUserTypeList(Long userId, Long userLevelId, List<Long> noticeIdList) 方法
     * @param ids           要查询的ID/复合键集合
     * @param valueTypeRef  返回Map中Value的Class类型
     * @param keyBuilder    一个函数，用于根据单个ID/复合键构建Redis Key
     * @param queryFunction 一个函数，用于从数据库批量查询
     * @return 一个Map，键是ID/复合键，值是对应的实体。
     * @param <K> ID/复合键的类型
     * @param <V> 实体的类型
     */
    public <K, V> Map<K, V> getMultiData(Collection<K> ids,
                                         TypeReference<V> valueTypeRef,
                                         Function<K, String> keyBuilder,
                                         CacheQueryMultiFunction<K, V> queryFunction) {
        return getMultiDataInternal(ids, valueTypeRef, keyBuilder, queryFunction, StoragePolicy.L1_L2_DB);
    }

    /**
     * 【方法二：Redis存取】根据ID列表，从多级缓存批量获取数据实体
     * @param ids
     * @param valueTypeRef
     * @param keyBuilder
     * @param queryFunction
     * @return
     * @param <K>
     * @param <V>
     */
    public <K, V> Map<K, V> getMultiDataFromL2(Collection<K> ids, TypeReference<V> valueTypeRef,
                                               Function<K, String> keyBuilder, CacheQueryMultiFunction<K, V> queryFunction) {
        return getMultiDataInternal(ids, valueTypeRef, keyBuilder, queryFunction, StoragePolicy.L2_DB);
    }

    /**
     * 【方法三：本地缓存存取】根据ID列表，从多级缓存批量获取数据实体
     * 不建议使用此方法，如果L1没有命中就会直接打到数据库，L1不缓存空值，因为L1不能单独设置空值缓存时间
     * 应直接使用 L1 -> L2 -> DB 完整缓存逻辑。
     * @param ids
     * @param valueTypeRef
     * @param keyBuilder
     * @param queryFunction
     * @return
     * @param <K>
     * @param <V>
     */
    public <K, V> Map<K, V> getMultiDataFromL1(Collection<K> ids, TypeReference<V> valueTypeRef,
                                               Function<K, String> keyBuilder, CacheQueryMultiFunction<K, V> queryFunction) {
        return getMultiDataInternal(ids, valueTypeRef, keyBuilder, queryFunction, StoragePolicy.L1_DB);
    }

    /**
     * 【方法一：整体存取】获取多个 Set 的并集。
     * 使用默认的 L1_L2_DB 缓存策略。
     *
     * @param unionCacheKey L1(Caffeine)中用于缓存最终并集结果的Key
     * @param setKeysInRedis L2(Redis)中要进行 SUNION 运算的多个Set的Key
     * @param typeRef       返回结果的 TypeReference，例如 new TypeReference<Set<String>>() {}
     * @param dbQueryFunction 当缓存未命中时，用于从数据库查询最终结果的函数
     * @return 最终的并集结果 Set
     * @param <T> Set 中元素的类型
     */
    public <T> Set<T> getUnionData(String unionCacheKey,
                                   List<String> setKeysInRedis,
                                   TypeReference<Set<T>> typeRef,
                                   Function<List<String>, Map<String, Set<T>>> dbQueryFunction
    ) {
        return getUnionDataInternal(unionCacheKey, setKeysInRedis, typeRef, dbQueryFunction, StoragePolicy.L1_L2_DB);
    }

    /**
     * 【方法二：Redis存取】从L2(Redis)或DB获取多个 Set 的并集。
     */
    public <T> Set<T> getUnionDataFromL2(String unionCacheKey, List<String> setKeysInRedis, TypeReference<Set<T>> typeRef,
                                         Function<List<String>, Map<String, Set<T>>> dbQueryFunction) {
        return getUnionDataInternal(unionCacheKey, setKeysInRedis, typeRef, dbQueryFunction, StoragePolicy.L2_DB);
    }

    /**
     * 【方法三：本地缓存存取】从L1(Caffeine)或DB获取多个 Set 的并集。
     */
    public <T> Set<T> getUnionDataFromL1(String unionCacheKey, List<String> setKeysInRedis, TypeReference<Set<T>> typeRef,
                                         Function<List<String>, Map<String, Set<T>>> dbQueryFunction) {
        return getUnionDataInternal(unionCacheKey, setKeysInRedis, typeRef, dbQueryFunction, StoragePolicy.L1_DB);
    }

    /**
     * 从hash中获取一个field
     * 这个方法缓存空值标记时会修改整个hash的过期时间，慎用
     * @param hashKey		key
     * @param field			item
     * @param resultType	存储类型
     * @param queryFunction	sql
     * @return
     * @param <T>
     */
    @Deprecated
    public <T> T getDataFromHash(String hashKey, String field, Class<T> resultType, HashFieldQueryFunction<T> queryFunction) {

        // 1. 调用 Resolver 来获取配置
        final MultiCacheProperties.CacheSettings config = getConfigFromKey(hashKey);
        String localCacheKey = hashKey + ":" + field;

        // 2. 尝试从 L1 获取
        T l1Result = getFromLocalCache(config.getNamespace(), localCacheKey);
        if (l1Result != null) {
            return CacheUtils.isEmpty(l1Result) ? null : l1Result;
        }

        // 3. 尝试从 L2 获取
        Optional<T> l2Result = getFromRedisHash(config.getNamespace(), hashKey, field, config.getRedis().getStorageType(), localCacheKey, resultType);
        if (l2Result.isPresent()) {
            return handleCacheHit(l2Result.get());
        }

        // 4. 从数据库加载
        return getFromDbHash(config.getNamespace(), hashKey, field, config, resultType, queryFunction);
    }

    /**
     * 【方法一：整体存储】缓存预热。直接接收一个准备好的Map，并批量写入到L1和L2缓存中。
     * 使用默认的 L1_L2_DB 缓存策略。
     *
     * @param config      要预热的缓存的配置枚举
     * @param dataToCache 待缓存的数据，Key是Redis Key，Value是要缓存的对象
     * @return 成功预热的缓存条目数量
     * @param <V>         要缓存的对象的类型
     */
    public <V> int preLoadCacheFromMap(MultiCacheProperties.CacheSettings config, Map<String, V> dataToCache) {
        return preLoadCacheFromMap(config, dataToCache, StoragePolicy.L1_L2_DB);
    }

    /**
     * 【方法二：Redis 存储】缓存预热。直接接收Map，只写入到L2(Redis)缓存中。
     */
    public <V> int preLoadCacheToL2FromMap(MultiCacheProperties.CacheSettings config, Map<String, V> dataToCache) {
        return preLoadCacheFromMap(config, dataToCache, StoragePolicy.L2_DB);
    }

    /**
     * 【方法三：本地缓存 存储】缓存预热。直接接收Map，只写入到L1(Caffeine)缓存中。
     */
    public <V> int preLoadCacheToL1FromMap(MultiCacheProperties.CacheSettings config, Map<String, V> dataToCache) {
        return preLoadCacheFromMap(config, dataToCache, StoragePolicy.L1_DB);
    }

    // =================================================================
    // ===================== 私有辅助方法和内部类 ======================
    // =================================================================

    /**
     * 根绝数据类型返回对应的读写策略
     * @param storageType
     * @return
     * @param <T>
     */
    private <T> RedisStorageStrategy<T> getStrategy(StorageType storageType) {
        return (RedisStorageStrategy<T>) strategyMap.get(storageType);
    }

    private <T> FieldBasedStorageStrategy<T> getFieldBasedStrategy(StorageType storageType) {
        RedisStorageStrategy<?> strategy = strategyMap.get(storageType);
        if (!(strategy instanceof FieldBasedStorageStrategy)) {
            throw new UnsupportedOperationException("存储类型 " + storageType + " 不支持按字段操作！");
        }
        return (FieldBasedStorageStrategy<T>) strategy;
    }

    /**
     * 私有核心实现
     *
     * @param key           完整的Redis Key
     * @param typeRef       返回类型
     * @param queryFunction DB查询函数
     * @param policy        缓存策略
     */
    private <T> T getSingleDataInternal(
            String key,
            TypeReference<T> typeRef,
            CacheQueryFunction<T> queryFunction,
            StoragePolicy policy
    ) {

        // 1. 调用 Resolver 来获取配置
        final MultiCacheProperties.CacheSettings config = getConfigFromKey(key);

        // 2. L1 缓存逻辑
        if (policy.isUseL1() && config.getCaffeine().getEnabled()) {
            T l1Result = getFromLocalCache(config.getNamespace(), key);
            if (l1Result != null) {
                return handleCacheHit(l1Result);
            }
        }

        // 3. L2 缓存逻辑
        if (policy.isUseL2()) {

            // 从 config 对象中获取 redis 配置
            Optional<T> l2Result = getFromRedis(key, config.getRedis().getStorageType(), typeRef);
            if (l2Result.isPresent()) {
                T value = l2Result.get();
                if (policy.isPopulateL1FromL2() && config.getCaffeine().getEnabled()) {
                    putInLocalCacheAsync(config.getNamespace(), key, value);
                }
                return handleCacheHit(value);
            }
        }

        // 4. 从 DB 获取
        return getFromDb(key, config, typeRef, queryFunction, policy);
    }

    /**
     * 批量数据读写
     * @param ids id列表
     * @param valueTypeRef
     * @param keyBuilder
     * @param queryFunction
     * @param storagePolicy
     * @return
     * @param <K>
     * @param <V>
     */
    private <K, V> Map<K, V> getMultiDataInternal(Collection<K> ids,
                                                  TypeReference<V> valueTypeRef,
                                                  Function<K, String> keyBuilder,
                                                  CacheQueryMultiFunction<K, V> queryFunction,
                                                  StoragePolicy storagePolicy
    ) {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyMap();
        }

        // 1. 调用 Resolver 来获取配置
        final MultiCacheProperties.CacheSettings config = getConfigFromKey(keyBuilder.apply(ids.iterator().next()));
        final Map<K, V> finalResultMap = new ConcurrentHashMap<>();
        Collection<K> missingIds = ids;

        // 2. 尝试从 L1 批量获取
        if (storagePolicy.isUseL1()) {
            missingIds = getFromLocalCacheMulti(ids, finalResultMap, keyBuilder, config.getNamespace());
            if (missingIds.isEmpty()) {
                return finalResultMap;
            }
        }

        // 3. 尝试从 L2 批量获取
        if (storagePolicy.isUseL2()) {
            missingIds = getFromRedisMulti(missingIds, finalResultMap, keyBuilder, config, valueTypeRef, storagePolicy);
            if (missingIds.isEmpty()) {
                return finalResultMap;
            }
        }

        // 4. 从 DB 批量获取
        Map<K, V> dbResultMap = getFromDbMulti(new ArrayList<>(missingIds), config, keyBuilder, queryFunction, storagePolicy);
        if (dbResultMap != null) {
            finalResultMap.putAll(dbResultMap);
        }
        return finalResultMap;
    }

    /**
     * 根据指定的缓存策略，获取多个Set的并集。
     */
    private <T> Set<T> getUnionDataInternal(String unionCacheKey,
                                            List<String> setKeysInRedis,
                                            TypeReference<Set<T>> typeRef,
                                            Function<List<String>, Map<String, Set<T>>> dbQueryFunction,
                                            StoragePolicy storagePolicy) {

        // 1. 调用 Resolver 来获取配置
        final MultiCacheProperties.CacheSettings config = getConfigFromKey(unionCacheKey);
        RedisStorageStrategy<Set<T>> strategy = getStrategy(StorageType.SET);

        // 2. 查询 L1
        if (storagePolicy.isUseL1()) {
            Set<T> l1Result = getFromLocalCache(config.getNamespace(), unionCacheKey);
            if (l1Result != null) {
                return handleCacheHit(l1Result);
            }
        }

        // 3. 查询 L2
        Set<T> finalResult = new HashSet<>();
        List<String> missingKeysAfterL2 = setKeysInRedis;
        if (storagePolicy.isUseL2()) {
            try {
                RedisStorageStrategy.UnionReadResult<T> l2ReadResult = strategy.readUnion(redisUtils, setKeysInRedis, typeRef);

                if (l2ReadResult != null) {
                    finalResult.addAll(l2ReadResult.getUnionResult()); // 合并L2中已有的并集
                    missingKeysAfterL2 = l2ReadResult.getMissedKeys();  // 获取L2中未命中的keys
                    log.info(LOG_PREFIX + "[L2 UNION] Hit size: {}, Missed keys count: {}",
                            l2ReadResult.getUnionResult().size(), l2ReadResult.getMissedKeys().size());
                } else {

                    // 如果 readUnion 返回 null (代表发生严重错误)，将所有 key 视为未命中
                    missingKeysAfterL2 = setKeysInRedis;
                }

            } catch (Exception e) {
                log.error(LOG_PREFIX + "[L2 UNION FAILED] Keys: {}. Error: {}", setKeysInRedis, e.getMessage(), e);

                // Redis异常，降级到数据库
                missingKeysAfterL2 = setKeysInRedis;
            }
        }

        // 如果L2处理后已经没有未命中的key了，则流程结束
        if (missingKeysAfterL2.isEmpty()) {
            if (storagePolicy.isUseL1()) {
                putInLocalCacheAsync(config.getNamespace(), unionCacheKey, finalResult);
            }
            return finalResult;
        }

        // 对 L2 中未命中的 key 查询数据库
        log.info(LOG_PREFIX + "[DB-QUERY FOR UNION] L2 miss for keys: {}", missingKeysAfterL2);
        Map<String, Set<T>> dbResultMap = dbQueryFunction.apply(missingKeysAfterL2);

        // 合并 DB 的结果
        if (dbResultMap != null) {
            dbResultMap.values().forEach(finalResult::addAll);
        }

        // 3. 回填L2的多个源Set
        if (storagePolicy.isUseL2()) {
            RBatch batch = redisUtils.getRedissonClient().createBatch();

            // 遍历DB查询结果，回填每一个Set
            if (dbResultMap != null && !dbResultMap.isEmpty()) {
                strategy.writeMulti(batch, dbResultMap, config.getRedis().getTtl());
            }

            // 找出那些在DB中没有数据，但参与了查询的Key，为它们写入空标记
            List<String> keysToMarkEmpty = missingKeysAfterL2.stream()
                    .filter(key -> dbResultMap == null || !dbResultMap.containsKey(key))
                    .collect(Collectors.toList());
            if (!keysToMarkEmpty.isEmpty()) {
                strategy.writeMultiEmpty(batch, keysToMarkEmpty, config.getRedis().getEmptyTtl());
            }
            batch.execute();
            log.info(LOG_PREFIX + "[L2 UNION POPULATE] 回填 {} 个Set成功 ({} 个空标记)",
                    (dbResultMap != null ? dbResultMap.size() : 0),
                    keysToMarkEmpty.size());
        }

        // 4. 回填L1的最终完整并集结果
        if (storagePolicy.isUseL1()) {
            Object valueToCacheForL1 = finalResult.isEmpty() ? CacheUtils.createEmptyData(typeRef) : finalResult;
            putInLocalCacheAsync(config.getNamespace(), unionCacheKey, valueToCacheForL1);
        }
        return finalResult;
    }

    /**
     * 从 key 中获取配置文件
     * @param key key
     * @return  配置文件
     */
    private MultiCacheProperties.CacheSettings getConfigFromKey(String key) {
        return configResolver.resolveConfigFromKey(key)
                .orElseThrow(() ->
                        new IllegalArgumentException(
                                String.format("无法从Key '%s' 中解析出任何已定义的缓存配置。请检查 application.yml 中的 'j-multi-cache.configs' " +
                                        "以及您的Key命名是否符合前缀约定。", key)
                        )
                );
    }

    /**
     * 从本地缓存 L1 获取数据
     * @param namespace	命名空间
     * @param key		键
     * @return
     * @param <T>
     */
    private <T> T getFromLocalCache(String namespace, String key) {
        key = CacheUtils.checkL1Key(key);
        try {
            Cache<String, Object> caffeineCache = (Cache<String, Object>) caffeineCacheManager.getCache(namespace).getNativeCache();
            if (caffeineCache == null) {
                log.warn(LOG_PREFIX + "[L1 WARN] Caffeine cache '{}' not found.", namespace);
                return null;
            }
            Object result = caffeineCache.getIfPresent(key);
            if (result != null) {
                log.info(LOG_PREFIX + "[L1 HIT] Key: {}", key);
                return (T) result;
            } else {
                log.info(LOG_PREFIX + "[L1 MISS] Key: {}", key);
                return null;
            }
        } catch (Exception e) {
            log.warn(LOG_PREFIX + "[L1_GET ERROR] Namespace: {}, Key: {}", namespace, key, e);
            return null;
        }
    }

    /**
     * 从本地缓存 L1 批量获取数据。
     *
     * @param ids           需要查询的ID列表
     * @param resultMap     用于存放命中结果的Map
     * @param keyBuilder    Key构造函数
     * @param namespace     Caffeine的命名空间
     * @return 未在L1中命中的ID列表
     */
    private <K, V> Collection<K> getFromLocalCacheMulti(Collection<K> ids, Map<K, V> resultMap,
                                                        Function<K, String> keyBuilder, String namespace) {
        List<K> missingFromL1 = new ArrayList<>();
        Cache<String, Object> caffeineCache = (Cache<String, Object>) caffeineCacheManager.getCache(namespace).getNativeCache();

        for (K id : ids) {
            String key = keyBuilder.apply(id);
            Object cachedValue = (caffeineCache != null) ? caffeineCache.getIfPresent(key) : null;
            if (cachedValue != null) {
                V entity = handleCacheHit((V) cachedValue);
                if (entity != null) {
                    resultMap.put(id, entity);
                }
            } else {
                missingFromL1.add(id);
            }
        }
        log.info(LOG_PREFIX + "[L1 MULTI] namespace: {} Hit: {}, Miss: {}", namespace, resultMap.size(), missingFromL1.size());
        return missingFromL1;
    }

    private <T> Optional<T> getFromRedis(String key, StorageType storageType, TypeReference<T> typeRef) {
        RedisStorageStrategy<T> strategy = getStrategy(storageType);
        T result = strategy.read(redisUtils, key, typeRef);
        if (result != null) {
            log.info(LOG_PREFIX + "[L2 HIT] Key: {}", key);

            // 不在这里回填L1
            return Optional.ofNullable(result);
        }
        log.info(LOG_PREFIX + "[L2 MISS] Key: {}", key);
        return Optional.empty();
    }

    /**
     * 从 redis 获取 Hash 数据, 并回种L1
     * @param namespace     命名空间
     * @param hashKey
     * @param field
     * @param storageType
     * @param localCacheKey
     * @param resultType
     * @return
     * @param <T>
     */
    private <T> Optional<T> getFromRedisHash(String namespace,
                                             String hashKey,
                                             String field,
                                             StorageType storageType,
                                             String localCacheKey,
                                             Class<T> resultType) {
        FieldBasedStorageStrategy<T> strategy = getFieldBasedStrategy(storageType);
        T result = strategy.readField(redisUtils, hashKey, field, resultType);
        if (result != null) {
            log.info(LOG_PREFIX + "[L2-HASH HIT] Key: {}, Field: {}", hashKey, field);
            putInLocalCacheAsync(namespace, localCacheKey, result);
            return Optional.ofNullable(result);
        }

        log.info(LOG_PREFIX + "[L2-HASH MISS] Key: {}, Field: {}", hashKey, field);
        return Optional.empty();
    }

    /**
     * 从 Redis L2 批量获取数据
     *
     * @param missingFromL1 L1未命中的ID列表
     * @param resultMap     用于存放命中结果的Map
     * @param keyBuilder    Key构造函数
     * @param config        缓存配置
     * @param valueTypeRef	类型
     * @return 未在L2中命中的ID列表
     */
    private <K, V> Collection<K> getFromRedisMulti(Collection<K> missingFromL1,
                                                   Map<K, V> resultMap,
                                                   Function<K, String> keyBuilder,
                                                   MultiCacheProperties.CacheSettings config,
                                                   TypeReference<V> valueTypeRef,
                                                   StoragePolicy storagePolicy
    ) {
        if (missingFromL1.isEmpty()) {
            return Collections.emptyList();
        }

        RedisStorageStrategy<?> strategy = getStrategy(config.getRedis().getStorageType());
        Map<String, K> keyToIdMap = missingFromL1.stream()
                .collect(Collectors.toMap(keyBuilder, Function.identity(), (a, b) -> a));
        List<String> keysToRead = new ArrayList<>(keyToIdMap.keySet());
        RBatch batch = redisUtils.getRedissonClient().createBatch();

        // 从策略获取包含了“转换后”数据的Future Map
        Map<String, CompletableFuture<Optional<V>>> futureMap =
                ((RedisStorageStrategy<V>) strategy).readMulti(batch, keysToRead, valueTypeRef);
        batch.execute();

        List<K> missingFromL2 = new ArrayList<>();

        // 遍历最终的Future，获取结果
        for (String key : keysToRead) {
            K id = keyToIdMap.get(key);
            CompletableFuture<Optional<V>> future = futureMap.get(key);
            try {

                // .join() 获取的就是已经由策略转换好的、最终类型为V的实体
                Optional<V> optionalEntity = future.join();
                if (optionalEntity == null) {

                    // L2 缓存未命中
                    missingFromL2.add(id);
                } else {

                    // L2 缓存命中
                    // 只要 optionalEntity 不为 null，就说明 Redis 中有记录
                    if (optionalEntity.isPresent()) {

                        // 命中，且有真实数据
                        V entity = optionalEntity.get();
                        resultMap.put(id, entity);

                        // 根据策略决定是否回填L1
                        if (storagePolicy.isPopulateL1FromL2()) {
                            putInLocalCacheAsync(config.getNamespace(), key, entity);
                        }
                    }

                    // 命中，但是空标记。不将其放入 resultMap，也不写入 L1 本地缓存。
                }
            } catch (Exception e) {
                log.error(LOG_PREFIX + "[L2 MULTI] FUTURE GET FAILED Key: {}. Error: {}", key, e.getMessage());
                missingFromL2.add(id);
            }
        }
        log.info(LOG_PREFIX + "[L2 MULTI] Hit: {}, Miss: {}", missingFromL1.size() - missingFromL2.size(), missingFromL2.size());
        return missingFromL2;
    }

    /**
     * 从 DB 中获取数据，并回种L2、L1缓存
     * @param key           redis key
     * @param config        缓存配置
     * @param typeRef       类型
     * @param queryFunction 查询函数
     * @param storagePolicy 缓存存储策略
     * @return
     * @param <T>
     */
    private <T> T getFromDb(String key,
                            MultiCacheProperties.CacheSettings config,
                            TypeReference<T> typeRef,
                            CacheQueryFunction<T> queryFunction,
                            StoragePolicy storagePolicy) {
        String lockKey = "lock:multicache:" + key;

        if (redisUtils.tryLock(lockKey, 5, 10, TimeUnit.SECONDS)) {
            try {
                log.info(LOG_PREFIX + "[LEADER] 获取锁成功, 查询数据库 key: {}", key);
                T result = queryFunction.query();
                T valueToCache;
                Duration ttlToUse;

                if (CacheUtils.isResultEmptyFromDb(result)) {
                    valueToCache = CacheUtils.createEmptyData(typeRef);
                    ttlToUse = config.getRedis().getEmptyTtl();
                } else {
                    valueToCache = result;
                    ttlToUse = config.getRedis().getTtl();
                }

                if (storagePolicy.isUseL2()) {
                    RedisStorageStrategy<T> strategy = getStrategy(config.getRedis().getStorageType());
                    strategy.write(redisUtils, key, valueToCache, ttlToUse);
                    log.info(LOG_PREFIX + "[L2 POPULATE] 回种缓存成功 key: {}, value: {}", key);
                }
                if (storagePolicy.isUseL1()) {
                    putInLocalCacheAsync(config.getNamespace(), key, valueToCache);
                }
                return result;
            } finally {
                redisUtils.unlock(lockKey);
            }
        } else {
            log.warn(LOG_PREFIX + "[FOLLOWER] 获取锁失败, 等待后重新尝试... Key: {}", key);
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return getFromRedis(key, config.getRedis().getStorageType(), typeRef)
                    .orElse(null);
        }
    }

    /**
     * 从 DB 获取hash数据
     * @param namespace      命名空间
     * @param hashKey        hash key
     * @param field          hash field
     * @param config         配置
     * @param resultType     结果类型
     * @param queryFunction  查询函数
     * @return
     * @param <T>
     */
    private <T> T getFromDbHash(String namespace,
                                String hashKey,
                                String field,
                                MultiCacheProperties.CacheSettings config,
                                Class<T> resultType,
                                HashFieldQueryFunction<T> queryFunction
    ) {
        String lockKey = "lock:multicache:" + hashKey + ":" + field;
        String localCacheKey = hashKey + ":" + field;

        if (redisUtils.tryLock(lockKey, 5, 10, TimeUnit.SECONDS)) {
            try {
                log.info(LOG_PREFIX + "[LEADER-HASH] 获取锁成功, 查询数据库 Key: {}, Field: {}", hashKey, field);
                T result = queryFunction.query(field);
                Duration ttlToUse;
                T valueToCache;

                if (CacheUtils.isResultEmptyFromDb(result)) {
                    ttlToUse = config.getRedis().getEmptyTtl();
                    valueToCache = (T) CacheUtils.createEmptyData(new TypeReference<Map<?, ?>>() {});
                } else {
                    ttlToUse = config.getRedis().getTtl();
                    valueToCache = result;
                }

                // 对于Hash，field的值可以是任何类型，所以可以直接写入
                FieldBasedStorageStrategy<T> strategy = getFieldBasedStrategy(config.getRedis().getStorageType());
                strategy.writeField(redisUtils, hashKey, field, valueToCache, ttlToUse);
                log.info(LOG_PREFIX + "[L2-HASH POPULATE] 回种缓存成功 key: {}, value: {}", hashKey, valueToCache);
                putInLocalCacheAsync(namespace, localCacheKey, valueToCache);

                log.info(LOG_PREFIX + "[POPULATE-HASH] DB result 回种. Key: {}, Field: {}", hashKey, field);
                return result;
            } finally {
                redisUtils.unlock(lockKey);
            }
        } else {
            log.warn(LOG_PREFIX + "[FOLLOWER-HASH] 获取锁失败, 等待后重新尝试... Key: {}, Field: {}", hashKey, field);
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return getFromRedisHash(namespace, hashKey, field, config.getRedis().getStorageType(), localCacheKey, resultType)
                    .orElse(null);
        }
    }

     /**
     * 从 DB 批量获取数据
     * 当L1和L2缓存均未命中时，从数据库批量获取数据，并根据策略回填缓存。
     * 这个方法实现了缓存穿透保护（分布式锁）和空值缓存。
     *
     * @param missingIds      在L1和L2缓存中都未找到的ID/复合键列表。
     * @param config          从 application.yml 解析出的、已合并的缓存配置对象。
     * @param keyBuilder      一个函数，用于根据单个ID/复合键构建Redis Key。
     * @param queryFunction   一个函数，用于根据 `missingIds` 从数据库批量查询数据。
     * @param storagePolicy   当前的缓存策略 (e.g., L1_L2_DB, L2_DB)，用于决定回填哪些缓存。
     * @return                一个Map，Key是ID/复合键，Value是从数据库查询到的对应实体。如果DB中也找不到，则不包含在返回的Map中。
     * @param <K>             ID/复合键的类型。
     * @param <V>             实体的类型。
     **/
    private <K, V> Map<K, V> getFromDbMulti(List<K> missingIds,
                                            MultiCacheProperties.CacheSettings config,
                                            Function<K, String> keyBuilder,
                                            CacheQueryMultiFunction<K, V> queryFunction,
                                            StoragePolicy storagePolicy
    ) {
        if (missingIds.isEmpty()) {
            return Collections.emptyMap();
        }
        String lockKey = "lock:multicache:multi:" + config.getNamespace() + ":" + CacheUtils.getMd5Key(missingIds);

        if (redisUtils.tryLock(lockKey, 10, 20, TimeUnit.SECONDS)) {
            log.info(LOG_PREFIX + "[LEADER-Multi] 获取锁成功, 查询数据库 namespace: {}", config.getNamespace());
            try {
                RedisStorageStrategy<V> strategy = getStrategy(config.getRedis().getStorageType());
                Map<K, V> dbResultMap = queryFunction.query(missingIds);
                Set<K> foundIds = (dbResultMap != null) ? dbResultMap.keySet() : Collections.emptySet();
                List<K> trulyMissingIds = missingIds.stream().filter(id -> !foundIds.contains(id)).collect(Collectors.toList());

                // 使用 Pipeline (RBatch) 来批量写入并设置不同TTL
                RBatch batch = redisUtils.getRedissonClient().createBatch();

                // 根据策略回填L2
                if (storagePolicy.isUseL2()) {

                    // 1. 回填真实数据 (使用长TTL)
                    if (dbResultMap != null && !dbResultMap.isEmpty()) {
                        Map<String, V> dataToCache = dbResultMap.entrySet().stream()
                                .collect(Collectors.toMap(entry -> keyBuilder.apply(entry.getKey()), Map.Entry::getValue));
                        strategy.writeMulti(batch, dataToCache, config.getRedis().getTtl());
                    }

                    // 2. 回填空值占位符 (使用短TTL)
                    if (!trulyMissingIds.isEmpty()) {
                        List<String> keysToMarkEmpty = trulyMissingIds.stream().map(keyBuilder).collect(Collectors.toList());
                        strategy.writeMultiEmpty(batch, keysToMarkEmpty, config.getRedis().getEmptyTtl());
                    }

                    // 一次性执行所有写入命令
                    if (!trulyMissingIds.isEmpty() || (dbResultMap != null && !dbResultMap.isEmpty())) {
                        batch.execute();
                        log.info(LOG_PREFIX + "[L2-Multi POPULATE] 回种缓存成功 ({} real, {} empty). Namespace: {}",
                                (dbResultMap != null ? dbResultMap.size() : 0), trulyMissingIds.size(), config.getNamespace());
                    }
                }

                // 根据策略回填L1
                if (storagePolicy.isUseL1()) {

                    // 回填 L1 (只回填真实数据)
                    if (dbResultMap != null && !dbResultMap.isEmpty()) {
                        Map<String, Object> dataToCacheL1 = new HashMap<>();
                        dbResultMap.forEach((id, entity) -> dataToCacheL1.put(keyBuilder.apply(id), entity));
                        putInLocalCacheMultiAsync(config.getNamespace(), dataToCacheL1);
                    }
                }

                return dbResultMap != null ? dbResultMap : Collections.emptyMap();
            } finally {
                redisUtils.unlock(lockKey);
            }
        } else {
            log.warn(LOG_PREFIX + "[FOLLOWER-HASH] 获取锁失败, 等待后重新尝试... namespace: {}", config.getNamespace());
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Collections.emptyMap();
        }
    }

    private <V> int preLoadCacheFromMap(MultiCacheProperties.CacheSettings config,
                                        Map<String, V> dataToCache,
                                        StoragePolicy storagePolicy
    ) {
        if (dataToCache == null || dataToCache.isEmpty()) {
            log.warn(LOG_PREFIX + "[WARM-UP-MAP] 传入的预热数据为空，预热结束。Namespace: {}", config.getNamespace());
            return 0;
        }

        log.info(LOG_PREFIX + "[WARM-UP-MAP] 开始为 '{}' 执行缓存预热，策略: {}，数量: {}",
                config.getNamespace(), storagePolicy, dataToCache.size());
        StopWatch stopWatch = new StopWatch("CacheWarmUpFromMap-" + config.getNamespace());

        try {

            // 1. 根据策略回填 L2 Redis
            if (storagePolicy.isUseL2()) {
                stopWatch.start("Populate L2 Cache");
                RedisStorageStrategy<V> strategy = getStrategy(config.getRedis().getStorageType());
                RBatch batch = redisUtils.getRedissonClient().createBatch();
                strategy.writeMulti(batch, (Map<String, V>) dataToCache, config.getRedis().getTtl());
                batch.execute();
                stopWatch.stop();
            }

            // 2. 根据策略回填 L1 Caffeine
            if (storagePolicy.isUseL1()) {
                stopWatch.start("Populate L1 Cache");
                putInLocalCacheMultiAsync(config.getNamespace(), new HashMap<>(dataToCache));
                stopWatch.stop();
            }

            log.info(LOG_PREFIX + "[WARM-UP-MAP] 缓存预热成功。Namespace: {}, 数量: {}, 耗时报告:\n{}",
                    config.getNamespace(), dataToCache.size(), stopWatch.prettyPrint());

            return dataToCache.size();

        } catch (Exception e) {
            log.error(LOG_PREFIX + "[WARM-UP-MAP] 缓存预热失败。Namespace: {}", config.getNamespace(), e);
            return -1; // 返回-1表示失败
        }
    }

    /**
     * L1 缓存回种
     * @param namespace 命名空间
     * @param key       键
     * @param value     值
     */
    private void putInLocalCacheAsync(String namespace, String key, Object value) {
        key = CacheUtils.checkL1Key(key);

        // 如果是空数据标记不回种L1
        if (CacheUtils.isSpecialEmptyData(value)) {
            log.info(LOG_PREFIX + "[L1 EMPTY VALUE] 无需回种 Key: {}", key);
            return;
        }
        try {
            String finalKey = key;
            CompletableFuture.runAsync(() -> {
                Cache<String, Object> caffeineCache = (Cache<String, Object>) caffeineCacheManager.getCache(namespace).getNativeCache();
                if (caffeineCache != null) {
                    caffeineCache.put(finalKey, value);
                    log.info(LOG_PREFIX + "[L1 POPULATE] Key: {}", finalKey);
                }
            }, asyncExecutor);
        } catch (Exception e) {
            log.warn(LOG_PREFIX + "[L1_GET ERROR] Namespace: {}, Key: {}", namespace, key, e);
        }
    }

    /**
     * L1 缓存批量回种
     * @param namespace 命名空间
     * @param dataToCache 数据
     */
    private void putInLocalCacheMultiAsync(String namespace, Map<String, Object> dataToCache) {
        if (dataToCache == null || dataToCache.isEmpty()) return;
        CompletableFuture.runAsync(() -> {
            Cache<String, Object> caffeineCache = (Cache<String, Object>) caffeineCacheManager.getCache(namespace).getNativeCache();
            if (caffeineCache != null) {
                caffeineCache.putAll(dataToCache);
                log.info(LOG_PREFIX + "[L1-MULTI POPULATE] 回种数据 {} 个items.", dataToCache.size());
            }
        }, asyncExecutor);
    }

    /**
     * 缓存命中处理
     * @param result
     * @return
     * @param <T>
     */
    private <T> T handleCacheHit(T result) {
        return CacheUtils.isSpecialEmptyData(result) ? CacheUtils.specialEmptyData2EmptyData(result) : result;
    }

    /**
     * 缓存命中处理 反序列化
     * @param jsonValue
     * @param valueClass
     * @return
     * @param <V>
     */
    private <V> V handleJsonCacheHit(String jsonValue, Class<V> valueClass) {

        // 检查是否是空值占位符
        if (CacheUtils.EMPTY_CACHE.equals(jsonValue)) {
            return null;
        }

        // 反序列化为目标实体
        try {
            return objectMapper.readValue(jsonValue, valueClass);
        } catch (Exception e) {
            log.error(LOG_PREFIX + "[JSON DESERIALIZE FAILED] Raw: {}", jsonValue, e);
            return null;
        }
    }
}
