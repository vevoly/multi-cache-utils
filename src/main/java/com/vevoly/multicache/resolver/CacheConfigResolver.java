package com.vevoly.multicache.resolver;

import com.vevoly.multicache.config.MultiCacheProperties;
import com.vevoly.multicache.enums.StorageType;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;

/**
 * 缓存配置解析器
 */
@Component
@RequiredArgsConstructor
public class CacheConfigResolver {

    private final MultiCacheProperties properties;

    private static final MultiCacheProperties.CacheSettings SYSTEM_DEFAULTS;

    static {
        SYSTEM_DEFAULTS = new MultiCacheProperties.CacheSettings();

        // Redis 系统默认值
        SYSTEM_DEFAULTS.getRedis().setTtl(Duration.ofHours(1));
        SYSTEM_DEFAULTS.getRedis().setEmptyTtl(Duration.ofMinutes(5));
        SYSTEM_DEFAULTS.getRedis().setStorageType(StorageType.STRING);

        // Caffeine 系统默认值
        SYSTEM_DEFAULTS.getCaffeine().setEnabled(true);
        SYSTEM_DEFAULTS.getCaffeine().setExpireAfterWrite(Duration.ofMinutes(10));
        SYSTEM_DEFAULTS.getCaffeine().setMaximumSize(1000L);
        SYSTEM_DEFAULTS.getCaffeine().setInitialCapacity(100);
    }

    private Set<String> configuredCacheNames;

    @PostConstruct
    private void initialize() {
        this.configuredCacheNames = properties.getConfigs().keySet();
    }

    /**
     * 从一个完整的 Redis Key 中，使用“最长前缀匹配”算法解析出 cacheName。
     *
     * @param key 完整的 Redis Key
     * @return 解析出的 cacheName，如果找不到则为 null
     */
    public String resolveCacheNameFromKey(String key) {
        if (key == null || key.isEmpty()) {
            return null;
        }

        Optional<String> bestMatch = configuredCacheNames.stream()
                .filter(cacheName -> key.equals(cacheName) || key.startsWith(cacheName + ":"))
                .max(Comparator.comparingInt(String::length));

        return bestMatch.orElse(null);
    }

    /**
     * 根据传入的 cacheName，查找并合并其配置。
     * 特定配置会覆盖全局默认配置。
     *
     * @param cacheName 缓存的逻辑名称
     * @return 一个【已合并】的、完整的 CacheSettings 对象
     */
    public MultiCacheProperties.CacheSettings resolve(String cacheName) {

        // 1. 获取全局默认配置
        MultiCacheProperties.CacheSettings customDefaults = properties.getDefaults();
        MultiCacheProperties.CacheSettings baseDefaults = mergeSettings(SYSTEM_DEFAULTS, customDefaults);
        baseDefaults.setNamespace("defaults");

        // 2. 获取该 cacheName 的特定配置 (可能为 null)
        MultiCacheProperties.CacheSettings specificConfig = properties.getConfigs().get(cacheName);

        // 3. 如果没有特定配置，直接返回默认配置
        if (specificConfig == null) {
            return baseDefaults;
        }

        specificConfig.setNamespace(cacheName);

        // 4. 执行合并逻辑
        return mergeSettings(baseDefaults, specificConfig);
    }

    /**
     * 从一个完整的 Redis Key 中，解析出 cacheName，并立即查找、合并、返回最终的配置对象。
     *
     * @param key 完整的 Redis Key
     * @return 返回一个 Optional，如果成功解析并找到配置，则包含已合并的 CacheSettings；否则为空。
     */
    public Optional<MultiCacheProperties.CacheSettings> resolveConfigFromKey(String key) {
        if (key == null || key.isEmpty()) {
            return Optional.empty();
        }
        String cacheName = resolveCacheNameFromKey(key);
        if (cacheName != null) {
            return Optional.of(resolve(cacheName));
        } else {
            return Optional.of(mergeSettings(SYSTEM_DEFAULTS, new MultiCacheProperties.CacheSettings()));
        }
    }

    /**
     * 合并配置项
     */
    private MultiCacheProperties.CacheSettings mergeSettings(
            MultiCacheProperties.CacheSettings base,
            MultiCacheProperties.CacheSettings specific
    ) {
        MultiCacheProperties.CacheSettings finalConfig = new MultiCacheProperties.CacheSettings();

        // 名称优先取 specific 的
        finalConfig.setNamespace(specific.getNamespace() != null ? specific.getNamespace() : base.getNamespace());

        MultiCacheProperties.RedisSettings specificRedis =
                Optional.ofNullable(specific.getRedis()).orElse(new MultiCacheProperties.RedisSettings());
        MultiCacheProperties.CaffeineSettings specificCaffeine =
                Optional.ofNullable(specific.getCaffeine()).orElse(new MultiCacheProperties.CaffeineSettings());

        // 获取 base 的 redis/caffeine 配置
        MultiCacheProperties.RedisSettings baseRedis = base.getRedis();
        MultiCacheProperties.CaffeineSettings baseCaffeine = base.getCaffeine();

        // 合并
        finalConfig.getRedis().setTtl(
                Optional.ofNullable(specificRedis.getTtl()).orElse(baseRedis.getTtl())
        );
        finalConfig.getRedis().setEmptyTtl(
                Optional.ofNullable(specificRedis.getEmptyTtl()).orElse(baseRedis.getEmptyTtl())
        );
        finalConfig.getRedis().setStorageType(
                Optional.ofNullable(specificRedis.getStorageType()).orElse(baseRedis.getStorageType())
        );

        finalConfig.getCaffeine().setEnabled(
                Optional.ofNullable(specificCaffeine.getEnabled()).orElse(baseCaffeine.getEnabled())
        );
        finalConfig.getCaffeine().setExpireAfterWrite(
                Optional.ofNullable(specificCaffeine.getExpireAfterWrite()).orElse(baseCaffeine.getExpireAfterWrite())
        );
        finalConfig.getCaffeine().setMaximumSize(
                Optional.ofNullable(specificCaffeine.getMaximumSize()).orElse(baseCaffeine.getMaximumSize())
        );
        finalConfig.getCaffeine().setInitialCapacity(
                Optional.ofNullable(specificCaffeine.getInitialCapacity()).orElse(baseCaffeine.getInitialCapacity())
        );
        return finalConfig;
    }
}