package com.vevoly.multicache.resolver;

import com.vevoly.multicache.config.MultiCacheProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

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
        MultiCacheProperties.CacheSettings defaults = properties.getDefaults();

        defaults.setNamespace("defaults");

        // 2. 获取该 cacheName 的特定配置 (可能为 null)
        MultiCacheProperties.CacheSettings specificConfig = properties.getConfigs().get(cacheName);

        // 3. 如果没有特定配置，直接返回默认配置
        if (specificConfig == null) {
            return defaults;
        }

        specificConfig.setNamespace(cacheName);

        // 4. 执行合并逻辑
        return mergeSettings(defaults, specificConfig);
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

        // 1. 使用“最长前缀匹配”算法解析出 cacheName
        Optional<String> bestMatch = configuredCacheNames.stream()
                .filter(cacheName -> key.equals(cacheName) || key.startsWith(cacheName + ":"))
                .max(Comparator.comparingInt(String::length));

        // 2. 如果解析成功，则继续查找并合并配置
        if (bestMatch.isPresent()) {
            String cacheName = bestMatch.get();

            // a. 获取全局默认配置
            MultiCacheProperties.CacheSettings defaults = properties.getDefaults();

            // b. 获取特定配置
            MultiCacheProperties.CacheSettings specificConfig = properties.getConfigs().get(cacheName);

            // c. 为 specificConfig 设置名字，以便合并后保留
            specificConfig.setNamespace(cacheName);

            // d. 执行合并并返回
            return Optional.of(mergeSettings(defaults, specificConfig));

        } else {
            return Optional.empty();
        }
    }

    /**
     * 将特定配置合并到默认配置之上。
     */
    private MultiCacheProperties.CacheSettings mergeSettings(
            MultiCacheProperties.CacheSettings defaults,
            MultiCacheProperties.CacheSettings specific
    ) {
        MultiCacheProperties.CacheSettings finalConfig = new MultiCacheProperties.CacheSettings();
        finalConfig.setNamespace(specific.getNamespace());

        // 1 合并 Redis 配置
        finalConfig.getRedis().setTtl(
                Optional.ofNullable(specific.getRedis().getTtl()).orElse(defaults.getRedis().getTtl())
        );
        finalConfig.getRedis().setEmptyTtl(
                Optional.ofNullable(specific.getRedis().getEmptyTtl()).orElse(defaults.getRedis().getEmptyTtl())
        );
        finalConfig.getRedis().setStorageType(
                Optional.ofNullable(specific.getRedis().getStorageType()).orElse(defaults.getRedis().getStorageType())
        );

        // 2. 合并 Caffeine 配置
        finalConfig.getCaffeine().setEnabled(
                Optional.ofNullable(specific.getCaffeine().getEnabled()).orElse(defaults.getCaffeine().getEnabled())
        );
        finalConfig.getCaffeine().setExpireAfterWrite(
                Optional.ofNullable(specific.getCaffeine().getExpireAfterWrite()).orElse(defaults.getCaffeine().getExpireAfterWrite())
        );
        finalConfig.getCaffeine().setMaximumSize(
                Optional.ofNullable(specific.getCaffeine().getMaximumSize()).orElse(defaults.getCaffeine().getMaximumSize())
        );
        finalConfig.getCaffeine().setInitialCapacity(
                Optional.ofNullable(specific.getCaffeine().getInitialCapacity()).orElse(defaults.getCaffeine().getInitialCapacity())
        );

        return finalConfig;
    }
}