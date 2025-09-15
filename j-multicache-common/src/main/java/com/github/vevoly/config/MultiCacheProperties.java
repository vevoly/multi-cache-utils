package com.github.vevoly.config;

import com.github.vevoly.enums.StorageType;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Data
@ConfigurationProperties(prefix = "j-multi-cache")
public class MultiCacheProperties {

    /**
     * 是否启用 j-multicache
     */
    private boolean enabled = true;

    /**
     * 全局默认的缓存配置
     */
    private CacheSettings defaults = new CacheSettings();

    /**
     * 为特定的 cacheName 设置独立的配置
     * Key是cacheName (e.g., "users", "user_details")。
     */
    private Map<String, CacheSettings> configs = new HashMap<>();

    @Data
    public static class CacheSettings {
        /** 缓存命名空间 */
        private String namespace;
        /** L2 (Redis) 配置 */
        private RedisSettings redis = new RedisSettings();
        /** L1 (Caffeine) 配置 */
        private CaffeineSettings caffeine = new CaffeineSettings();
    }

    @Data
    public static class RedisSettings {
        /** 正常值的过期时间 */
        private Duration ttl;
        /** 空值标记的过期时间 */
        private Duration emptyTtl;
        /** Redis 存储类型 */
        private StorageType storageType;
    }

    @Data
    public static class CaffeineSettings {
        /** 是否为这个 cacheName 启用 L1 缓存 */
        private Boolean enabled;
        /** 写入后过期时间 */
        private Duration expireAfterWrite;
        /** 最大条目数 */
        private Long maximumSize;
        /** 初始容量 */
        private Integer initialCapacity;
    }
}
