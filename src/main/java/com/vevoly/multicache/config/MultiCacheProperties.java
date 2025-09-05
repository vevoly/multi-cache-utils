package com.vevoly.multicache.config;

import com.vevoly.multicache.enums.StorageType;
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
        private Duration ttl = Duration.ofMinutes(10);
        /** 空值标记的过期时间 */
        private Duration emptyTtl = Duration.ofMinutes(1);
        /** Redis 存储类型 */
        private StorageType storageType = StorageType.STRING;
    }

    @Data
    public static class CaffeineSettings {
        /** 是否为这个 cacheName 启用 L1 缓存 */
        private Boolean enabled = true;
        /** 写入后过期时间 */
        private Duration expireAfterWrite = Duration.ofMinutes(5);
        /** 最大条目数 */
        private Long maximumSize = 1000L;
        /** 初始容量 */
        private Integer initialCapacity = 100;
    }
}
