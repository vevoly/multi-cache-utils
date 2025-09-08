package com.vevoly.multicache;

import com.vevoly.multicache.config.MultiCacheProperties;
import com.vevoly.multicache.resolver.CacheConfigResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CacheConfigResolverTest {

    private MultiCacheProperties properties;
    private CacheConfigResolver resolver;

    @BeforeEach
    void setUp() {
        properties = new MultiCacheProperties();

        // 设置默认值
        properties.getDefaults().getRedis().setTtl(Duration.ofHours(1));
        properties.getDefaults().getCaffeine().setMaximumSize(500L);

        // 设置特定值
        MultiCacheProperties.CacheSettings userCacheSettings = new MultiCacheProperties.CacheSettings();
        userCacheSettings.getRedis().setTtl(Duration.ofMinutes(30)); // 覆盖 ttl
        properties.getConfigs().put("user_cache", userCacheSettings);

        resolver = new CacheConfigResolver(properties);
    }

    @Test
    void shouldReturnSpecificConfigWhenExists() {
        MultiCacheProperties.CacheSettings settings = resolver.resolve("user_cache");

        // 验证特定值被应用
        assertEquals(Duration.ofMinutes(30), settings.getRedis().getTtl());

        // 验证默认值被继承
        assertEquals(500L, settings.getCaffeine().getMaximumSize());
    }

    @Test
    void shouldReturnDefaultConfigWhenNotExists() {
        MultiCacheProperties.CacheSettings settings = resolver.resolve("unknown_cache");

        // 验证返回的是默认配置
        assertEquals(Duration.ofHours(1), settings.getRedis().getTtl());
        assertEquals(500L, settings.getCaffeine().getMaximumSize());
    }
}
