package com.vevoly.multicache.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.vevoly.multicache.MultiCacheUtils;
import com.vevoly.multicache.enums.CacheName;
import com.vevoly.multicache.utils.CacheUtils;
import com.vevoly.multicache.utils.RedisUtils;
import com.vevoly.multicache.entity.User;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@SpringBootTest
@Testcontainers // 启用 Testcontainers
class MultiCacheUtilsIntegrationTest {

    @Container // 声明一个 Docker 容器
    public static GenericContainer<?> redis = new GenericContainer<>("redis:6-alpine")
            .withExposedPorts(6379);

    @Autowired
    private MultiCacheUtils multiCacheUtils;

    @Autowired
    private RedisUtils redisUtils;

    // 动态地将 Testcontainer 启动的 Redis 地址和端口注入到 Spring Boot 配置中
    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", redis::getFirstMappedPort);
    }

    @Test
    @DisplayName("测试 getSingleData 的 L1 -> L2 -> DB 完整流程")
    void testGetSingleData_FullFlow() {
        String cacheName = CacheName.USER_DETAILS.getName();
        String key = CacheUtils.getCacheKey(cacheName, "1");

        // 1. 第一次调用：应该穿透到DB
        AtomicInteger dbCallCount = new AtomicInteger(0);
        User userFromDb = multiCacheUtils.getSingleData(
                key, new TypeReference<>() {},
                () -> {
                    dbCallCount.incrementAndGet();
                    return new User(1L, "test");
                }
        );

        assertEquals(1, dbCallCount.get()); // 验证DB被调用了1次
        assertEquals("test", userFromDb.getName());

        // 2. 第二次调用：应该命中L1缓存
        User userFromL1 = multiCacheUtils.getSingleData(
                key, new TypeReference<>() {},
                () -> {
                    dbCallCount.incrementAndGet();
                    return null; // 如果DB被调用，会返回null
                }
        );
        assertEquals(1, dbCallCount.get()); // 验证DB没有被再次调用
        assertEquals("test", userFromL1.getName());

        // 3. (可选) 清理L1，再调用，应该命中L2
        // ...
    }
}