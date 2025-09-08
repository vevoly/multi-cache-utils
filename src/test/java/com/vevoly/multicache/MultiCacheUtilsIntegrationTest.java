package com.vevoly.multicache;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.core.type.TypeReference;
import com.vevoly.multicache.MultiCacheUtils;
import com.vevoly.multicache.enums.CacheName;
import com.vevoly.multicache.utils.CacheUtils;
import com.vevoly.multicache.utils.RedisUtils;
import com.vevoly.multicache.entity.User;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@DisplayName("MultiCacheUtils 集成测试")
class MultiCacheUtilsIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private MultiCacheUtils multiCacheUtils;

    @Autowired
    private RedisUtils redisUtils;

    @Autowired
    @Qualifier("caffeineCacheManager")
    private CacheManager caffeineCacheManager;

    private final static long userId = 1L;
    private final static String siteId = "site-001";
    private final static String username = "test";

    private final static User user01 = new User(2L, "site-001", "test2");

    @BeforeEach
    void cleanUpCaches() {
        caffeineCacheManager.getCacheNames().forEach(name -> {
            caffeineCacheManager.getCache(name).clear();
        });
        redisUtils.getRedissonClient().getKeys().flushall();
    }

    @Nested
    @DisplayName("getSingleData 方法测试")
    class GetSingleDataTests {

        @Test
        @DisplayName("策略 L1_L2_DB (String): 应完整地经过 L1 -> L2 -> DB")
        void singleDataStringTest() throws InterruptedException {
            String cacheName = CacheName.USER_DETAILS.getNamespace();
            String key = CacheUtils.getCacheKey(cacheName, String.valueOf(userId));
            AtomicInteger dbCallCount = new AtomicInteger(0);

            // 1. 第一次: 穿透到 DB
            User userFromDb = multiCacheUtils.getSingleData(
                    key,
                    new TypeReference<>() {},
                    () -> {
                        dbCallCount.incrementAndGet();
                        return new User(userId, siteId, username);
                    });
            assertEquals(1, dbCallCount.get());
            Thread.sleep(100);

            // 2. 第二次: 命中 L1
            User userFromL1 = multiCacheUtils.getSingleData(
                    key,
                    new TypeReference<>() {},
                    () -> { dbCallCount.incrementAndGet(); return null; }
            );
            assertEquals(1, dbCallCount.get());
            assertEquals(username, userFromL1.getName());

            // 3. 清理L1, 第三次: 命中 L2
            caffeineCacheManager.getCache(cacheName).clear();
            User userFromL2 = multiCacheUtils.getSingleData(
                    key,
                    new TypeReference<>() {},
                    () -> { dbCallCount.incrementAndGet(); return null; }
            );
            assertEquals(1, dbCallCount.get());
            assertEquals(username, userFromL2.getName());
        }

        @Test
        @DisplayName("策略 L2_DB (List): 应跳过L1，并在L2命中后不回填L1")
        void singleDataListTest() {
            String cacheName = CacheName.USER_LIST.getNamespace();
            String key = CacheUtils.getCacheKey(cacheName, siteId);
            AtomicInteger dbCallCount = new AtomicInteger(0);

            List<User> listFromDb = multiCacheUtils.getSingleDataFromL2(
                    key,
                    new TypeReference<>() {},
                    () -> {dbCallCount.incrementAndGet();
                        return List.of(new User(userId, siteId, username), user01);
            });
            assertEquals(1, dbCallCount.get());
            assertEquals(2, listFromDb.size());

            List<User> listFromL2 = multiCacheUtils.getSingleDataFromL2(
                    key, new TypeReference<>() {},
                    () -> { dbCallCount.incrementAndGet(); return null;
                    }
            );
            assertEquals(1, dbCallCount.get());
            assertEquals(2, listFromL2.size());

            assertEquals(username, listFromL2.get(0).getName());
        }
    }

    // --- 测试 getMultiData ---
    @Nested
    @DisplayName("getMultiData 方法测试")
    class GetMultiDataTests {

        @Test
        @DisplayName("应能批量获取数据，并正确处理部分命中L2的情况")
        void multiDataPartialHit() {
            final String cacheName = CacheName.USER_DETAILS.getNamespace();
            List<Long> userIds = List.of(10L, 11L, 12L);
            AtomicInteger dbCallCount = new AtomicInteger(0);

            Function<Long, String> keyBuilder = id -> CacheUtils.getCacheKey(cacheName, String.valueOf(id));

            // 准备: L2中预缓存11L, L1中预缓存12L
            redisUtils.set(keyBuilder.apply(11L), new User(11L, siteId, "user_from_L2"), Duration.ofMinutes(1));
            caffeineCacheManager.getCache(cacheName).put(keyBuilder.apply(12L), new User(12L, siteId, "user_from_L1"));

            Map<Long, User> resultMap = multiCacheUtils.getMultiData(
                    userIds,
                    new TypeReference<>() {},
                    keyBuilder,
                    missingIds -> {
                        dbCallCount.incrementAndGet();
                        assertThat(missingIds).containsExactlyInAnyOrder(10L);
                        return Map.of(10L, new User(10L, siteId, "user_from_db"));
                    }
            );

            assertEquals(1, dbCallCount.get());
            assertEquals(3, resultMap.size());
            assertEquals("user_from_db", resultMap.get(10L).getName());
            assertEquals("user_from_L2", resultMap.get(11L).getName());
            assertEquals("user_from_L1", resultMap.get(12L).getName());
        }
    }

    // --- 测试 getUnionData ---
    @Nested
    @DisplayName("getUnionData 方法测试 (Set)")
    class GetUnionDataTests {
        private final String unionCacheName = CacheName.BANNER_USER_TYPE.getNamespace();
        private final String userCacheName = CacheName.BANNER_USER_TYPE_USER.getNamespace();
        private final String levelCacheName = CacheName.BANNER_USER_TYPE_LEVEL.getNamespace();

        @Test
        @DisplayName("应能正确计算并集，并回源查询未命中部分")
        void getUnionDataPartialHit() {
            Long userId = 100L;
            Long levelId = 5L;

            String unionCacheKey = CacheUtils.getCacheKey(unionCacheName, String.valueOf(userId), String.valueOf(levelId));
            String userSetKey = CacheUtils.getCacheKey(userCacheName, String.valueOf(userId));
            String levelSetKey = CacheUtils.getCacheKey(levelCacheName, String.valueOf(levelId));

            // 准备: userSetKey在Redis中不存在，levelSetKey存在
            redisUtils.sSet(levelSetKey, 1000L, 1001L, 1002L);
            Set<Long> objects = redisUtils.sGet(levelSetKey);
            AtomicInteger dbCallCount = new AtomicInteger(0);

            Set<Long> result = multiCacheUtils.getUnionData(
                    unionCacheKey,
                    List.of(userSetKey, levelSetKey),
                    new TypeReference<>() {},
                    missingKeys -> {
                        dbCallCount.incrementAndGet();

                        // 验证只为未命中的 userSetKey 查询DB
                        assertThat(missingKeys).containsExactlyInAnyOrder(userSetKey);
                        return Map.of(userSetKey, Set.of(1003L, 1001L)); // DB返回1003, 还有一个与L2重复的1001
                    }
            );

            assertEquals(1, dbCallCount.get());

            // 验证最终结果是 L2 和 DB 的并集
            assertThat(result).containsExactlyInAnyOrder(1001L, 1002L, 1003L);
        }
    }

    // --- 测试 getPageData ---
    @Nested
    @DisplayName("getPageData 方法测试 (Page)")
    class GetPageDataTests {
        private final String cacheName = CacheName.USER_PAGE.getNamespace();

        @Test
        @DisplayName("应能缓存和读取MyBatis-Plus Page对象")
        void getPageDataTest() {
            long currentPage = 1;
            long pageSize = 10;

            String key = CacheUtils.getMd5Key(cacheName, siteId, String.valueOf(userId), String.valueOf(currentPage), String.valueOf(pageSize));
            AtomicInteger dbCallCount = new AtomicInteger(0);

            // 1. 第一次调用，查询DB
            Page<User> pageFromDb = multiCacheUtils.getSingleDataFromL1(
                    key,
                    new TypeReference<>() {},
                    () -> {
                        dbCallCount.incrementAndGet();
                        Page<User> page = new Page<>(1, 10, 25);
                        page.setRecords(List.of(new User(1L, siteId, "user1")));
                        return page;
                    }
            );
            assertEquals(1, dbCallCount.get());
            assertEquals(25, pageFromDb.getTotal());
            assertEquals(1, pageFromDb.getRecords().size());

            // 2. 第二次调用，应命中缓存（这里简化，不区分L1/L2）
            Page<User> pageFromCache = multiCacheUtils.getSingleDataFromL1(
                    key,
                    new TypeReference<>() {},
                    () -> {
                        dbCallCount.incrementAndGet();
                        return null;
                    }
            );
            assertEquals(1, dbCallCount.get());
            assertEquals(25, pageFromCache.getTotal());
            assertEquals("user1", pageFromCache.getRecords().get(0).getName());
        }
    }
}
