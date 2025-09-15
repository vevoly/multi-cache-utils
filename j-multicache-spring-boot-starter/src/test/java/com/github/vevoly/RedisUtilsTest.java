package com.github.vevoly;


import com.github.vevoly.enums.CacheName;
import com.github.vevoly.core.CacheUtils;
import com.github.vevoly.utils.RedisUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Map;
import java.util.Set;

@SpringBootTest(classes = TestApplication.class)
public class RedisUtilsTest extends BaseIntegrationTest {

    @Autowired
    private RedisUtils redisUtils;

    private final String unionCacheName = CacheName.BANNER_USER_TYPE.getNamespace();
    private final String userCacheName = CacheName.BANNER_USER_TYPE_USER.getNamespace();
    private final String levelCacheName = CacheName.BANNER_USER_TYPE_LEVEL.getNamespace();

    @Test
    void unionTest() {

        Long userId = 100L;
        Long levelId = 5L;

        String unionCacheKey = CacheUtils.getCacheKey(unionCacheName, String.valueOf(userId), String.valueOf(levelId));
        String userSetKey = CacheUtils.getCacheKey(userCacheName, String.valueOf(userId));
        String levelSetKey = CacheUtils.getCacheKey(levelCacheName, String.valueOf(levelId));

        redisUtils.sSet(levelSetKey, 1000L, "1001", "1002");
        Set<Long> objects = redisUtils.sGet(levelSetKey);
        System.out.println(objects);

        Map<String, Object> stringObjectMap = redisUtils.sunionAndFindMisses(List.of(userSetKey, levelSetKey));


        System.out.println(stringObjectMap);

    }

}
