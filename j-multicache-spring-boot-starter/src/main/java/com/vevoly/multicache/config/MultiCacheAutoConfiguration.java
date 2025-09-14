package com.vevoly.multicache.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vevoly.multicache.MultiCacheUtils;
import com.vevoly.multicache.resolver.CacheConfigResolver;
import com.vevoly.multicache.strategy.RedisStorageStrategy;
import com.vevoly.multicache.strategy.impl.*;
import com.vevoly.multicache.utils.RedisUtils;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;
import java.util.concurrent.Executor;

@Configuration
@EnableConfigurationProperties(MultiCacheProperties.class)
@ConditionalOnClass({RedissonClient.class, CaffeineCacheManager.class})
public class MultiCacheAutoConfiguration {

    @Bean("stringStorageStrategy")
    @ConditionalOnMissingBean(name = "stringStorageStrategy")
    public StringStorageStrategy stringStorageStrategy(ObjectMapper objectMapper) {
        return new StringStorageStrategy(objectMapper);
    }

    @Bean("listStorageStrategy")
    @ConditionalOnMissingBean(name = "listStorageStrategy")
    public ListStorageStrategy listStorageStrategy(ObjectMapper objectMapper) {
        return new ListStorageStrategy(objectMapper);
    }

    @Bean("setStorageStrategy")
    @ConditionalOnMissingBean(name = "setStorageStrategy")
    public SetStorageStrategy setStorageStrategy(ObjectMapper objectMapper) {
        return new SetStorageStrategy(objectMapper);
    }

    @Bean("hashStorageStrategy")
    @ConditionalOnMissingBean(name = "hashStorageStrategy")
    public HashStorageStrategy hashStorageStrategy(ObjectMapper objectMapper) {
        return new HashStorageStrategy(objectMapper);
    }

    @Bean("pageStorageStrategy")
    @ConditionalOnMissingBean(name = "pageStorageStrategy")
    public PageStorageStrategy pageStorageStrategy(ObjectMapper objectMapper) {
        return new PageStorageStrategy(objectMapper);
    }

    @Bean("asyncMultiCacheExecutor")
    @ConditionalOnMissingBean(name = "asyncMultiCacheExecutor")
    public Executor asyncMultiCacheExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("MultiCache-Async-");
        executor.initialize();
        return executor;
    }

    @Bean
    @ConditionalOnMissingBean
    public CacheConfigResolver cacheConfigResolver(MultiCacheProperties properties) {
        return new CacheConfigResolver(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public MultiCacheUtils multiCacheUtilsV4(
            RedissonClient redissonClient,
            @Qualifier("caffeineCacheManager") CaffeineCacheManager caffeineCacheManager,
            List<RedisStorageStrategy<?>> strategies,
            ObjectMapper objectMapper,
            @Qualifier("asyncMultiCacheExecutor") Executor asyncExecutor,
            CacheConfigResolver configResolver
    ) {
        RedisUtils internalRedisUtils = new RedisUtils(redissonClient);
        return new MultiCacheUtils(internalRedisUtils, caffeineCacheManager, strategies, objectMapper, asyncExecutor, configResolver);
    }
}
