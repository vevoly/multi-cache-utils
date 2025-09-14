package com.vevoly.multicache.utils;

import io.micrometer.common.util.StringUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.*;
import org.redisson.client.codec.StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 缓存工具类
 *
 */
@Slf4j
@Getter
@Component
@AllArgsConstructor
public class RedisUtils implements com.vevoly.multicache.core.RedisUtils {

    private final RedissonClient redissonClient;

    /**
     * 获取分布式锁
     * @param lockKey 锁键
     * @return 锁对象
     */

    @Override
    public RLock getLock(String lockKey) {
        return redissonClient.getLock(lockKey);
    }

    /**
     * 加锁，带超时时间
     * @param lockKey 锁键
     * @param waitTime 最长等待时间（秒）
     * @param leaseTime 锁持有时间（秒）
     * @return 是否加锁成功
     */
    @Override
    public boolean tryLock(String lockKey, long waitTime, long leaseTime, TimeUnit timeUnit) {
        try {
            RLock lock = getLock(lockKey);
            return lock.tryLock(waitTime, leaseTime, timeUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("尝试获取分布式锁异常: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * 解锁
     * @param lockKey 锁键
     */
    @Override
    public void unlock(String lockKey) {
        try {
            if (redissonClient.isShutdown() || redissonClient.isShuttingDown()) {
                log.warn("Redisson 关闭, 跳过解锁 key: {}", lockKey);
                return;
            }

            RLock lock = getLock(lockKey);
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        } catch (Exception e) {
            log.warn("释放分布式锁异常: {}", e.getMessage(), e);
        }
    }

    /**
     * 發佈消息
     * @param channel
     * @param message
     */
    public void publish(String channel, Object message) {
        redissonClient.getTopic(channel).publish(message);
    }

    /**
     * 指定缓存失效时间
     * @param key
     * @param time
     * @return
     */
    public boolean expire(String key, long time) {

        Optional.ofNullable(redissonClient)
                .filter(template -> time > 0)
                .ifPresent(client -> client.getKeys().expire(key, time, TimeUnit.SECONDS));
        return true;
    }

    /**
     * 查找匹配key
     *
     * @param pattern key
     * @return /
     */
    public List<String> scan(String pattern) {

        return Optional.of(redissonClient).map(client -> {
            List<String> patternList = new ArrayList();
            client.getKeys().getKeysByPattern(pattern).forEach(item->patternList.add(item));
            return patternList;
        }).orElse(Collections.emptyList());
    }


    public boolean hasKey(String key) {
        return redissonClient.getBucket(key).isExists();
    }

    public long hasKeys(String... keys) {
        if (keys == null || keys.length == 0) {
            return 0L;
        }
        RKeys rkeys = redissonClient.getKeys();
        return rkeys.countExists(keys);
    }

    public long hasKeys(List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return 0L;
        }
        return hasKeys(keys.toArray(new String[0]));
    }

    /**
     * 获取指定 Key 在 Redis 中的真实数据类型。
     *
     * @param key 键
     * @return RType 枚举。如果 key 不存在，则返回 null。
     */
    public RType getType(String key) {
        return redissonClient.getKeys().getType(key);
    }

    /**
     * 删除缓
     * @param keys 可以传一个值 或多个
     */
    public void del(String... keys) {
        redissonClient.getKeys().delete(keys);
    }

    // ============================String=============================

    /**
     * 普通缓存获取
     * @param key 键
     * @return 值
     */
    @Override
    public <T> T get(String key) {
        try {
            RBucket<T> bucket = redissonClient.getBucket(key);
            return bucket.get();
        } catch (Exception e) {
            log.error("获取redis key:{} 失败", key, e);
            return null;
        }
    }

    /**
     *
     * 批量获取
     * @param keys
     * @return
     */
    public <T> List<T> batchGet(List<String> keys) {
        RBatch rBatch = redissonClient.createBatch(BatchOptions.defaults());
        List<T> rs = new ArrayList<>();
        List<RFuture> futureList = List.of();
        keys.stream().forEach(key->{
            futureList.add(rBatch.getBucket(key).getAsync());
        });
        rBatch.executeAsync();
        futureList.stream().forEach(future->{
            future.whenComplete((v,e)->{
                rs.add((T) v);
            });
        });

        return rs.stream().filter(v->null!=v).collect(Collectors.toList());
    }

    /**
     * 普通缓存放入
     * @param key 键
     * @param value 值
     * @return true成功 false失败
     */
    @Override
    public boolean set(String key, Object value, Duration ttl) {
        redissonClient.getBucket(key).set(value, ttl);
        return true;
    }

    /**
     * 获取list
     *
     * @param key
     * @return
     * @param <R>
     */
    @Override
    public <R> List<R> getList(String key) {
        return redissonClient.getList(key);
    }

    /**
     * 普通缓存放入
     *
     * @param key   键
     * @param value 值
     * @param ttl   过期时间
     */
    @Override
    public <T> void setList(String key, List<T> value, Duration ttl) {
        Optional.ofNullable(redissonClient).ifPresent(client -> {
            RList<T> list = client.getList(key);
            list.clear();
            list.addAll(value);
            list.expire(ttl);
        });
    }

    /**
     * 按key list獲取結果集
     * @param keyList
     * @return
     * @param <R>
     */
    public <R> List<R> getList(List<String> keyList) {
        List<R> result = new ArrayList<>();
        for (String key : keyList) {
            RType type = redissonClient.getKeys().getType(key);
            if (!RType.LIST.equals(type)) {
                log.warn("Redis key={} is not a list, but {}", key, type);
                continue; // 跳过
            }
            List<R> list = getList(key);
            if (list != null && !list.isEmpty()) {
                result.addAll(list);
            }
        }
        return result;
    }

    /**
     * 批量获取多个 key 的值。
     * 配合MultiCache工具类
     * @param keys 要查询的 key 的集合
     * @return 一个列表，包含所有 key 对应的值。顺序与输入 keys 一致，不存在的 key 对应位置为 null。
     */
    public List<String> mget(Collection<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return Collections.emptyList();
        }
        RBuckets buckets = redissonClient.getBuckets();
        Map<String, String> resultAsMap = buckets.get(keys.toArray(new String[0]));
        return keys.stream().map(resultAsMap::get).collect(Collectors.toList());
    }

    /**
     * 批量设置多个 key-value 对，并为它们统一设置过期时间。
     * 使用 Redisson 的 RBatch (管道) 技术，将多个命令一次性发送到 Redis 服务器。
     *
     * @param data 要设置的 key-value Map
     * @param ttl  统一的过期时间
     */
    public void mset(Map<String, String> data, Duration ttl) {
        if (CollectionUtils.isEmpty(data)) {
            return;
        }
        RBatch batch = redissonClient.createBatch(BatchOptions.defaults());
        data.forEach((key, value) -> {

            // 在 Batch 模式下，所有操作都是异步的，返回 RFuture
            batch.getBucket(key).setAsync(value, ttl);
        });

        // 同步执行批量操作
        batch.execute();
    }

    // ================================Map=================================

    /**
     * HashGet
     * @param key 键 不能为null
     * @param hashKey 项 不能为null
     * @return 值
     */
    @Override
    public <HK, HV> HV hget(String key, HK hashKey) {
        return (HV) redissonClient.getMap(key).get(hashKey);
    }

    /**
     * 获取hashKey对应的所有键值
     * @param key 键
     * @return 对应的多个键值
     */
    @Override
    public <HK, HV> Map<HK, HV> hmget(String key) {
        return redissonClient.getMap(key);
    }

    @Override
    public boolean hmset(String key, Map<String, Object> map, Duration ttl) {

        Optional.ofNullable(redissonClient).map(client -> {
            RMap rMap = client.getMap(key);
            rMap.putAll(map);
            if (ttl.toMillis() > 0) {
                rMap.expire(ttl);
            }
            return true;
        });
        return true;
    }

    /**
     * 获取哈希表中的所有字段和值。
     *
     * @param mapName 哈希表名
     * @return 包含所有字段和值的 Map，如果哈希表为空则返回空的 Map。
     */
    public Map<String, Map<String, String>> getHashFieldValues(String mapName) {
        // 获取指定的 Redisson Map
        RMap<String, Map<String, String>> map = redissonClient.getMap(mapName);

        // 返回所有键值对
        return map == null ? Collections.emptyMap() : new HashMap<>(map);
    }


    /**
     * 将整个 Map 存入哈希表中。过期时间默认一天
     *
     * @param mapName 哈希表名
     * @param dataMap 要存入的 Map 数据
     */
    public void setHashFieldValues(String mapName, Map<String, Map<String, String>> dataMap,long expire) {
        if (dataMap == null || dataMap.isEmpty()) {
            throw new IllegalArgumentException("数据 Map 不能为空");
        }
        // 获取指定的 Redisson Map
        RMap<String, Map<String, String>> map = redissonClient.getMap(mapName);

        // 批量存入数据
        map.putAll(dataMap);
        map.expire(expire, TimeUnit.SECONDS);
    }

    @Override
    public boolean hset(String key, String item, Object value, long time) {

        Optional.ofNullable(redissonClient).map(client -> {
            RMap rMap = client.getMap(key);
            rMap.put(item, value);
            if (time > 0) {
                rMap.expire(Duration.ofSeconds(time));
            }
            return true;
        });
        return true;
    }

    /**
     * 删除hash表中的值，没有namespace
     *
     * @param key  键 不能为null
     * @param item 项 可以使多个 不能为null
     */
    public void hRemoveItem(String key, Object... item) {
        for (Object i : item){
            redissonClient.getMap(key).remove(i.toString());
        }
    }

    // ============================set=============================

    /**
     * 根据key获取Set中的所有值
     * @param key 键
     * @return
     */
    @Override
    public <T> Set<T> sGet(String key) {
        RSet<T> rSet = redissonClient.getSet(key, StringCodec.INSTANCE);
        return rSet.readAll();
    }

    /**
     * 根据value从一个set中查询,是否存在
     * @param key 键
     * @param value 值
     * @return true 存在 false不存在
     */
    public boolean sHasKey(String key, Object value) {
        return redissonClient.getSet(key).contains(value);
    }

    /**
     * 将set数据放入缓存
     * @param key 键
     * @param time 时间(秒)
     * @param values 值 可以是多个
     * @return 成功个数
     */
    @Override
    public long sSet(String key, long time, Object... values) {

        RSet<Object> rSet = redissonClient.getSet(key, StringCodec.INSTANCE);

        // 确保传入的 values 都是字符串
        List<String> stringValues = Arrays.stream(values)
                .map(String::valueOf)
                .collect(Collectors.toList());

        rSet.clear();
        long count = rSet.addAllCounted(stringValues);
        if (time > 0) {
            expire(key, time);
        }
        return count;
    }

    /**
     * 计算并集，使用原生SUNION性能更优。
     *
     * @param keys 要计算并集的 Set 的 Key 列表
     * @return 返回一个包含所有并集结果的 Java Set<String>。
     */
    public Set<String> sUnion(String... keys) {
        if (keys == null || keys.length == 0) {
            return Collections.emptySet();
        }
        return sUnion(List.of(keys));
    }

    /**
     * 重载版本，接收 List<String>
     */
    @Override
    public Set<String> sUnion(List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptySet();
        }

        // 1. 获取 RScript 对象
        RScript script = redissonClient.getScript(StringCodec.INSTANCE);

        // 2.  定义 Lua 脚本
        String luaScript = "return redis.call('SUNION', unpack(KEYS))";

        try {
            List<Object> keysAsObjects = List.copyOf(keys);

            // 3. 执行脚本
            List<String> result = script.eval(RScript.Mode.READ_ONLY, luaScript,
                    RScript.ReturnType.MULTI, keysAsObjects
            );
            return result != null ? Set.copyOf(result) : Collections.emptySet();
        } catch (Exception e) {
            log.error("执行 SUNION Lua 脚本失败. Keys: {}", keys, e);
            return Collections.emptySet();
        }
    }

    /**
     * 使用 Lua 脚本一次性执行 SUNION 并找出不存在的 Keys。
     *
     * @param keys 要操作的 Set 的 Key 列表
     * @return 返回一个 Map，包含两个键：
     *         "unionResult" -> (Set<String>) 存在的keys计算出的并集
     *         "missedKeys"  -> (List<String>)不存在的keys
     */
    @Override
    public Map<String, Object> sunionAndFindMisses(List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            Map<String, Object> result = new HashMap<>();
            result.put("unionResult", Collections.emptySet());
            result.put("missedKeys", Collections.emptyList());
            return result;
        }

        // 1. 定义 Lua 脚本
        String luaScript =
                "local existingKeys = {} \n" +
                        "local missedKeys = {} \n" +
                        "for i, key in ipairs(KEYS) do \n" +
                        "  if redis.call('EXISTS', key) == 1 then \n" +
                        "    table.insert(existingKeys, key) \n" +
                        "  else \n" +
                        "    table.insert(missedKeys, key) \n" +
                        "  end \n" +
                        "end \n" +
                        "local unionResult = {} \n" +
                        "if #existingKeys > 0 then \n" +
                        "  unionResult = redis.call('SUNION', unpack(existingKeys)) \n" +
                        "end \n" +
                        "return {unionResult, missedKeys}";
        RScript script = redissonClient.getScript(StringCodec.INSTANCE);
        try {

            // 2. 执行脚本
            List<Object> keysAsObjects = List.copyOf(keys);

            // eval 返回的是一个包含两个元素的列表: [并集结果列表, 未命中key列表]
            List<Object> result = script.eval(
                    RScript.Mode.READ_ONLY,
                    luaScript,
                    RScript.ReturnType.MULTI,
                    keysAsObjects
            );

            // 3. 解析返回的结果
            Map<String, Object> finalResult = new HashMap<>();
            if (result != null && result.size() == 2) {
                Set<String> unionSet = Set.copyOf((List<String>) result.get(0));
                List<String> missedList = (List<String>) result.get(1);
                finalResult.put("unionResult", unionSet);
                finalResult.put("missedKeys", missedList);
            } else {

                // 如果脚本执行异常或返回格式不对，做降级处理
                finalResult.put("unionResult", Collections.emptySet());
                finalResult.put("missedKeys", keys); // 认为全部未命中
            }
            return finalResult;

        } catch (Exception e) {
            log.error("执行 sunionAndFindMisses Lua 脚本失败. Keys: {}", keys, e);

            // 异常时，认为全部未命中
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("unionResult", Collections.emptySet());
            errorResult.put("missedKeys", keys);
            return errorResult;
        }
    }

    // ===============================list=================================

    /**
     * @param key           限流Key
     * @param maxCount      最大次数
     * @param windowSeconds 时间窗口（秒）
     * @return true=允许 false=拒绝
     */
    public boolean tryAcquire(String key, int maxCount, int windowSeconds) {

        Long result = redissonClient.getScript().eval(
                RScript.Mode.READ_WRITE,
                rateLimitScript,
                RScript.ReturnType.INTEGER,
                Collections.singletonList(key),
                maxCount, // ARGV[1] = limit
                windowSeconds // ARGV[2] = expire
        );
        return result != null && result == 1;
    }

    private final String rateLimitScript = """
			local key = KEYS[1]
			local limit = tonumber(ARGV[1])
			local expire = tonumber(ARGV[2])
			local current = redis.call('incr', key)
			
			if current == 1 then
				redis.call('expire', key, expire)
			end
			if current > limit then
				return 0
			else
				return 1
			end`
			""";

    /** 获取值 */
    public Long getNumValue(String key) {
        RAtomicLong atomicLong = redissonClient.getAtomicLong(key);
        if (!atomicLong.isExists()) {
            return null; // key不存在或已过期
        }
        return atomicLong.get();
    }

    /**
     * 批量设置值并过期
     * @param keyValues key -> value
     * @param ttl 过期时间
     * @param unit 时间单位
     */
    public void batchSetWithExpire(Map<String, Long> keyValues, long ttl, TimeUnit unit) {
        RBatch batch = redissonClient.createBatch();
        keyValues.forEach((key, value) -> {
            RAtomicLongAsync atomicLong = batch.getAtomicLong(key);
            atomicLong.setAsync(value);
            atomicLong.expireAsync(ttl, unit);
        });
        batch.execute();
    }

    public BigDecimal getBigDecimalValue(String key) {
        RAtomicDouble atomicDouble = redissonClient.getAtomicDouble(key);
        if (!atomicDouble.isExists()) {
            return null; // null 表示 key 不存在或已过期
        }
        return BigDecimal.valueOf(atomicDouble.get());
    }

    /**
     * 批量设置 RAtomicDouble 值（带过期时间）
     * @param values   key -> BigDecimal 值
     * @param ttl      有效期
     * @param timeUnit 有效期单位
     */
    public void batchSetBigDecimalValues(Map<String, BigDecimal> values, long ttl, TimeUnit timeUnit) {
        if (values == null || values.isEmpty()) {
            return;
        }

        RBatch batch = redissonClient.createBatch();

        values.forEach((key, value) -> {
            RAtomicDoubleAsync atomicDouble = batch.getAtomicDouble(key);
            // 先设置值
            atomicDouble.setAsync(value.doubleValue());
            // 再设置过期时间
            atomicDouble.expireAsync(ttl, timeUnit);
        });

        batch.execute(); // 一次性提交到 Redis
    }

    public void setBigDecimalValue(String key, BigDecimal value, long ttl, TimeUnit timeUnit) {
        if (key == null || value == null) {
            return;
        }

        RAtomicDouble atomicDouble = redissonClient.getAtomicDouble(key);
        atomicDouble.set(value.doubleValue());
        atomicDouble.expire(ttl, timeUnit);
    }

    /** 归 0 */
    public void reset(String key) {
        redissonClient.getAtomicLong(key).set(0);
    }

    public void resetDouble(String key) {
        redissonClient.getAtomicDouble(key).set(0);
    }

    /** 减 N */
    public long decrementBy(String key, long delta) {
        return redissonClient.getAtomicLong(key).addAndGet(-delta);
    }


    public BigDecimal hincr(String key, BigDecimal by) {
        RAtomicDouble counter = redissonClient.getAtomicDouble(key);
        double newValue = counter.addAndGet(by.doubleValue());
        return BigDecimal.valueOf(newValue);
    }

    public Map<String, BigDecimal> hincrBatch(Map<String, BigDecimal> increments) {
        if (increments == null || increments.isEmpty()) {
            return Collections.emptyMap();
        }

        // 启动批处理
        RBatch batch = redissonClient.createBatch();

        // 存放每个 key 对应的异步执行器
        Map<String, RFuture<Double>> futures = new HashMap<>(increments.size());

        for (Map.Entry<String, BigDecimal> entry : increments.entrySet()) {
            String key = entry.getKey();
            double delta = entry.getValue().doubleValue();

            // 使用 RAtomicDouble 批处理
            RAtomicDoubleAsync atomic = batch.getAtomicDouble(key);
            RFuture<Double> future = atomic.addAndGetAsync(delta);

            futures.put(key, future);
        }

        // 执行批处理
        batch.execute();

        // 收集结果
        Map<String, BigDecimal> results = new HashMap<>(futures.size());
        for (Map.Entry<String, RFuture<Double>> entry : futures.entrySet()) {
            results.put(entry.getKey(), BigDecimal.valueOf(entry.getValue().join()));
        }

        return results;
    }

    /**
     * 接口限流
     * @param key			限流key
     * @param maxCount		窗口期内最大訪問次數
     * @param windowSeconds	窗口期秒數
     * 使用方法： 參考 saveLog(ChannelAccessLogReq channelAccessLog)
     *
     * @return
     */
    @Override
    public boolean isRateLimit(String key, int maxCount, int windowSeconds) {
        if (maxCount <= 0) {
            maxCount = 1;
        }
        if (windowSeconds <= 0) {
            windowSeconds = 1;
        }
        String keyPrefix = "rate_limit:";
        key = key.startsWith(keyPrefix) ? key : keyPrefix + key;

        boolean allowed = tryAcquire(key, maxCount, windowSeconds);
        return !allowed;
    }

    public boolean isRateLimit(String key) {
        return isRateLimit(key, 1, 1);
    }

    public void setNeverExpire(String key) {
        redissonClient.getAtomicDouble(key).clearExpire();
    }
}