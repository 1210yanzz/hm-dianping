package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis中查询店铺信息
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断店铺缓存是否存在，非空白即存在
        if (StrUtil.isNotBlank(json)) {
            // 3.如果存在，返回店铺信息
            R shop = JSONUtil.toBean(json, type);
            return shop;  //JSONUtil的toBean方法将json字符串转换为Shop对象
        }
        // 判断店铺缓存是否为空值（isNotBlank只有在字符串有值时才判断true，null和""都判断为false）
        if (json != null) {   //前面判断shopJson是null或是""，这里判断shopJson不是null，得到shopJson是空值""
            // 返回错误信息
            return null;
        }
        // 4.如果不存在，查询数据库
        R r = dbFallback.apply(id);
        // 5.判断数据库查询结果是否存在
        if (r == null) {
            // 6.如果不存在，将空值写入redis并返回错误信息
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 7.如果存在，将数据库查询结果返回并写入redis
        this.set(key, r, time, unit);  //JSONUtil的toJsonStr方法将Shop对象转换为json字符串
        return r;  //返回数据库查询结果
    }

    // 新建一个线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    // 通过逻辑过期来解决缓存击穿问题
    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis中查询店铺信息
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断店铺缓存是否存在
        if (StrUtil.isBlank(json)) {
            // 3.如果不存在，返回null
            return null;
        }
        // 4.存在，判断缓存是否过期
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);  // JSONUtil的toBean方法将json字符串转换为RedisData对象
        JSONObject shopData = (JSONObject) redisData.getData();  // getData方法返回的是Object类型，需要强转为JSONObject
        R r = JSONUtil.toBean(shopData, type);  // JSONUtil的toBean方法将json字符串转换为R对象
        LocalDateTime expireTime = redisData.getExpireTime();  // 获取过期时间
        // 5.过期时间在当前时间之后，则未过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 未过期，返回店铺信息
            return r;
        }
        // 过期，实现缓存重建
        // 6.尝试获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 7.判断是否获取成功
        if (isLock) {
            //  如果获取成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 根据id查询数据库
                    R r1 = dbFallback.apply(id);
                    // 7.将查询结果写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unlock(lockKey);
                }
            });
        }
        //  获取失败，则直接返回店铺信息
        return r;
    }

    // 尝试获取锁，获取成功返回true，获取失败返回false
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    // 释放锁
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
