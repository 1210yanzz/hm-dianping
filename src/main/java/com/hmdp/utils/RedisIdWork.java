package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWork {
    /**
     * 起始的时间戳
     */
    private static final long START_TIMESTAMP = 1696118400;

    /**
     * 序列号位数
     */
    private static final int COUNT_BITS = 32;

    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWork(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String keyPrefix){
        //1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowTimestamp = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowTimestamp - START_TIMESTAMP;
        //2.生成序列号
        //2.1获取当前日期，精确到天
        String date = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        //2.2生成序列号
        long increment = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        //3.接收返回
        return timestamp << COUNT_BITS | increment;
    }

    public static void main(String[] args) {
        LocalDateTime time = LocalDateTime.of(2023, 10, 1, 0, 0, 0);
        long epochSecond = time.toEpochSecond(ZoneOffset.UTC);
        System.out.println(epochSecond);
    }
}
