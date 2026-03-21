package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    private static final long COUNT_BITS = 32;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final long START_TIMESTAMP = 1640995200L;

    public Long nextId(String keyPrefix) {
        //1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - START_TIMESTAMP;
        //2.生成序列号
        //2.1 获取当前日期，格式为yyyyMMddHHmmss
        String data = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //2.2 利用Redis指令生成序列号：输入对应的键，值自增1
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + data);
        //3 组装最终的ID
        return timestamp << COUNT_BITS | count;


    }

}
