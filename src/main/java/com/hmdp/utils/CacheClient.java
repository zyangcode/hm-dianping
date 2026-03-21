package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.events.Event;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {


    private StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    //1.普通缓存
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    //2.逻辑过期缓存
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        redisData.setData(value);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    // 缓存穿透
    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {//使用泛型，只要你赋值给type,所有的R都可确定
        String key = keyPrefix + id;
        // 1. 从 Redis 查询商铺缓存
        String Json = stringRedisTemplate.opsForValue().get(key);
        // 2. 判断缓存是否为null或者空对象
        if (StrUtil.isNotBlank(Json)) {
            // 3. 不是：将 JSON 字符串转为 Shop 对象并返回
            return JSONUtil.toBean(Json, type);

        }
        if (Json != null) {
            //缓存是否为空字符串：返回错误
            return null;
        }
        // 4. 是：根据 id 查询数据库
        R r = dbFallback.apply(id);
        // 5. 数据库也不存在：返回错误
        if (r == null) {
            //将key为shop的id，value为空字符串，过期时间为5分钟的键值对存到Redis中
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 6. 数据库存在：将 Shop 对象转为 JSON 字符串，写入 Redis 缓存
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(r), time, TimeUnit.MINUTES);
        this.set(key, r, time, unit);
        // 7. 返回查询结果
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    // 缓存击穿 ：逻辑过期
    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
//        // 1. 从 Redis 查询商铺缓存
//        String Json = stringRedisTemplate.opsForValue().get(key);
//        // 2. 判断缓存是否为null或者空对象
//        if (StrUtil.isBlank(Json)) {
//            return null;
//        }
//        // 3. 不是：将 JSON 字符串转为 RedisData 对象
//        RedisData redisData = JSONUtil.toBean(Json, RedisData.class);
//        // 4.获取数据，过期时间，判断是否过期
//        // 4.1 获取数据
//        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
//        // 4.2 获取过期时间
//        LocalDateTime expireTime = redisData.getExpireTime();

        LogicalExpire<R> logicalExpire = queryWithLogicalExpire1(key, type);
        if (logicalExpire == null) {
            return null;
        }
        LocalDateTime expireTime = logicalExpire.getExpireTime();
        R r = logicalExpire.getData();
        // 5. 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 未过期，返回数据
            return r;
        }
        // 5.2 过期，缓存重建
        // 6 缓存重建
        // 6.1 获取互斥锁
        Boolean isLock = tryLock(LOCK_SHOP_KEY + id);
        // 6.2 判断是否获取到锁
        if (isLock) {
            //为了排除第一个线程拿到锁，执行查询，重建后，释放锁，然后又有一些残余的线程刚好执行到判断是否有锁，此时就会拿到锁的情况，
            //此时可以让当前的过期时间与重新查询的过期时间进行比较，不相同直接返回旧数据，相同则重建缓存
            LogicalExpire<R> logicalExpire1 = queryWithLogicalExpire1(key, type);
            if (logicalExpire1 == null) {
                return null;
            }
            LocalDateTime expireTime1 = logicalExpire1.getExpireTime();
            R r2 = logicalExpire1.getData();
            //判断过期时间是否相同
            if (!expireTime1.equals(expireTime)) {
                // 过期时间不同，直接返回旧数据
                return r2;
            }
            // 6.3 成功，开启独立线程(使用线程池），缓存重建，释放互斥锁
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 重建缓存
                    // 查询数据库
                    R r1 = dbFallback.apply(id);
                    // 写入缓存
                    setWithLogicalExpire(key, r1, time, unit);

                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放互斥锁
                    unlock(LOCK_SHOP_KEY + id);
                }
            });

        }
        // 6.4 失败，返回旧信息
        return r;
    }

    private Boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);//明确目的：当一个用户成功拿到锁时，只要不主动释放锁或者超时，其他人就拿不到锁
        //而这个代码就实现了这个逻辑
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

   public <ID,R> LogicalExpire queryWithLogicalExpire1(String key, Class<R> type){
       // 1. 从 Redis 查询商铺缓存
       String Json = stringRedisTemplate.opsForValue().get(key);
       // 2. 判断缓存是否为null或者空对象
       if (StrUtil.isBlank(Json)) {
           return null;
       }
       // 3. 不是：将 JSON 字符串转为 RedisData 对象
       RedisData redisData = JSONUtil.toBean(Json, RedisData.class);
       // 4.获取数据，过期时间，判断是否过期
       // 4.1 获取数据
       R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
       // 4.2 获取过期时间
       LocalDateTime expireTime = redisData.getExpireTime();
       return new LogicalExpire<R>(expireTime, r);
   }
}