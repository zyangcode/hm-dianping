package com.hmdp;

import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private RedisIdWorker redisIdWorker;
    private ExecutorService es = Executors.newFixedThreadPool(100);

//    @Test
//    void testSaveShop() throws InterruptedException {
//        shopService.saveShop2Redis(1L, 10L);
//    }
//    @Test
//    void localTime(){
//        LocalDateTime now = LocalDateTime.now();
//        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
//        System.out.println(nowSecond);
//    }

    @Test
    void testNextId() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(10);
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                Long id = redisIdWorker.nextId("order");
                System.out.println(id);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println(end - begin);
        es.shutdown();

    }



}
