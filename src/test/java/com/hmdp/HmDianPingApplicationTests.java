package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisIdWork;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private CacheClient cacheClient;

    @Resource
    private RedisIdWork redisIdWork;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Test
    public void testSaveShop() throws InterruptedException {
        Shop shop = shopService.getById(14L);

        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + 14, shop, 10L, TimeUnit.SECONDS);
    }

    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    public void testIdWork() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(300);


        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWork.nextId("order");
                System.out.println("id = " + id);
            }
            latch.countDown();
        };

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();
        long endTime = System.currentTimeMillis();
        System.out.println("time = " + (endTime - startTime));
    }

    @Test
    public void loadShopData() {
        // 1. 查询店铺信息
        List<Shop> list = shopService.list();
        // 2. 将店铺分组，按照typeId进行分组，id一致的放入一个集合
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        // 3. 分批写入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            // 3.1. 获取类型id
            Long typeId = entry.getKey();
            String key = SHOP_GEO_KEY + typeId;
            // 3.2. 获取同类型的店铺集合
            List<Shop> shops = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shops.size());
            // 3.3. 写入redis geo add key 经度 纬度 member
            for (Shop shop : shops) {
                //stringRedisTemplate.opsForGeo().add(key, new Point(shop.getX(), shop.getY()), shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(shop.getId().toString(), new Point(shop.getX(), shop.getY())));
            }
            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }


}
