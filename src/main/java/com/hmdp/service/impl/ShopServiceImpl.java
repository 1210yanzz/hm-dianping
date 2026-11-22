package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
        // Shop shop = queryWithPassThrough(id);
        //Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 缓存穿透和缓存击穿
        //Shop shop = queryWithMutex(id);

        // 通过逻辑过期来解决缓存击穿问题
        // Shop shop = queryWithLogicalExpire(id);
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);


        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);  //返回数据库查询结果
    }

    // 解决缓存穿透问题
    /*public Shop queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY + id;
        // 1.从redis中查询店铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断店铺缓存是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 3.如果存在，返回店铺信息
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;  //JSONUtil的toBean方法将json字符串转换为Shop对象
        }
        // 判断店铺缓存是否为空值（isNotBlank只有在字符串有值时才判断true，null和""都判断为false）
        if (shopJson != null) {   //前面判断shopJson是null或是""，这里判断shopJson不是null，得到shopJson是空值""
            // 返回错误信息
            return null;
        }
        // 4.如果不存在，查询数据库
        Shop shop = getById(id);
        // 5.判断数据库查询结果是否存在
        if (shop == null) {
            // 6.如果不存在，将空值写入redis并返回错误信息
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 7.如果存在，将数据库查询结果返回并写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);  //JSONUtil的toJsonStr方法将Shop对象转换为json字符串
        return shop;  //返回数据库查询结果
    }*/

    // 解决缓存穿透问题的基础上，再解决缓存击穿问题
    /*public Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        // 1.从redis中查询店铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断店铺缓存是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 3.如果存在，返回店铺信息
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;  //JSONUtil的toBean方法将json字符串转换为Shop对象
        }
        // 判断店铺缓存是否为空值（isNotBlank只有在字符串有值时才判断true，null和空串""都判断为false）
        if (shopJson != null) {   //前面判断shopJson是null或是""，这里判断shopJson不是null，得到shopJson是空值""
            // 返回错误信息
            return null;
        }
        // 4.实现缓存重建
        // 4.1.尝试获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 4.2 判断是否获取成功
            if (!isLock) {
                // 4.3 如果获取失败，则休眠一段时间并重试
                Thread.sleep(50);  // 休眠50毫秒
                return queryWithMutex(id);  // 重试查询
            }
            // 4.4 获取成功，查询数据库
            shop = getById(id);
            Thread.sleep(200);
            // 5.判断数据库查询结果是否存在
            if (shop == null) {
                // 6.如果不存在，将空值写入redis并返回错误信息
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 7.如果存在，将数据库查询结果返回并写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);  //JSONUtil的toJsonStr方法将Shop对象转换为json字符串
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 8.释放锁
            unlock(lockKey);
        }
        return shop;  //返回数据库查询结果
    }*/

    // 新建一个线程池
    //private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    // 通过逻辑过期来解决缓存击穿问题
    /*public Shop queryWithLogicalExpire(Long id) {
        String key = CACHE_SHOP_KEY + id;
        // 1.从redis中查询店铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断店铺缓存是否存在
        if (StrUtil.isBlank(shopJson)) {
            // 3.如果不存在，返回null
            return null;
        }
        // 4.存在，判断缓存是否过期
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);  // JSONUtil的toBean方法将json字符串转换为RedisData对象
        JSONObject shopData = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(shopData, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.过期时间在当前时间之后，则未过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 未过期，返回店铺信息
            return shop;
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
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unlock(lockKey);
                }
            });
        }
        //  获取失败，则直接返回店铺信息
        return shop;
    }*/


    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        String key = CACHE_SHOP_KEY + id;
        // 1.获取店铺信息
        Shop shop = getById(id);
        Thread.sleep(200);
        // 2.设置逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 3.将店铺信息写入redis
        String shopJSON = JSONUtil.toJsonStr(redisData);
        stringRedisTemplate.opsForValue().set(key, shopJSON);
    }

    // 尝试获取锁，获取成功返回true，获取失败返回false
    /*private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }*/

    // 释放锁
    /*private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }*/

    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要根据坐标查询，按数据库查询
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;  // 计算分页的起始位置
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;  // 计算分页的结束位置
        // 查询redis，按照距离排序、分页
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        // 解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 列表元素不足from个，则返回空列表
            return Result.ok(Collections.emptyList());
        }
        // 截取from到end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            String idStr = result.getContent().getName();  // 获取id
            ids.add(Long.valueOf(idStr));
            Distance distance = result.getDistance();// 获取距离
            distanceMap.put(idStr, distance);
        });
        // 根据id查询店铺
        String idsStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("order by field(id, " + idsStr + ")").list();
        // 添加距离信息
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 返回
        return Result.ok(shops);
    }

}
