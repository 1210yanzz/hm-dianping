package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        String key = "shopType:list";
        // 1.从redis中查询店铺类型信息
        List<String> shopTypes = stringRedisTemplate.opsForList().range(key, 0, -1);
        // 2.判断店铺类型信息是否存在
        if(shopTypes != null && !shopTypes.isEmpty()){
            // 3.存在，返回店铺类型信息（遍历集合，将字符串转换为ShopType对象，再存入集合中）
            List<ShopType> shopTypeList = shopTypes.stream().map(shopType -> {
                ShopType shopType1 = JSONUtil.toBean(shopType, ShopType.class);  // 将字符串转换为ShopType对象
                return shopType1;
            }).collect(Collectors.toList());
            return Result.ok(shopTypeList);
        }
        // 4.不存在，查询数据库
        List<ShopType> shopTypeList = list();
        // 5.判断数据库查询结果是否存在，
        if(shopTypeList == null || shopTypeList.isEmpty()){
            // 6.如果不存在，返回空
            return Result.fail("店铺类型信息不存在");
        }
        // 7.存在，将数据库查询结果写入redis（遍历集合，将ShopType对象转换为字符串，再存入集合中）
        List<String> shopTypeListStr = shopTypeList.stream()
                .map(shopType -> JSONUtil.toJsonStr(shopType))
                .collect(Collectors.toList());
        stringRedisTemplate.opsForList().leftPushAll(key, shopTypeListStr);
        // 6.返回数据库查询结果
        return Result.ok(shopTypeList);
    }
}
