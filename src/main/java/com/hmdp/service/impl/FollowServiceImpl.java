package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private FollowMapper followMapper;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService iUserService;

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        // 获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 判断是否关注
        if (isFollow) {
            // 关注，新增数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            Boolean result = save(follow);
            // 关注成功，保存到Redis的set集合  sadd userId followUserId
            if (result) {
                stringRedisTemplate.opsForSet().add("follows:" + userId, followUserId.toString());
            }
        } else {
            // 取消关注，删除数据
            Boolean result = followMapper.unfollow(userId, followUserId);
            // 取关成功，从Redis的set集合中删除  sdel userId followUserId
            if (result) {
                stringRedisTemplate.opsForSet().remove("follows:" + userId, followUserId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        // 查询是否关注
        // 获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        Integer count = followMapper.isFollow(userId, followUserId);

        return Result.ok(count > 0);
    }

    @Override
    public Result getFollowers(Long id) {
        // 获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        String key1 = "follows:" + userId;
        // 获取指定用户
        String key2 = "follows:" + id;
        // 求交集，找共同关注者
        Set<String> followers = stringRedisTemplate.opsForSet().intersect(key1, key2);
        // 判断是否存在交集
        if (followers == null || followers.size() == 0) {
            return Result.ok(Collections.emptyList());
        }
        // 将Set集合转换为List集合
        List<Long> ids = followers.stream().map(s -> Long.valueOf(s)).collect(Collectors.toList());
        // 查询用户信息
        List<User> users = iUserService.listByIds(ids);
        List<UserDTO> userDTOS = users
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }
}
