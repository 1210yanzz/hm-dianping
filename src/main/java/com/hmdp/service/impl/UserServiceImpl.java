package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private UserMapper userMapper;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 发送手机验证码
     *
     * @param phone
     * @param session
     * @return
     */
    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            //2.手机号不符合，返回错误信息
            return Result.fail("手机号格式无效");
        }
        //3.符合，生成验证码
        String code = RandomUtil.randomNumbers(6);
        //4.将验证码保存到redis中
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //5.发送验证码
        log.info("发送验证码成功，验证码为：{}", code);
        //6.返回
        return Result.ok();
    }

    /**
     * 登录功能
     *
     * @param loginForm
     * @param session
     * @return
     */
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1.校验手机号
        if (RegexUtils.isPhoneInvalid(loginForm.getPhone())) {
            return Result.fail("手机号格式无效");
        }
        //2.从redis中获取验证码，校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + loginForm.getPhone());
        String code = loginForm.getCode();
        if (cacheCode == null || !code.equals(cacheCode)) {
            //3.验证码无效，返回错误信息
            return Result.fail("验证码无效");
        }
        //4.根据手机号查询用户
        User user = userMapper.selectByPhone(loginForm.getPhone());
        //5.判断手机号是否存在
        if (user == null) {
            //6.不存在，创建新用户
            user = createUserWithPhone(loginForm.getPhone());
        }
        //7.保存用户信息到redis
        //7.1 生成随机token，作为登录令牌
        String token = UUID.randomUUID().toString();
        //7.2 将user对象转为HashMap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString())
        );
        //7.3保存
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY + token, userMap);
        //7.4 设置redis缓存的过期时间
        stringRedisTemplate.expire(LOGIN_USER_KEY + token, 30, TimeUnit.MINUTES);
        //8. 返回token
        return Result.ok(token);
    }

    /**
     * 根据手机号创建用户
     *
     * @param phone
     * @return
     */
    private User createUserWithPhone(String phone) {
        //1.创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(12));
        //2.保存用户
        save(user);
        return user;
    }
}
