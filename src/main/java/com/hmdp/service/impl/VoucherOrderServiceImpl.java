package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWork;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


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
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWork redisIdWork;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    // @PostConstruct表示在当前类初始化之后执行
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {

        String queueName = "stream.order";

        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取消息队列中的消息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed()));
                    // 2.判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        //如果失败，说明没有获取到消息，继续下一次获取
                        continue;
                    }
                    // 3.如果成功，解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);  //count = 1所以只有一个消息，直接取第一个
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //获取到信息，可以下单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认消息 SACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("获取订单信息失败", e);
                    // 未被确认的消息会进入pending-list，要进行处理
                    handlePendingList();
                }

            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取pending-list中的消息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0")));
                    // 2.判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        //如果失败，说明pending-list中没有消息，结束循环
                        break;
                    }
                    // 3.如果成功，解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);  //count = 1所以只有一个消息，直接取第一个
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //如果成功，说明获取到信息，可以下单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认消息 SACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单消息异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

    // 阻塞队列
/*    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取阻塞队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("获取订单信息失败", e);
                }

            }
        }
    }*/

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 获取用户
        Long userId = voucherOrder.getUserId();
        // 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 获取锁
        boolean isLock = lock.tryLock();
        // 判断是否获取锁成功
        if (!isLock) {
            log.error("不允许重复下单");
            return;
        }
        try {
            // 获取代理对象
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户id
        Long userId = UserHolder.getUser().getId();
        // 生成订单id
        long orderId = redisIdWork.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId));
        // 2.判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            // 2.1 不为，0无购买资格
            return Result.fail(r == 1 ? "库存不足" : "请勿重复下单");
        }
        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 3. 返回结果
        return Result.ok(orderId);
    }

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString());
        // 2.判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            // 2.1 不为，0无购买资格
            return Result.fail(r == 1 ? "库存不足" : "请勿重复下单");
        }
        // 2.2 为0有购买资格，把下单信息保存到阻塞队列
        long orderId = redisIdWork.nextId("order");
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3 设置订单id
        voucherOrder.setId(orderId);
        //2.4.设置用户id
        voucherOrder.setUserId(UserHolder.getUser().getId());
        //2.5.设置优惠券id
        voucherOrder.setVoucherId(voucherId);
        // 2.6.将下单信息保存到阻塞队列
        orderTasks.add(voucherOrder);
        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 3. 返回结果
        return Result.ok(orderId);
    }*/


    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //5.一人一单
        Long userId = voucherOrder.getUserId();
        //5.1.查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //5.2 判断订单是否存在
        if (count > 0) {
            log.error("用户已经购买过该优惠券!");
        }
        //6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")  //set stock = stock - 1
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)  //where voucher_id = voucherId and stock > 0
                .update();
        if (!success) {
            log.error("扣减失败!");
        }
        //7.创建订单
        save(voucherOrder);

    }


/*    @Override
    public Result seckillVoucher(Long voucherId) {
        //1.查询优惠劵
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断优惠卷秒杀是否开始
        LocalDateTime beginTime = voucher.getBeginTime();
        if (beginTime.isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始!");
        }
        //3.判断秒杀是否结束
        LocalDateTime endTime = voucher.getEndTime();
        if (endTime.isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束!");
        }
        //4.判断库存是否充足
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足!");
        }

        Long userId = UserHolder.getUser().getId();
        // 创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 获取锁
        boolean isLock = lock.tryLock();
        // 判断是否获取锁成功
        if (!isLock) {
            return Result.fail("请勿重复下单!");
        }
        try {
            // 获取代理对象
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }

    }*/

    /*@NonNull
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        //5.一人一单
        //5.1.查询订单
        int count = query().eq("user_id", UserHolder.getUser().getId()).eq("voucher_id", voucherId).count();
        //5.2 判断订单是否存在
        if (count > 0) {
            return Result.fail("用户已经购买过该优惠券!");
        }
        //6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")  //set stock = stock - 1
                .eq("voucher_id", voucherId).gt("stock", 0)  //where voucher_id = voucherId and stock > 0
                .update();
        if (!success) {
            return Result.fail("扣减失败!");
        }
        //7.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //7.1.设置订单id
        long orderId = redisIdWork.nextId("order");
        voucherOrder.setId(orderId);
        //7.2.设置用户id
        voucherOrder.setUserId(UserHolder.getUser().getId());
        //7.3.设置优惠券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        //8.返回订单
        return Result.ok(orderId);

    }*/
}
