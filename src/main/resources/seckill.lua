--1.参数列表
--1.1 优惠卷id
local voucherId = ARGV[1]
--1.2 用户id
local userId = ARGV[2]
--1.3 订单id
local orderId = ARGV[3]

--2.数据key
--2.1 库存key
local stockKey = "seckill:stock:" .. voucherId
--2.2 订单key
local orderKey = "seckill:order:" .. voucherId

--3.逻辑处理
--3.1 判断库存是否充足
if (tonumber(redis.call("get", stockKey)) <= 0) then
    --库存不足
    return 1
end
--3.2 判断用户是否重复下单
if (redis.call("sismember", orderKey, userId) == 1) then
    --用户重复下单
    return 2
end
--3.3 非重复下单，第一次来就创建订单
--3.3.1 扣库存
redis.call("decr", stockKey)
--3.3.2 创建订单
redis.call("sadd", orderKey, userId)
--4. 发送消息到消息队列
redis.call("xadd", "stream.order", "*", "userId", userId, "voucherId", voucherId, "id", orderId)
--5. 返回结果
return 0

