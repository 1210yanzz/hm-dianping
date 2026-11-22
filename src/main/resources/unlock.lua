
--比较当前线程标识与锁的线程标识是否一致
if(redis.call('get', KEYS[1]) == ARGV[1]) then
    --释放锁
    return redis.call('del', KEYS[1])
end
--不一致，失败返回0
return 0