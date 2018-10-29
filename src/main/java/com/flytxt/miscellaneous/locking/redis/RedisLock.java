package com.flytxt.miscellaneous.locking.redis;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import com.flytxt.miscellaneous.locking.DistributedLock;

/**
 * The RedisLock class
 *
 * @author sivasyam
 *
 */
public class RedisLock implements DistributedLock {

    private Config redisConfig;

    private RedissonClient redissonClient;

    private RLock redisFairLock;

    private static final String HBASE_INSERT_LOCK = "hbaseInsertLock";

    public RedisLock(String redisServerIP, String redisServerPort) {
        redisConfig = new Config();
        redisConfig.useSingleServer().setAddress(redisServerIP + ":" + redisServerPort);
        redissonClient = Redisson.create(redisConfig);
        redisFairLock = redissonClient.getFairLock(HBASE_INSERT_LOCK);
    }

    public void accquire() {
        redisFairLock.lock();
    }

    public void release() {
        redisFairLock.unlock();
    }

    public boolean isLocked() {
        return redisFairLock.isLocked();
    }
}