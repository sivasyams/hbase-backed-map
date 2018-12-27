package com.flytxt.miscellaneous.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * The RedisResourceManger class
 *
 * @author sivasyam
 *
 */
public class RedisResourceManger {

    private static JedisPool jedisPool;

    private static volatile Object LOCK = new Object();

    public static Jedis getResourceFromPool(final String redisServerHost, final int redisServerPort) {
        if (jedisPool == null) {
            synchronized (LOCK) {
                if (jedisPool == null) {
                    jedisPool = new JedisPool(redisServerHost, redisServerPort);
                }
            }

        }
        return jedisPool.getResource();
    }
}