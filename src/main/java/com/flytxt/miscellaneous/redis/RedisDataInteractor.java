package com.flytxt.miscellaneous.redis;

import com.flytxt.miscellaneous.entity.HbaseDataEntity;

import redis.clients.jedis.Jedis;

/**
 * The RedisInteractor class
 *
 * @author sivasyam
 *
 */
public class RedisDataInteractor {

    private Jedis jedis;

    public RedisDataInteractor(String serverHostName) {
        String redisHost = serverHostName.split("[:]")[0];
        Integer redisPort = Integer.parseInt(serverHostName.split("[:]")[1]);
        jedis = new Jedis(redisHost, redisPort);
    }

    public void putDataToRedis(HbaseDataEntity hbaseDataEntity) {
        jedis.set(hbaseDataEntity.getValueAsByte(), hbaseDataEntity.getKeyAsByte());
    }

    public HbaseDataEntity getDataFromRedis(byte[] hbaseValue) {
        byte[] hbaseKey = jedis.get(hbaseValue);
        if (hbaseKey == null) {
            return null;
        }
        HbaseDataEntity hbaseDataEntity = new HbaseDataEntity(hbaseKey, hbaseValue);
        return hbaseDataEntity;
    }

    public void removeDataFromRedis(byte[] key) {
        jedis.del(key);
    }

    public Jedis getHandle() {
        return jedis;
    }
}
