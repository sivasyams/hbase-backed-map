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
        jedis = new Jedis(serverHostName);
    }

    public void putDataToRedis(HbaseDataEntity hbaseDataEntity) {
        jedis.set(hbaseDataEntity.getKeyAsByte(), hbaseDataEntity.getValueAsByte());
    }

    public HbaseDataEntity getDataFromRedis(byte[] key) {
        HbaseDataEntity hbaseDataEntity = new HbaseDataEntity(key, jedis.get(key));
        return hbaseDataEntity;
    }

    public void removeDataFromRedis(byte[] key) {
        jedis.del(key);
    }
}
