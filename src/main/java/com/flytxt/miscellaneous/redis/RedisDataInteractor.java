package com.flytxt.miscellaneous.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flytxt.miscellaneous.entity.HbaseDataEntity;

import redis.clients.jedis.Jedis;

/**
 * The RedisInteractor class
 *
 * @author sivasyam
 *
 */
public class RedisDataInteractor {

    private String redisHostName;

    private Integer redisHostPort;

    private final Logger redisDataInteractorLogger = LoggerFactory.getLogger(this.getClass());

    public RedisDataInteractor(String serverHostName) {
        redisHostName = serverHostName.split("[:]")[0];
        redisHostPort = Integer.parseInt(serverHostName.split("[:]")[1]);
    }

    public void putDataToRedis(HbaseDataEntity hbaseDataEntity) {
        Jedis jedis = this.getJedis();
        try {
            jedis.set(hbaseDataEntity.getValueAsByte(), hbaseDataEntity.getKeyAsByte());
        } catch (Exception e) {
            redisDataInteractorLogger.error("ERROR: {}", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null && jedis.isConnected()) {
                jedis.close();
            }
        }
    }

    public HbaseDataEntity getDataFromRedis(byte[] hbaseValue) {
        Jedis jedis = this.getJedis();
        try {
            byte[] hbaseKey = jedis.get(hbaseValue);
            if (hbaseKey == null) {
                return null;
            }
            HbaseDataEntity hbaseDataEntity = new HbaseDataEntity(hbaseKey, hbaseValue);
            return hbaseDataEntity;
        } catch (Exception e) {
            redisDataInteractorLogger.error("ERROR: {}", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null && jedis.isConnected()) {
                jedis.close();
            }
        }
    }

    public void removeDataFromRedis(byte[] key) {
        Jedis jedis = this.getJedis();
        try {
            jedis.del(key);
        } catch (Exception e) {
            redisDataInteractorLogger.error("ERROR: {}", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null && jedis.isConnected()) {
                jedis.close();
            }
        }
    }

    public Jedis getJedis() {
        Jedis jedis = RedisResourceManger.getResourceFromPool(redisHostName, redisHostPort);
        return jedis;
    }
}
