package com.flytxt.miscellaneous;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flytxt.miscellaneous.entity.HbaseDataEntity;
import com.flytxt.miscellaneous.hbase.HBaseDataInteractor;
import com.flytxt.miscellaneous.locking.DistributedLock;
import com.flytxt.miscellaneous.locking.redis.RedisLock;
import com.flytxt.miscellaneous.redis.RedisDataInteractor;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanResult;

/**
 * The HbaseBackedMap class
 *
 * @author sivasyam
 *
 */
public class HbaseBackedMap extends HBaseDataInteractor {

    private DistributedLock distributedLock;

    private RedisDataInteractor redisDataInteractor;

    private long lastRowKey = 0;

    private ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);

    private final Logger hbaseBackedMapLogger = LoggerFactory.getLogger(this.getClass());

    public HbaseBackedMap() {
        super();
        try {
            this.updateHbaseLastRowKey();
            scheduledExecutor.scheduleWithFixedDelay(hbaseDataCommiter, 10, 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            hbaseBackedMapLogger.error("ERROR: {}", e);
            throw new RuntimeException(e);
        }
    }

    public void setRedisDetails(String serverDetails) {
        if (distributedLock == null) {
            distributedLock = new RedisLock(serverDetails);
        }
        if (redisDataInteractor == null) {
            redisDataInteractor = new RedisDataInteractor(serverDetails);
        }
        this.exportDataFromRedis(lastRowKey);
        super.loadHbaseDataToMemory();
    }

    private void put(HbaseDataEntity hbaseDataEntity) {
        try {
            CustomDataMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsByte());
            redisDataInteractor.putDataToRedis(hbaseDataEntity);
            super.putDataToHbase(hbaseDataEntity);
        } catch (Exception e) {
            hbaseBackedMapLogger.error("ERROR: {}", e);
            throw new RuntimeException(e);
        }
    }

    public String get(Long key) {
        try {
            if (CustomDataMap.getValue(key) != null) {
                return Bytes.toString(CustomDataMap.getValue(key));
            } else {
                HbaseDataEntity hbaseDataEntity = redisDataInteractor.getDataFromRedis(Bytes.toBytes(key));
                if (hbaseDataEntity.getValueAsByte() == null) {
                    hbaseDataEntity = super.getDataFromHbase(Bytes.toBytes(key));
                    if (hbaseDataEntity.getValueAsByte() != null) {
                        CustomDataMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsByte());
                        return hbaseDataEntity.getValueAsString();
                    }
                } else {
                    CustomDataMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsByte());
                    return hbaseDataEntity.getValueAsString();
                }
            }
            return null;
        } catch (IOException e) {
            hbaseBackedMapLogger.error("ERROR: {}", e);
            throw new RuntimeException(e);
        }
    }

    public void remove(Long key) {
        try {
            if (CustomDataMap.getValue(key) != null) {
                CustomDataMap.remove(key);
            }
            redisDataInteractor.removeDataFromRedis(Bytes.toBytes(key));
            super.removeDataFromHbase(Bytes.toBytes(key));
        } catch (IOException e) {
            hbaseBackedMapLogger.error("ERROR: {}", e);
            throw new RuntimeException(e);
        }
    }

    public long put(String value) {
        try {
            if (CustomDataMap.getKey(value) != null) {
                return CustomDataMap.getKey(value);
            }
            HbaseDataEntity hbaseDataEntity = redisDataInteractor.getDataFromRedis(Bytes.toBytes(value));
            if (hbaseDataEntity == null) {
                long uniqueDataIdentifier = createDataEntity(hbaseDataEntity, value);
                return uniqueDataIdentifier;
            } else {
                CustomDataMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsByte());
                super.putDataToHbase(hbaseDataEntity);
                return hbaseDataEntity.getKeyAsLong();
            }
        } catch (Exception e) {
            hbaseBackedMapLogger.error("ERROR: {}", e);
            throw new RuntimeException(e);
        }
    }

    private long createDataEntity(HbaseDataEntity hbaseDataEntity, String value) {
        try {
            if (distributedLock.isLocked()) {
                this.createDataEntity(hbaseDataEntity, value);
            }
            distributedLock.accquire();
            if (this.lastRowKey == 0) {
                this.updateHbaseLastRowKey();
            }
            long newlyGeneratedLastRowValue = lastRowKey + 1;
            hbaseDataEntity = new HbaseDataEntity(newlyGeneratedLastRowValue, value);
            this.put(hbaseDataEntity);
            lastRowKey = newlyGeneratedLastRowValue;
            distributedLock.release();
            return newlyGeneratedLastRowValue;
        } catch (Exception e) {
            hbaseBackedMapLogger.error("ERROR: {}", e);
            throw new RuntimeException(e);
        }
    }

    public HbaseDataEntity getHbaseDataEntity(String value) {
        try {
            HbaseDataEntity hbaseDataEntity = super.scanHbaseForEntity(Bytes.toBytes(value));
            if (hbaseDataEntity == null) {
                return null;
            } else {
                return hbaseDataEntity;
            }
        } catch (Exception e) {
            hbaseBackedMapLogger.error("ERROR: {}", e);
            throw new RuntimeException(e);
        }
    }

    private void exportDataFromRedis(long lastHbaseRowKey) {
        Jedis jedis = redisDataInteractor.getJedis();
        try {
            ScanResult<String> scanResult = jedis.scan("0");
            String nextCursor = scanResult.getStringCursor();
            int counter = 0;
            while (true) {
                nextCursor = scanResult.getStringCursor();
                List<String> hbaseDataList = scanResult.getResult();
                for (counter = 0; counter < hbaseDataList.size(); counter++) {
                    if (counter == hbaseDataList.size()) {
                        break;
                    }
                    String hbaseData = hbaseDataList.get(counter);
                    byte[] hbaseKey = jedis.get(Bytes.toBytes(hbaseData));
                    if (Bytes.toLong(hbaseKey) > lastHbaseRowKey) {
                        HbaseDataEntity hbaseDataEntityToExport = new HbaseDataEntity(hbaseKey, Bytes.toBytes(hbaseData));
                        super.putDataToHbase(hbaseDataEntityToExport);
                    }
                }
                if (nextCursor.equals("0")) {
                    break;
                }
                scanResult = jedis.scan(nextCursor);
            }
            super.commitHbaseData();
            this.updateHbaseLastRowKey();
        } catch (Exception e) {
            hbaseBackedMapLogger.error("ERROR: {}", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null && jedis.isConnected()) {
                jedis.close();
            }
        }
    }

    private void updateHbaseLastRowKey() {
        try {
            HbaseDataEntity lastRowData = super.getLastRowData();
            if (lastRowData != null && lastRowData.getKeyAsLong() != null) {
                this.lastRowKey = lastRowData.getKeyAsLong();
            }
        } catch (Exception e) {
            hbaseBackedMapLogger.info("ERROR: {}", e);
            throw new RuntimeException(e);
        }
    }

    private void persistData() {
        try {
            if (distributedLock.isLocked()) {
                this.persistData();
            }
            distributedLock.accquire();
            super.commitHbaseData();
            distributedLock.release();
        } catch (Exception e) {
            hbaseBackedMapLogger.error("ERROR: {}", e);
            throw new RuntimeException(e);
        }
    }

    Runnable hbaseDataCommiter = new Runnable() {

        public void run() {
            try {
                persistData();
            } catch (Exception e) {
                hbaseBackedMapLogger.error("ERROR: {}", e);
                throw new RuntimeException(e);
            }
        }
    };
}