package com.flytxt.miscellaneous;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Bytes;

import com.flytxt.miscellaneous.entity.HbaseDataEntity;
import com.flytxt.miscellaneous.hbase.HBaseDataInteractor;
import com.flytxt.miscellaneous.locking.DistributedLock;
import com.flytxt.miscellaneous.locking.redis.RedisLock;
import com.flytxt.miscellaneous.redis.RedisDataInteractor;

import redis.clients.jedis.ScanResult;

/**
 * The HbaseBackedMap class
 *
 * @author sivasyam
 *
 */
public class HbaseBackedMap extends HBaseDataInteractor {

    private HashMap<Long, byte[]> dataStorageMap;

    private DistributedLock distributedLock;

    private RedisDataInteractor redisDataInteractor;

    private long lastRowKey = 0;

    private ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);

    public HbaseBackedMap() {
        super();
        try {
            dataStorageMap = new HashMap<Long, byte[]>();
            this.updateHbaseLastRowKey();
            scheduledExecutor.schedule(hbaseDataCommiter, 60, TimeUnit.SECONDS);
        } catch (Exception e) {
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
    }

    private void put(HbaseDataEntity hbaseDataEntity) {
        try {
            distributedLock.accquire();
            dataStorageMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsByte());
            redisDataInteractor.putDataToRedis(hbaseDataEntity);
            super.putDataToHbase(hbaseDataEntity);
            distributedLock.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String get(Long key) {
        try {
            if (dataStorageMap.containsKey(key)) {
                return Bytes.toString(dataStorageMap.get(key));
            } else {
                HbaseDataEntity hbaseDataEntity = redisDataInteractor.getDataFromRedis(Bytes.toBytes(key));
                if (hbaseDataEntity.getValueAsByte() == null) {
                    hbaseDataEntity = super.getDataFromHbase(Bytes.toBytes(key));
                    if (hbaseDataEntity.getValueAsByte() != null) {
                        dataStorageMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsByte());
                        return hbaseDataEntity.getValueAsString();
                    }
                } else {
                    dataStorageMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsByte());
                    return hbaseDataEntity.getValueAsString();
                }
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void remove(Long key) {
        try {
            if (dataStorageMap.containsKey(key)) {
                dataStorageMap.remove(key);
            }
            redisDataInteractor.removeDataFromRedis(Bytes.toBytes(key));
            super.removeDataFromHbase(Bytes.toBytes(key));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long put(String value) {
        try {
            if (distributedLock.isLocked()) {
                Thread.sleep(500);
                this.put(value);
            }
            for (Entry<Long, byte[]> inMemoryData : dataStorageMap.entrySet()) {
                if (Bytes.toString(inMemoryData.getValue()).equals(value)) {
                    return inMemoryData.getKey();
                }
            }
            HbaseDataEntity hbaseDataEntity = redisDataInteractor.getDataFromRedis(Bytes.toBytes(value));
            if (hbaseDataEntity == null) {
                hbaseDataEntity = super.scanHbaseForEntity(Bytes.toBytes(value));
                if (hbaseDataEntity == null) {
                    if (this.lastRowKey == 0) {
                        this.updateHbaseLastRowKey();
                    }
                    long newlyGeneratedLastRowValue = lastRowKey + 1;
                    hbaseDataEntity = new HbaseDataEntity(newlyGeneratedLastRowValue, value);
                    this.put(hbaseDataEntity);
                    lastRowKey = newlyGeneratedLastRowValue;
                    return newlyGeneratedLastRowValue;
                } else {
                    redisDataInteractor.putDataToRedis(hbaseDataEntity);
                    dataStorageMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsByte());
                    return hbaseDataEntity.getKeyAsLong();
                }
            } else {
                dataStorageMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsByte());
                return hbaseDataEntity.getKeyAsLong();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void exportDataFromRedis(long lastHbaseRowKey) {
        try {
            ScanResult<String> scanResult = redisDataInteractor.getHandle().scan("0");
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
                    byte[] hbaseKey = redisDataInteractor.getHandle().get(Bytes.toBytes(hbaseData));
                    if (Bytes.toLong(hbaseKey) > lastHbaseRowKey) {
                        HbaseDataEntity hbaseDataEntityToExport = new HbaseDataEntity(hbaseKey, Bytes.toBytes(hbaseData));
                        super.putDataToHbase(hbaseDataEntityToExport);
                    }
                }
                if (nextCursor.equals("0")) {
                    break;
                }
                scanResult = redisDataInteractor.getHandle().scan(nextCursor);
            }
            super.commitHbaseData();
            this.updateHbaseLastRowKey();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void updateHbaseLastRowKey() {
        try {
            HbaseDataEntity lastRowData = super.getLastRowData();
            if (lastRowData != null && lastRowData.getKeyAsLong() != null) {
                this.lastRowKey = lastRowData.getKeyAsLong();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void commitData() {
        try {
            distributedLock.accquire();
            super.commitHbaseData();
            distributedLock.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    Runnable hbaseDataCommiter = new Runnable() {

        public void run() {
            try {
                if (!distributedLock.isLocked()) {
                    commitData();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    };
}