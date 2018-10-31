package com.flytxt.miscellaneous;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;

import com.flytxt.miscellaneous.entity.HbaseDataEntity;
import com.flytxt.miscellaneous.hbase.HBaseDataInteractor;
import com.flytxt.miscellaneous.locking.DistributedLock;
import com.flytxt.miscellaneous.locking.redis.RedisLock;

/**
 * The HbaseBackedMap class
 *
 * @author sivasyam
 *
 */
public class HbaseBackedMap extends HBaseDataInteractor {

    private HashMap<Long, String> dataStorageMap;

    private DistributedLock distributedLock;

    private long lastRowValue;

    public HbaseBackedMap(String redisServerIPAndPort) {
        super();
        dataStorageMap = new HashMap<Long, String>();
        lastRowValue = 0;
        distributedLock = new RedisLock(redisServerIPAndPort);
    }

    private void put(HbaseDataEntity hbaseDataEntity) {
        try {
            distributedLock.accquire();
            super.putDataToHbase(hbaseDataEntity);
            dataStorageMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsString());
            distributedLock.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String get(Long key) {
        try {
            if (dataStorageMap.containsKey(key)) {
                return dataStorageMap.get(key);
            } else {
                HbaseDataEntity hbaseDataEntity = super.getDataFromHBase(Bytes.toBytes(key));
                if (hbaseDataEntity.getValueAsByte() != null) {
                    dataStorageMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsString());
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
            super.removeDataFromHbase(Bytes.toBytes(key));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long put(String value) {
        try {
            if (distributedLock.isLocked()) {
                Thread.sleep(5000);
                this.put(value);
            }
            for (Entry<Long, String> inMemoryStringValue : dataStorageMap.entrySet()) {
                if (inMemoryStringValue.getValue().equals(value)) {
                    return inMemoryStringValue.getKey();
                }

            }
            HbaseDataEntity hbaseDataEntity = super.scanHbaseForEntity(Bytes.toBytes(value));
            if (hbaseDataEntity == null) {
                if (lastRowValue == 0) {
                    HbaseDataEntity lastRowData = super.getLastRowData();
                    if (lastRowData != null && lastRowData.getKeyAsLong() != null) {
                        lastRowValue = lastRowData.getKeyAsLong();
                    }
                }
                long newlyGeneratedLastRowValue = lastRowValue + 1;
                hbaseDataEntity = new HbaseDataEntity(newlyGeneratedLastRowValue, value);
                this.put(hbaseDataEntity);
                lastRowValue = newlyGeneratedLastRowValue;
                return newlyGeneratedLastRowValue;
            } else {
                dataStorageMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsString());
                return hbaseDataEntity.getKeyAsLong();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}