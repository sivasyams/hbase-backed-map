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

    private HashMap<Long, byte[]> dataStorageMap;

    private DistributedLock distributedLock;

    private long lastRowValue;

    public HbaseBackedMap() {
        super();
        dataStorageMap = new HashMap<Long, byte[]>();
        lastRowValue = 0;
    }

    public void setRedisLock(String serverDetails) {
        if (distributedLock == null) {
            distributedLock = new RedisLock(serverDetails);
        }
    }

    private void put(HbaseDataEntity hbaseDataEntity) {
        try {
            distributedLock.accquire();
            super.putDataToHbase(hbaseDataEntity);
            dataStorageMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsByte());
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
                HbaseDataEntity hbaseDataEntity = super.getDataFromHBase(Bytes.toBytes(key));
                if (hbaseDataEntity.getValueAsByte() != null) {
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
            for (Entry<Long, byte[]> inMemoryStringValue : dataStorageMap.entrySet()) {
                if (Bytes.toString(inMemoryStringValue.getValue()).equals(value)) {
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
                dataStorageMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsByte());
                return hbaseDataEntity.getKeyAsLong();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}