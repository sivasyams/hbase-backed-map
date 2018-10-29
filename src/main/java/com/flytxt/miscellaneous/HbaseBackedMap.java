package com.flytxt.miscellaneous;

import java.io.IOException;
import java.util.HashMap;

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

    private HashMap<String, Long> dataStorageMap;

    private DistributedLock distributedLock;

    private long lastRowValue;

    public HbaseBackedMap(String serverIP, String serverPort) {
        super();
        dataStorageMap = new HashMap<String, Long>();
        lastRowValue = 0;
        distributedLock = new RedisLock(serverIP, serverPort);
    }

    private void loadData(String key, Long value) {
        try {
            distributedLock.accquire();
            HbaseDataEntity hbaseDataEntity = new HbaseDataEntity(key, value);
            super.putDataToHbase(hbaseDataEntity);
            dataStorageMap.put(key, value);
            distributedLock.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Long getData(String key) {
        try {
            if (dataStorageMap.containsKey(key)) {
                return dataStorageMap.get(key);
            } else {
                HbaseDataEntity hbaseDataEntity = super.getDataFromHBase(Bytes.toBytes(key));
                if (hbaseDataEntity.getValueAsByte() != null) {
                    dataStorageMap.put(hbaseDataEntity.getKeyAsString(), hbaseDataEntity.getValueAsLong());
                    return hbaseDataEntity.getValueAsLong();
                }
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void remove(String key) {
        try {
            if (dataStorageMap.containsKey(key)) {
                dataStorageMap.remove(key);
            }
            super.removeDataFromHbase(Bytes.toBytes(key));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long put(String key) {
        try {
            if (distributedLock.isLocked()) {
                Thread.sleep(5000);
                this.put(key);
            }
            Long equivalentLongValue = this.getData(key);
            if (equivalentLongValue == null) {
                if (lastRowValue == 0) {
                    HbaseDataEntity lastRowData = super.getLastRowData();
                    if (lastRowData != null && lastRowData.getValueAsLong() != null) {
                        lastRowValue = Bytes.toLong(lastRowData.getValueAsByte());
                    }
                }
                long newlyGeneratedLastRowValue = lastRowValue + 1;
                this.loadData(key, newlyGeneratedLastRowValue);
                lastRowValue = newlyGeneratedLastRowValue;
                return newlyGeneratedLastRowValue;
            } else {
                return equivalentLongValue;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}