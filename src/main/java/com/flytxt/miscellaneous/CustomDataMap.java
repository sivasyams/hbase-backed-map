package com.flytxt.miscellaneous;

import java.util.HashMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.flytxt.miscellaneous.entity.HbaseDataEntity;

/**
 * The CustomDataMap class
 *
 * @author sivasyam
 *
 */
public class CustomDataMap {

    private static HashMap<Long, byte[]> keyValueMap = new HashMap<Long, byte[]>();

    private static HashMap<String, Long> valueKeyMap = new HashMap<String, Long>();

    public static void put(Long key, byte[] value) {
        keyValueMap.put(key, value);
        valueKeyMap.put(Bytes.toString(value), key);
    }

    public static byte[] getValue(Long key) {
        return keyValueMap.get(key);
    }

    public static Long getKey(String value) {
        return valueKeyMap.get(value);
    }

    public static void remove(Long key) {
        String value = Bytes.toString(keyValueMap.get(key));
        valueKeyMap.remove(value);
        keyValueMap.remove(key);
    }

    public static void loadToMemory(HbaseDataEntity hbaseDataEntity) {
        keyValueMap.put(hbaseDataEntity.getKeyAsLong(), hbaseDataEntity.getValueAsByte());
        valueKeyMap.put(hbaseDataEntity.getValueAsString(), hbaseDataEntity.getKeyAsLong());
    }
}
