package com.flytxt.miscellaneous;

/**
 * The FlyDataSet class
 *
 * @author sivasyam
 *
 */
public class FlyDataSet {

    private static HbaseBackedMap hbaseBackedMap = new HbaseBackedMap();

    public static void setRedisDetails(String serverDetails) {
        hbaseBackedMap.setRedisDetails(serverDetails);
    }

    public static long store(String stringValue) {
        long hbaseKey = hbaseBackedMap.put(stringValue);
        return hbaseKey;
    }

    public static String read(Long key) {
        String storedStringValue = hbaseBackedMap.get(key);
        return storedStringValue;
    }
}