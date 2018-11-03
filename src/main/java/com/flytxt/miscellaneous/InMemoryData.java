package com.flytxt.miscellaneous;

public class InMemoryData {

    private static HbaseBackedMap hbaseBackedMap = new HbaseBackedMap();

    public static void setRedisLock(String serverDetails) {
        hbaseBackedMap.setRedisLock(serverDetails);
    }

    public static long store(String stringValue) {
        long hbaseKey = hbaseBackedMap.put(stringValue);
        return hbaseKey;
    }

    public static void remove(Long key) {
        hbaseBackedMap.remove(key);
    }

    public static String read(Long key) {
        String storedStringValue = hbaseBackedMap.get(key);
        return storedStringValue;
    }
}