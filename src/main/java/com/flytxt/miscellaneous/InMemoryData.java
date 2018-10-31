package com.flytxt.miscellaneous;

public class InMemoryData {

    private static HbaseBackedMap hbaseBackedMap;

    public static void setRedisContainerDetails(String redisServerIPAndPort) {
        hbaseBackedMap = new HbaseBackedMap(redisServerIPAndPort);
    }

    public static long managedStore(String stringValue) {
        long hbaseKey = hbaseBackedMap.put(stringValue);
        return hbaseKey;
    }

    public static void store(Long key, String value) {
        hbaseBackedMap.put(key, value);
    }

    public static void remove(Long key) {
        hbaseBackedMap.remove(key);
    }

    public static String read(Long key) {
        String storedStringValue = hbaseBackedMap.get(key);
        return storedStringValue;
    }
}