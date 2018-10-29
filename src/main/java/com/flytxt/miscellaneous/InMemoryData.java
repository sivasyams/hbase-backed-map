package com.flytxt.miscellaneous;

public class InMemoryData {

    private static HbaseBackedMap hbaseBackedMap;

    public static void setRedisContainerDetails(String redisServerIPAndPort) {
        hbaseBackedMap = new HbaseBackedMap(redisServerIPAndPort);
    }

    public static long put(String key) {
        long hbaseKey = hbaseBackedMap.put(key);
        return hbaseKey;
    }

    public static void remove(String key) {
        hbaseBackedMap.remove(key);
    }
}