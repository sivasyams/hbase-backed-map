package com.flytxt.miscellaneous.entity;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * The HbaseDataEntity class
 *
 * @author sivasyam
 *
 */
public class HbaseDataEntity {

    private byte[] key;

    private byte[] value;

    public HbaseDataEntity(String key, Long value) {
        this.key = Bytes.toBytes(key);
        this.value = Bytes.toBytes(value);
    }

    public HbaseDataEntity(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKeyAsByte() {
        return key;
    }

    public byte[] getValueAsByte() {
        return value;
    }

    public String getKeyAsString() {
        return Bytes.toString(key);
    }

    public Long getValueAsLong() {
        return Bytes.toLong(value);
    }
}