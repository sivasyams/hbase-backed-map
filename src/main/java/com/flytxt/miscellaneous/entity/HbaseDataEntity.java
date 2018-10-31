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

    public HbaseDataEntity(Long key, String value) {
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

    public Long getKeyAsLong() {
        return Bytes.toLong(key);
    }

    public String getValueAsString() {
        return Bytes.toString(value);
    }
}