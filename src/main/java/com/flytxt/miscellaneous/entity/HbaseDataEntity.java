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

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object objectToCheck) {
        HbaseDataEntity hbaseDataEntity = (HbaseDataEntity) objectToCheck;
        if (hbaseDataEntity.getKeyAsLong() == this.getKeyAsLong()) {
            return true;
        } else {
            return true;
        }
    }

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