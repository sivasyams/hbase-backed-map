package com.flytxt.miscellaneous.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.flytxt.miscellaneous.entity.HbaseDataEntity;

/**
 * The HBaseDataInteractor class
 *
 * @author sivasyam
 *
 */
public abstract class HBaseDataInteractor {

    private Scan hbaseScanner;

    private Configuration hbaseConfig;

    private HBaseAdmin hbaseAdmin;

    private HTable hbaseTable;

    private static final String TABLE_NAME = "SUBSCRIBER_STRINGS";

    private static final String COLUMN_FAMILY = "S";

    private static final String COLUMN_NAME = "stringValue";

    protected HBaseDataInteractor() {
        try {
            hbaseConfig = HBaseConfiguration.create();
            hbaseAdmin = new HBaseAdmin(hbaseConfig);
            if (!hbaseAdmin.tableExists(TABLE_NAME)) {
                HTableDescriptor hbaseTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                hbaseTableDescriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
                hbaseAdmin.createTable(hbaseTableDescriptor);
            }
            hbaseTable = new HTable(hbaseConfig, TABLE_NAME);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void putDataToHbase(HbaseDataEntity hbaseDataEntity) throws IOException {
        Put putOperation = new Put(hbaseDataEntity.getKeyAsByte());
        putOperation.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME), hbaseDataEntity.getValueAsByte());
        hbaseTable.put(putOperation);
    }

    protected HbaseDataEntity getDataFromHBase(byte[] key) throws IOException {
        HbaseDataEntity hbaseDataEntity = null;
        Get getOperation = new Get(key);
        getOperation.addFamily(Bytes.toBytes(COLUMN_FAMILY));
        Result result = hbaseTable.get(getOperation);
        byte[] rowValue = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME));
        hbaseDataEntity = new HbaseDataEntity(key, rowValue);
        return hbaseDataEntity;
    }

    protected void removeDataFromHbase(byte[] key) throws IOException {
        Delete deleteOperation = new Delete(key);
        deleteOperation.deleteColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME));
        hbaseTable.delete(deleteOperation);
    }

    protected HbaseDataEntity getLastRowData() throws IOException {
        hbaseScanner = new Scan();
        hbaseScanner.setReversed(true);
        ResultScanner hbaseResultScanner = hbaseTable.getScanner(hbaseScanner);
        Result scannedResult = hbaseResultScanner.next();
        hbaseResultScanner.close();
        if (scannedResult != null) {
            byte[] rowKey = scannedResult.getRow();
            byte[] rowValue = scannedResult.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME));
            HbaseDataEntity hbaseDataEntity = new HbaseDataEntity(rowKey, rowValue);
            return hbaseDataEntity;
        } else {
            return null;
        }
    }

    protected HbaseDataEntity scanHbaseForEntity(byte[] entityValue) throws IOException {
        HbaseDataEntity hbaseDataEntity = null;
        SingleColumnValueFilter hbaseScanFilter = new SingleColumnValueFilter(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME), CompareOp.EQUAL, entityValue);
        hbaseScanner = new Scan();
        hbaseScanner.setReversed(false);
        hbaseScanner.setFilter(hbaseScanFilter);
        hbaseScanner.addFamily(Bytes.toBytes(COLUMN_FAMILY));
        hbaseScanner.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME));
        ResultScanner filteredResultScanner = hbaseTable.getScanner(hbaseScanner);
        for (Result filteredResult : filteredResultScanner) {
            byte[] filteredRowKey = filteredResult.getRow();
            byte[] filteredRowValue = filteredResult.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME));
            hbaseDataEntity = new HbaseDataEntity(filteredRowKey, filteredRowValue);
        }
        return hbaseDataEntity;
    }
}