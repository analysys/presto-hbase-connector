package com.analysys.presto.connector.hbase.query;

import com.analysys.presto.connector.hbase.meta.HBaseColumnHandle;
import com.analysys.presto.connector.hbase.schedule.HBaseSplit;
import com.analysys.presto.connector.hbase.utils.Constant;
import com.analysys.presto.connector.hbase.utils.Utils;
import io.airlift.log.Logger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.net.InetAddress;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * 使用rawCell api获取字段值，统计api性能，他的耗时比getValue快20%左右，但是在组件中性能提升不明显
 * HBase record cursor fetch record in split, scan
 * Don't jump young blood.
 * Created by wupeng on 2018/1/19.
 */
class HBaseScanRecordCursorClientSideNg extends HBaseRecordCursor {

    private static final Logger log = Logger.get(HBaseScanRecordCursorClientSideNg.class);

    private Iterator iterator;

    private Connection connection = null;

    HBaseScanRecordCursorClientSideNg(List<HBaseColumnHandle> columnHandles, HBaseSplit hBaseSplit,
                                      ClientSideRegionScanner scanner, Map<Integer, HBaseColumnHandle> fieldIndexMap, Connection connection) {
        this.startTime = System.currentTimeMillis();
        this.columnHandles = columnHandles;
        this.fieldIndexMap = fieldIndexMap;
        this.rowKeyColName = requireNonNull(hBaseSplit.getRowKeyName(),
                "RowKeyName cannot be null if you want to query by RowKey");

        /*long printId = getPrintId();
        for (int i = 0; i < this.columnHandles.size(); i++) {
            log.info("printId=" + printId
                    + ", columnHandleInfo[ " + i + " ]:" + columnHandles.get(i).toString());
        }*/

        this.split = hBaseSplit;
        try {
            if (scanner != null)
                this.iterator = scanner.iterator();

            this.connection = connection;
        } catch (Exception ex) {
            log.error(ex, ex.getMessage());
            this.close();
        }
        this.totalBytes = 0L;
    }

    public boolean advanceNextPosition() {
        /*if (recordCount <= 0) {
            log.info("advanceNextPosition: " + this.split.toSimpleString2());
        }*/
        try {
            Result record = this.getNextRecord();
            if (record == null) {
                InetAddress localhost = InetAddress.getLocalHost();
                // 随机打印，减少Split运行结果的打印数量
                // if (System.currentTimeMillis() % SYSTEMOUT_INTERVAL >= 0)
                if (System.currentTimeMillis() % Constant.SYSTEMOUT_INTERVAL >= 0)
                    log.info("SCAN RECORD. advanceNextPosition: tableName=" + split.getTableName() + ", startRow=" + split.getStartRow()
                            + ", endRow=" + split.getEndRow() + ". READ_DATA_TIME="
                            + (System.currentTimeMillis() - startTime) + " mill secs. recordCount=" + recordCount
                            + ", startTime=" + new Date(startTime).toString() + ", localhost=" + localhost.getHostAddress()
                            + ", 指定运行的节点ip: "
                            + (split.getAddresses().size() > 0 ? split.getAddresses().get(0).toString() : ""));
                return false;
            } else {
                // columnHandles will be empty if sql is like count(*)
                if (columnHandles.isEmpty()) {
                    return true;
                }

                fields = new Object[this.columnHandles.size()];
                ordinalPositionAndFieldsIndexMap.clear();
                int fieldIndex = 0;
                for (Cell cell : record.rawCells()) {
                    // Fetch the index and type of column by column name
                    HBaseColumnHandle hch = fieldIndexMap.get(Bytes.toString(
                            Utils.arrayCopy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())).hashCode());
                    if (hch == null) {
                        continue;
                    }

                    // Set value to fields array
                    fields[fieldIndex] = matchValue(hch.getColumnType(),
                            Utils.arrayCopy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    // Store values in an array for queries
                    ordinalPositionAndFieldsIndexMap.put(hch.getOrdinalPosition(), fieldIndex);
                    fieldIndex++;
                }
                // Handle the value of rowKey column
                setRowKeyValue2FieldsAry(record, fieldIndex);
                return true;
            }
        } catch (Exception ex) {
            log.error(ex, ex.getMessage());
            this.close();
            return false;
        }
    }

    private Result getNextRecord() {
        try {
            if (iterator == null || !iterator.hasNext()) {
                return null;
            } else {
                recordCount++;
                return (Result) iterator.next();
            }
        } catch (Exception ex) {
            if (!ex.getMessage().contains("InterrupedIOException")) {
                log.error(ex, ex.getMessage());
            }
            this.close();
            return null;
        }
    }

    public void close() {
        if (this.connection != null)
            try {
                this.connection.close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
    }

    public long getCompletedBytes() {
        return this.totalBytes;
    }

    public long getReadTimeNanos() {
        return 0L;
    }

}
