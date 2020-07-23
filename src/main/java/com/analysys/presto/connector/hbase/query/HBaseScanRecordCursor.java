/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.analysys.presto.connector.hbase.query;

import com.analysys.presto.connector.hbase.meta.HBaseColumnHandle;
import com.analysys.presto.connector.hbase.schedule.HBaseSplit;
import io.airlift.log.Logger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import java.net.InetAddress;
import java.util.*;

import static com.analysys.presto.connector.hbase.utils.Constant.SYSTEMOUT_INTERVAL;
import static com.analysys.presto.connector.hbase.utils.Utils.arrayCopy;
import static java.util.Objects.requireNonNull;

/**
 * Use record.rawCells() api to loop column value, this is 20% faster than result.getValue
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseScanRecordCursor extends HBaseRecordCursor {

    private static final Logger log = Logger.get(HBaseScanRecordCursor.class);

    private ResultScanner resultScanner;
    private Iterator iterator;

    private Connection connection = null;

    HBaseScanRecordCursor(List<HBaseColumnHandle> columnHandles, HBaseSplit hBaseSplit,
                          ResultScanner scanner, Map<Integer, HBaseColumnHandle> fieldIndexMap, Connection connection) {
        this.startTime = System.currentTimeMillis();
        this.columnHandles = columnHandles;
        this.fieldIndexMap = fieldIndexMap;

        this.rowKeyColName = requireNonNull(hBaseSplit.getRowKeyName(),
                "RowKeyName cannot be null if you want to query by RowKey");

        this.split = hBaseSplit;
        try {
            this.resultScanner = scanner;
            if (resultScanner != null) {
                this.iterator = resultScanner.iterator();
            }

            this.connection = connection;
        } catch (Exception ex) {
            log.error(ex, ex.getMessage());
            this.close();
        }
        this.totalBytes = 0L;
    }

    @Override
    public boolean advanceNextPosition() {
        try {
            Result record = this.getNextRecord();
            if (record == null) {
                InetAddress localhost = InetAddress.getLocalHost();
                // Random printing
                if (System.currentTimeMillis() % SYSTEMOUT_INTERVAL == 0) {
                    log.info("SCAN RECORD. advanceNextPosition: tableName=" + split.getTableName() + ", startRow=" + split.getStartRow()
                            + ", endRow=" + split.getEndRow() + ". READ_DATA_TIME="
                            + (System.currentTimeMillis() - startTime) + " mill secs. recordCount=" + recordCount
                            + ", startTime=" + new Date(startTime).toString() + ", localhost=" + localhost.getHostAddress()
                            + ", specified worker ip: "
                            + (split.getAddresses().size() > 0 ? split.getAddresses().get(0).toString() : ""));
                }
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
                            arrayCopy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())).hashCode());
                    if (hch == null) {
                        continue;
                    }

                    // Set value to fields array
                    fields[fieldIndex] = matchValue(hch.getColumnType(),
                            arrayCopy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
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
            log.error(ex, ex.getMessage());
            this.close();
        }
        return null;
    }

    @Override
    public void close() {
        if (this.resultScanner != null) {
            try {
                this.resultScanner.close();
            } catch (Exception e) {
                log.warn(e.getMessage(), e);
            }
        }

        if (this.connection != null) {
            try {
                this.connection.close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public long getCompletedBytes() {
        return this.totalBytes;
    }

    @Override
    public long getReadTimeNanos() {
        return 0L;
    }

}
