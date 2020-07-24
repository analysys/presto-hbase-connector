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
import com.google.common.base.Preconditions;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.analysys.presto.connector.hbase.utils.Constant.SYSTEMOUT_INTERVAL;
import static com.analysys.presto.connector.hbase.utils.Utils.arrayCopy;
import static java.util.Objects.requireNonNull;

/**
 * Use record.rawCells() api to loop column value, this is 20% faster than result.getValue
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseGetRecordCursor extends HBaseRecordCursor {

    private static final Logger log = Logger.get(HBaseGetRecordCursor.class);

    private Connection connection;

    private int currentRecordIndex = 0;

    private Result[] results = null;

    HBaseGetRecordCursor(List<HBaseColumnHandle> columnHandles, HBaseSplit hBaseSplit,
                         Map<Integer, HBaseColumnHandle> fieldIndexMap, Connection connection) {
        startTime = System.currentTimeMillis();
        this.columnHandles = columnHandles;
        this.fieldIndexMap = fieldIndexMap;

        this.rowKeyColName = requireNonNull(hBaseSplit.getRowKeyName(),
                "RowKeyName cannot be null if you want to query by RowKey");

        this.split = hBaseSplit;
        this.connection = connection;
        try (Table table = connection.getTable(
                TableName.valueOf(hBaseSplit.getSchemaName() + ":" + hBaseSplit.getTableName()))) {
            List<String> rowKeys = hBaseSplit.getConstraint().stream()
                    .map(cond -> (String) cond.getValue()).collect(Collectors.toList());
            this.results = getResults(rowKeys, table);
        } catch (Exception e) {
            log.error(e, e.getMessage());
            this.close();
        }
        this.totalBytes = 0L;
    }

    private Result[] getResults(List<String> rowKeys, Table table) {
        List<Get> gets = rowKeys.stream().map(rowKey -> {
                    Get get = new Get(Bytes.toBytes(rowKey));
                    for (ColumnHandle ch : columnHandles) {
                        HBaseColumnHandle hch = (HBaseColumnHandle) ch;
                        // RowKey column has no column family, so we don't need to do get.addColumn() here.
                        if (this.split.getRowKeyName() != null
                                && this.split.getRowKeyName().equals(hch.getColumnName())) {
                            continue;
                        }
                        get.addColumn(Bytes.toBytes(hch.getFamily()), Bytes.toBytes(hch.getColumnName()));
                    }
                    return get;
                }
        ).collect(Collectors.toList());

        try {
            return table.get(gets);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public boolean advanceNextPosition() {
        String colName = null;
        try {
            // if we got error when reading data, return false to end this reading.
            if (results == null) {
                return false;
            } else if (this.currentRecordIndex >= this.results.length) {
                InetAddress localhost = InetAddress.getLocalHost();
                // Random printing
                if (System.currentTimeMillis() % SYSTEMOUT_INTERVAL == 0) {
                    log.info("BATCH GET RECORD. tableName=" + split.getTableName()
                            + ", rowKey_0=" + split.getConstraint().get(0) + ", READ_DATA_TIME="
                            + (System.currentTimeMillis() - startTime) + " mill secs. recordCount=" + recordCount
                            + ", startTime=" + new Date(startTime).toString() + ", localhost=" + localhost.getHostAddress()
                            + ", specified worker ip: "
                            + (split.getAddresses().size() > 0 ? split.getAddresses().get(0).toString() : ""));
                }
                return false;
            } else {
                fields = new Object[this.columnHandles.size()];
                ordinalPositionAndFieldsIndexMap.clear();
                int fieldIndex = 0;

                Result record = this.results[this.currentRecordIndex];

                for (Cell cell : record.rawCells()) {
                    colName = Bytes.toString(
                            arrayCopy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    HBaseColumnHandle hch = fieldIndexMap.get(colName.hashCode());
                    if (hch == null) {
                        continue;
                    }
                    Object value = matchValue(hch.getColumnType(),
                            arrayCopy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    fields[fieldIndex] = value;
                    ordinalPositionAndFieldsIndexMap.put(hch.getOrdinalPosition(), fieldIndex);
                    fieldIndex++;
                }

                // Handle the value of rowKey column
                setRowKeyValue2FieldsAry(record, fieldIndex);

                this.currentRecordIndex++;
                return true;
            }
        } catch (Exception ex) {
            log.error(ex, ex.getMessage());
            this.close();
            log.error("fieldIndexMap.size=" + fieldIndexMap.size() + ", ERROR ColName=" + colName);
            fieldIndexMap.forEach((cName, columnHandle) ->
                    log.error("fieldIndexMap: key=" + cName + ", hch.toString=" + columnHandle.toString())
            );
            return false;
        }
    }

    @Override
    public void close() {
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

    @Override
    public Type getType(int field) {
        Preconditions.checkArgument(field < this.columnHandles.size(), "Invalid field index");
        return this.columnHandles.get(field).getColumnType();
    }

}
