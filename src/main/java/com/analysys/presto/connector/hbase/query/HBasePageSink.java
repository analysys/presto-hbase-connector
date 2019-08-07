package com.analysys.presto.connector.hbase.query;

import com.analysys.presto.connector.hbase.connection.HBaseClientManager;
import com.analysys.presto.connector.hbase.meta.HBaseExtendedTableHandle;
import com.analysys.presto.connector.hbase.meta.HBaseInsertTableHandle;
import com.analysys.presto.connector.hbase.utils.Utils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.VariableWidthBlock;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.analysys.presto.connector.hbase.utils.Constant.ARRAY_STRING_SPLITTER;
import static com.analysys.presto.connector.hbase.utils.Constant.SYSTEMOUT_INTERVAL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * write data to HBase
 *
 * @author wupeng
 * @date 2018/4/25.
 */
public class HBasePageSink implements ConnectorPageSink {

    private static final Logger log = Logger.get(HBasePageSink.class);

    private final List<Type> columnTypes;
    private final List<String> columnNames;
    private String schemaName = null;
    private String tableName = null;
    private HBaseClientManager clientManager;
    private final int rowKeyColumnChannel;
    private final Map<String, String> colNameAndFamilyNameMap;

    public HBasePageSink(ConnectorSession connectorSession, HBaseClientManager clientManager,
                         HBaseExtendedTableHandle extendedTableHandle) {
        requireNonNull(clientManager, "clientManager is null");
        this.columnTypes = extendedTableHandle.getColumnTypes();
        this.columnNames = extendedTableHandle.getColumnNames();

        this.clientManager = clientManager;
        HBaseInsertTableHandle insertTableHandle = (HBaseInsertTableHandle) extendedTableHandle;
        this.rowKeyColumnChannel = insertTableHandle.getRowKeyColumnChannel();
        this.colNameAndFamilyNameMap = insertTableHandle.getColNameAndFamilyNameMap();

        try {
            this.tableName = extendedTableHandle.getSchemaTableName().getTableName();
            this.schemaName = extendedTableHandle.getSchemaTableName().getSchemaName();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page) {
        long startTime = System.currentTimeMillis();
        List<Put> puts = new ArrayList<>(10000);
        String rowKey = null;
        try (Connection connection = this.clientManager.createConnection();
             Table table = connection.getTable(TableName.valueOf(schemaName + ":" + tableName))) {

            for (int position = 0; position < page.getPositionCount(); position++) {
                rowKey = getRowKeyByChannel(page, this.rowKeyColumnChannel, position);
                Put put = new Put(Bytes.toBytes(rowKey));
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    // The value of rowKey has been planted in object Put already,
                    // so we don't need to append it here.
                    // if (this.tableMetaInfo.getColumns().get(channel).isIsRowKey()) {
                    if (channel == rowKeyColumnChannel) {
                        continue;
                    }
                    appendColumnValue(put, page, position, channel, channel);
                }
                puts.add(put);

                if (puts.size() >= 10000) {
                    table.put(puts);
                    puts.clear();
                }
            }

            table.put(puts);

            if (System.currentTimeMillis() % SYSTEMOUT_INTERVAL == 0)
                log.info("INSERT DATA. StartTime=" + new Date(startTime).toString()
                        + ", used " + (System.currentTimeMillis() - startTime)
                        + " million seconds, pageCount=" + page.getPositionCount() + ", rowKey=" + rowKey
                        + ", table=" + schemaName + ":" + tableName);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return NOT_BLOCKED;
    }

    /**
     * Find the value of RowKey column by channel.
     *
     * @param page     page
     * @param channel  rowKey channel
     * @param position position
     * @return rowKey
     */
    private String getRowKeyByChannel(Page page, int channel, int position) {
        Preconditions.checkState(channel >= 0,
                "You must specify ROW_KEY column for Table %s.%s in your .json file.",
                schemaName, tableName);
        Block block = page.getBlock(channel);
        return columnTypes.get(channel).getSlice(block, position).toStringUtf8();
    }

    private void appendColumnValue(Put put, Page page, int position, int channel, int destChannel) {
        Block block = page.getBlock(channel);
        Type type = columnTypes.get(destChannel);
        String columnName = columnNames.get(destChannel);

        String columnFamilyName = this.colNameAndFamilyNameMap.get(columnNames.get(destChannel));

        // get value, add to Put
        if (block.isNull(position)) {
            // row.setNull(destChannel);
            return;
        } else if (TIMESTAMP.equals(type)) {
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName),
                    Bytes.toBytes(type.getLong(block, position)));
        } else if (BIGINT.equals(type)) {
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName),
                    Bytes.toBytes(type.getLong(block, position)));
        } else if (INTEGER.equals(type)) {
            int intValue = ((Long) type.getLong(block, position)).intValue();
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName),
                    Bytes.toBytes(intValue));
        } else if (BOOLEAN.equals(type)) {
            int intValue = ((Long) type.getLong(block, position)).intValue();
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName),
                    Bytes.toBytes(intValue));
        } else if (DOUBLE.equals(type)) {
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName),
                    Bytes.toBytes(type.getDouble(block, position)));
        } else if (type.getClass().getSuperclass().equals(DecimalType.class)) {
            BigDecimal value = ((SqlDecimal) type.getObjectValue(null, block, position))
                    .toBigDecimal();

            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName),
                    Bytes.toBytes(value));
        } else if (isVarcharType(type)) {
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName),
                    Bytes.toBytes(type.getSlice(block, position).toStringUtf8()));
        }
        // We only support Array<String>
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY)) {
            Object obj = type.getObject(block, position);
            Block vBlock;
            if (obj instanceof VariableWidthBlock)
                vBlock = (VariableWidthBlock) obj;
            else
                vBlock = (DictionaryBlock) obj;
            StringBuilder buff = new StringBuilder();
            for (int i = 0; i < vBlock.getPositionCount(); i++) {
                Slice slice = vBlock.getSlice(i, 0, vBlock.getSliceLength(i));
                String value = slice.toStringUtf8();
                if (i > 0)
                    buff.append(ARRAY_STRING_SPLITTER);
                buff.append(Utils.removeExtraSpaceInArrayString(value));
            }
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName),
                    Bytes.toBytes(buff.toString()));
        } else {
            throw new UnsupportedOperationException("Type is not supported: " + type);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        closeSession();
        // the committer does not need any additional info.
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {
        closeSession();
    }

    private void closeSession() {
    }

}
