package com.analysys.presto.connector.hbase.query;

import com.analysys.presto.connector.hbase.meta.HBaseColumnHandle;
import com.analysys.presto.connector.hbase.schedule.HBaseSplit;
import com.analysys.presto.connector.hbase.utils.Constant;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.*;
import com.google.common.base.Preconditions;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.analysys.presto.connector.hbase.utils.Constant.*;
import static com.analysys.presto.connector.hbase.utils.Utils.arrayCopy;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;

/**
 * HBase record cursor fetch record in split
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseRecordCursor implements RecordCursor {

    private static final Logger log = Logger.get(HBaseRecordCursor.class);
    private static final int MAX_BLOCK_SIZE = 1024 * 1024;

    List<HBaseColumnHandle> columnHandles;

    long totalBytes;
    Object[] fields;
    Map<Integer, Integer> ordinalPositionAndFieldsIndexMap = new HashMap<>();
    Map<Integer, HBaseColumnHandle> fieldIndexMap = null;

    public HBaseSplit split;

    long startTime = 0;
    long recordCount = 0;

    String rowKeyColName = null;

    HBaseRecordCursor() {
    }

    @Override
    public boolean advanceNextPosition() {
        throw new UnsupportedOperationException();
    }

    /**
     * We store the column value in HBase like Bytes.toBytes(value) rather than Bytes.toBytes(value.toString)
     *
     * @param type  type
     * @param value value
     * @return object
     */
    Object matchValue(Type type, byte[] value) {
        if (type == null) {
            return Bytes.toString(value);
        }

        Class typeClass = type.getClass();
        // return null if value is null
        // || (For varchar type if length is 0, return value is Bytes.toString(value), this will return "")
        // beside varchar type if length is 0, the value will be null
        if (value == null) {
            return null;
        }
        if (value.length == 0 && !typeClass.equals(VARCHAR_CLASS)) {
            return null;
        }

        if (typeClass.equals(VARCHAR_CLASS)) {
            return Bytes.toString(value);
        } else if (typeClass.equals(INTEGER_CLASS)) {
            return Bytes.toInt(value);
        } else if (typeClass.equals(BIGINT_CLASS)) {
            return Bytes.toLong(value);
        } else if (typeClass.equals(DOUBLE_CLASS)) {
            return Bytes.toDouble(value);
        } else if (typeClass.equals(TIMESTAMP_CLASS)) {
            return Bytes.toLong(value);
        } else if (typeClass.equals(BOOLEAN_CLASS)) {
            // 0: false, 1: true
            return Bytes.toInt(value);
        } else if (type.getClass().getSuperclass().equals(DecimalType.class)) {
            return Bytes.toBigDecimal(value);
        } else {
            return Bytes.toString(value);
        }
    }

    private Object getFieldValue(int field) {
        if (ordinalPositionAndFieldsIndexMap.containsKey(columnHandles.get(field).getOrdinalPosition())) {
            return fields[ordinalPositionAndFieldsIndexMap.get(columnHandles.get(field).getOrdinalPosition())];
        } else {
            return null;
            /*log.error("Cannot find the value of field index " + field
                    + ", field array is " + Arrays.toString(fields)
                    + ", split=" + split.toString());
            return "ERROR_VALUE";*/
        }
    }

    /**
     * 0: false, 1: true
     *
     * @param field field index
     * @return boolean
     */
    @Override
    public boolean getBoolean(int field) {
        this.checkFieldType(field, BooleanType.BOOLEAN);
        return (Integer) this.getFieldValue(field) == 1;
    }

    @Override
    public long getLong(int field) {
        Type type = this.getType(field);
        Object value = this.getFieldValue(field);
        Class typeClass = type.getClass();
        if (typeClass.equals(INTEGER.getClass())) {
            return (Integer) value;
        } else if (typeClass.equals(SmallintType.SMALLINT.getClass())) {
            return (Short) value;
        }
        // We don't support timestamp at current version, we use long instead of timestamp
        // so this elseif could be removed now
        else if (typeClass.equals(TIMESTAMP.getClass())) {
            try {
                return (Long) value;
            } catch (Exception e) {
                log.error(e, e.getMessage());
                this.close();
                return 0;
            }
        }
        // When the precision of decimal <= 18(Type is ShortDecimalType) getLong method will be executed.
        else if (typeClass.getSuperclass().equals(DecimalType.class)) {
            DecimalType dType = (DecimalType) type;
            if (dType.isShort()) {
                return ((BigDecimal) value).unscaledValue().longValue();
            } else {
                return ((BigDecimal) value).unscaledValue().longValue();
            }
        } else {
            return (Long) value;
        }
    }

    @Override
    public double getDouble(int field) {
        this.checkFieldType(field, DOUBLE);
        return (Double) this.getFieldValue(field);
    }

    @Override
    public Slice getSlice(int field) {
        Object value = this.getFieldValue(field);
        if (value == null) {
            return null;
        }
        Type type = this.getType(field);
        if (type instanceof VarcharType) {
            return Slices.utf8Slice((String) value);
        } else if (type instanceof VarbinaryType) {
            return Slices.utf8Slice(value.toString());
        } else if (type instanceof DecimalType) {
            BigDecimal dec = (BigDecimal) value;
            return Decimals.encodeScaledValue(dec);
        } else {
            throw new IllegalStateException("getSlice not implemented for " + type
                    + ", type.class=" + type.getClass() + ", value=" + value);
        }
    }

    @Override
    public Object getObject(int field) {
        Object fieldValue;
        Type type;
        PageBuilderStatus pageBuilderStatus = new PageBuilderStatus(MAX_BLOCK_SIZE);
        type = this.getType(field);
        fieldValue = this.getFieldValue(field);
        if (fieldValue == null || "".equals(fieldValue)) {
            return this.getType(field).createBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), 0).build();
        }

        if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY)) {
            String[] values = ((String) fieldValue).split(Constant.ARRAY_STRING_SPLITTER);
            Type elementType = type.getTypeParameters().get(0);
            BlockBuilder builder = elementType.createBlockBuilder(null, values.length);
            for (String value : values) {
                if (value != null && !"".equals(value)) {
                    TypeUtils.writeNativeValue(elementType, builder, value);
                }
            }
            return builder.build();
        } else {
            throw new UnsupportedOperationException("OOPS！UNSUPPORTED TYPE：" + type.getDisplayName());
        }
    }

    /**
     * Define what kind of value will be shown as NULL
     *
     * @param field field index of columnHandles
     * @return boolean
     */
    @Override
    public boolean isNull(int field) {
        Preconditions.checkArgument(field < this.columnHandles.size(), "Invalid field index");
        Object value = this.getFieldValue(field);
        return value == null;
    }

    private void checkFieldType(int field, Type expected) {
        Type actual = this.getType(field);
        Preconditions.checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field,
                expected, actual);
    }

    public void setRowKeyValue2FieldsAry(Result record, int fieldIndex) {
        // Handle the value of rowKey
        // Check out whether columns to be queried contain rowKey field
        if (fieldIndexMap.containsKey(this.rowKeyColName.hashCode())) {
            if (record.rawCells() != null && record.rawCells().length > 0) {
                Cell cell = record.rawCells()[0];
                HBaseColumnHandle rowKeyHandle = fieldIndexMap.get(rowKeyColName.hashCode());
                String rowKeyValue = Bytes.toString(
                        arrayCopy(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                fields[fieldIndex] = rowKeyValue;
                ordinalPositionAndFieldsIndexMap.put(rowKeyHandle.getOrdinalPosition(), fieldIndex);
            }
        }
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("WARNING! You haven't achieve close() method yet, " +
                "this can lead to a leakage of connection.");
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
