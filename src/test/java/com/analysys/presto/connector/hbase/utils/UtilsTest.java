package com.analysys.presto.connector.hbase.utils;

import com.analysys.presto.connector.hbase.schedule.ConditionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class UtilsTest {

  @Test
  public void testAddZeroPrefix() {
    Assert.assertEquals("fooBar", Utils.addZeroPrefix("fooBar", 0));
    Assert.assertEquals("0fooBar", Utils.addZeroPrefix("fooBar", 1));
  }

  @Test
  public void testArrayCopy() {
    Assert.assertArrayEquals(new byte[]{8, 16}, Utils.arrayCopy(new byte[]{1, 2, 4, 8, 16, 32}, 3, 2));
  }

  @Test
  public void testIsBatchGetReturnFalse() {
    final ArrayList<ConditionInfo> conditions = new ArrayList<>();
    final ConditionInfo conditionInfo = new ConditionInfo("fooBar", null, null, null);
    conditions.add(conditionInfo);

    Assert.assertFalse(Utils.isBatchGet(conditions, "fooBar"));
  }

  @Test
  public void testIsBatchGetReturnTrue() {
    final ArrayList<ConditionInfo> conditions = new ArrayList<>();
    final ConditionInfo conditionInfo = new ConditionInfo("fooBar", Constant.CONDITION_OPER.EQ, null, null);
    conditions.add(conditionInfo);

    Assert.assertTrue(Utils.isBatchGet(conditions, "fooBar"));
  }

  /*@PrepareForTest({FileUtils.class, Utils.class})
  @Test
  public void testGetColumnMetaFromJson() throws Exception {
    PowerMockito.mockStatic(FileUtils.class);

    PowerMockito.when(FileUtils.readFileToString(Mockito.any(), Mockito.anyString())).thenReturn("{" +
            "\"columns\":[ " +
            "{\"family\": foo, \"columnName\": Bar, \"type\": \"fooBar\"}, " +
            "{\"family\": foo, \"columnName\": Bar, \"type\": \"string\"}, " +
            "{\"family\": foo, \"columnName\": Bar, \"type\": \"int\"}, " +
            "{\"family\": foo, \"columnName\": Bar, \"type\": \"bigint\"}, " +
            "{\"family\": foo, \"columnName\": Bar, \"type\": \"double\"}, " +
            "{\"family\": foo, \"columnName\": Bar, \"type\": \"boolean\"}, " +
            "{\"family\": foo, \"columnName\": Bar, \"type\": \"array<string>\"}, " +
            "{\"family\": foo, \"columnName\": Bar, \"type\": \"timestamp\"}, " +
            "{\"family\": foo, \"columnName\": Bar, \"type\": \"datetime\"}, " +
            "{\"family\": foo, \"columnName\": Bar, \"type\": \"number\"}" +
            "] }");

    Assert.assertArrayEquals(new Object[]{new HBaseColumnMetadata("foo", "Bar", VarcharType.VARCHAR, true),
            new HBaseColumnMetadata("foo", "Bar", VarcharType.VARCHAR, false),
            new HBaseColumnMetadata("foo", "Bar", IntegerType.INTEGER, false),
            new HBaseColumnMetadata("foo", "Bar", BigintType.BIGINT, false),
            new HBaseColumnMetadata("foo", "Bar", DoubleType.DOUBLE, false),
            new HBaseColumnMetadata("foo", "Bar", BooleanType.BOOLEAN, false),
            new HBaseColumnMetadata("foo", "Bar", new ArrayType(VarcharType.VARCHAR), false),
            new HBaseColumnMetadata("foo", "Bar", TimestampType.TIMESTAMP, false),
            new HBaseColumnMetadata("foo", "Bar", TimestampType.TIMESTAMP, false),
            new HBaseColumnMetadata("foo", "Bar", DecimalType.createDecimalType(DECIMAL_DEFAULT_PRECISION, DECIMAL_DEFAULT_SCALE), false)
    }, Utils.getColumnMetaFromJson("a","b","c").toArray());
  }*/
}
