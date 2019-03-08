package com.analysys.presto.connector.hbase.utils;

import com.analysys.presto.connector.hbase.meta.HBaseColumnMetadata;
import com.analysys.presto.connector.hbase.meta.TableMetaInfo;
import com.analysys.presto.connector.hbase.schedule.ConditionInfo;
import com.facebook.presto.jdbc.internal.jackson.databind.ObjectMapper;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import java.io.File;
import java.util.List;

import static com.analysys.presto.connector.hbase.utils.Constant.*;

/**
 * utils
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class Utils {

    public static final Logger logger = Logger.get(Utils.class);

    /**
     * Read table json from metaDir by schema name and table name
     *
     * @param schemaName schema name
     * @param tableName  table name
     * @param metaDir    meta dir
     * @return json file content
     */
    private static String readTableJson(String schemaName, String tableName, String metaDir) {
        try {
            String tableMetaPath = metaDir + File.separator
                    + (schemaName == null || "".equals(schemaName) ? DEFAULT_HBASE_NAMESPACE_NAME : schemaName)
                    + File.separator + tableName + TABLE_META_FILE_TAIL;
            return FileUtils.readFileToString(new File(tableMetaPath), JSON_ENCODING_UTF8);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return "";
    }

    /**
     * Read table json from metaDir by schema name and table name.
     * And convert it to an Object of TableMetaInfo.
     *
     * @param schemaName schema name
     * @param tableName  table name
     * @param metaDir    meta info dir
     * @return Object of TableMetaInfo
     */
    public static TableMetaInfo getTableMetaInfoFromJson(String schemaName, String tableName, String metaDir) {
        long startTime = System.currentTimeMillis();
        try {
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = readTableJson(schemaName, tableName, metaDir);
            return mapper.readValue(jsonString, TableMetaInfo.class);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            logger.info(String.format("Read meta info of TABLE %s.%s from json, totally used %d ms.",
                    schemaName, tableName, (System.currentTimeMillis() - startTime)));
        }
        return null;
    }

    /**
     * Fetch column meta info from json file
     *
     * @param schemaName schema name
     * @param tableName  table name
     * @param metaDir    meta dir
     * @return list of column meta info
     */
    public static ImmutableList<ColumnMetadata> getColumnMetaFromJson(String schemaName, String tableName, String metaDir) {
        long startTime = System.currentTimeMillis();
        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        try {
            String jsonStr = readTableJson(schemaName, tableName, metaDir);
            JSONObject obj = new JSONObject(jsonStr);
            JSONArray cols = obj.getJSONArray(JSON_TABLEMETA_COLUMNES);
            for (int i = 0; i < cols.length(); i++) {
                JSONObject temp = new JSONObject(cols.getString(i));
                String family = temp.getString(JSON_TABLEMETA_FAMILY);
                String columnName = temp.getString(JSON_TABLEMETA_COLUMNNAME);
                String type = temp.getString(JSON_TABLEMETA_TYPE);
                columnsMetadata.add(new HBaseColumnMetadata(family, columnName, matchType(type)));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            logger.info(String.format("Read COLUMN meta info of TABLE %s.%s from json, totally used %d ms.",
                    schemaName, tableName, (System.currentTimeMillis() - startTime)));
        }
        return columnsMetadata.build();
    }

    /**
     * Add zero prefix to a salt string
     *
     * @param key       key
     * @param missCount missCount
     * @return standard salt value
     */
    public static String addZeroPrefix(String key, int missCount) {
        for (int j = missCount; j > 0; j--) {
            key = "0" + key;
        }
        return key;
    }

    /**
     * Find the presto type of column you configured in json file by type flag.
     *
     * @param type The type value that configured in json file.
     * @return type in presto
     */
    private static Type matchType(String type) {
        if (type == null) {
            return VarcharType.VARCHAR;
        }

        switch (type.toLowerCase()) {
            case "string":
                return VarcharType.VARCHAR;
            case "int":
                return IntegerType.INTEGER;
            case "bigint":
                return BigintType.BIGINT;
            case "double":
                return DoubleType.DOUBLE;
            case "boolean":
                return BooleanType.BOOLEAN;
            case "array<string>":
                return new ArrayType(VarcharType.VARCHAR);
            case "timestamp":
                return TimestampType.TIMESTAMP;
            case "datetime":
                return TimestampType.TIMESTAMP;
            case "number":
                return DecimalType.createDecimalType(DECIMAL_DEFAULT_PRECISION, DECIMAL_DEFAULT_SCALE);
            default:
                return VarcharType.VARCHAR;
        }
    }

    /**
     * Whether sql constraint contains conditions like "rowKey='xxx'" or "rowKey in ('xxx','xxx')"
     *
     * @return true if this sql is batch get.
     */
    public static boolean isBatchGet(List<ConditionInfo> conditions, String rowKeyColName) {
        if (conditions != null) {
            for (ConditionInfo cond : conditions) {
                if (rowKeyColName.equals(cond.getColName())
                        && cond.getOperator() == Constant.CONDITION_OPER.EQ) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Copy contents in ${srcAry} from position ${srcPos} for ${length} bytes.
     *
     * @param srcAry source array
     * @param srcPos start position
     * @param length length
     * @return copied byte array
     */
    public static byte[] arrayCopy(byte[] srcAry, int srcPos, int length) {
        byte[] destAry = new byte[length];
        System.arraycopy(srcAry, srcPos, destAry, 0, length);
        return destAry;
    }

}



