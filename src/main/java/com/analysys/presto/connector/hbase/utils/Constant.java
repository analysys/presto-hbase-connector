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
package com.analysys.presto.connector.hbase.utils;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * Constant
 *
 * @author wupeng
 * @date 2019/01/29
 */
public interface Constant {
    public final String ARRAY_STRING_SPLITTER = "\001";
    public final String COMMA = ",";
    public final String ROWKEY_SPLITER = "\001";

    public final int BATCHGET_SPLIT_RECORD_COUNT = 20;
    public final int BATCHGET_SPLIT_MAX_COUNT = 30;

    /**
     * DecimalType(DECIMAL_DEFAULT_PRECISION, DECIMAL_DEFAULT_SCALE)
     */
    public static final int DECIMAL_DEFAULT_PRECISION = 18;
    public static final int DECIMAL_DEFAULT_SCALE = 3;

    public static final int SYSTEMOUT_INTERVAL = 40;

    public static final String CONNECTOR_NAME = "hbase";

    enum CONDITION_OPER {
        // bigger
        GT,
        // less
        LT,
        // equal
        EQ,
        // greater than or equal to
        GE,
        // less than or equal to
        LE
    }

    static final Class VARCHAR_CLASS = VARCHAR.getClass();
    static final Class INTEGER_CLASS = INTEGER.getClass();
    static final Class BIGINT_CLASS = BIGINT.getClass();
    static final Class DOUBLE_CLASS = DOUBLE.getClass();
    static final Class TIMESTAMP_CLASS = TIMESTAMP.getClass();
    static final Class BOOLEAN_CLASS = BOOLEAN.getClass();

    static final String DEFAULT_HBASE_NAMESPACE_NAME = "default";
    static final String TABLE_META_FILE_TAIL = ".json";

    static final String JSON_TABLEMETA_COLUMNES = "columns";
    static final String JSON_TABLEMETA_FAMILY = "family";
    static final String JSON_TABLEMETA_COLUMNNAME = "columnName";
    static final String JSON_TABLEMETA_TYPE = "type";
    static final String JSON_TABLEMETA_ISROWKEY = "isRowKey";

    static final String JSON_ENCODING_UTF8 = "UTF-8";

    static final String ROWKEY_TAIL = "|";

    public static final String HBASE_NAMESPACE_DEFAULT = "default";

}
