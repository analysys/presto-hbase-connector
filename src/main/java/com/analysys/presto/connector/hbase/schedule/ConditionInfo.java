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
package com.analysys.presto.connector.hbase.schedule;

import com.analysys.presto.connector.hbase.utils.Constant;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.type.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.math.BigDecimal;

import static io.prestosql.spi.type.IntegerType.INTEGER;

/**
 * Condition info
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class ConditionInfo {
    public static final Logger log = Logger.get(ConditionInfo.class);
    private final String colName;
    private final Object value;
    private final Constant.CONDITION_OPER operator;
    private final Type type;

    @JsonCreator
    public ConditionInfo(@JsonProperty("colName") String colName,
                         @JsonProperty("operator") Constant.CONDITION_OPER operator,
                         @JsonProperty("value") Object value,
                         @JsonProperty("type") Type type) {
        this.colName = colName;
        this.operator = operator;
        this.value = value;
        this.type = type;
    }

    @JsonProperty
    public String getColName() {
        return colName;
    }

    @JsonProperty
    public Constant.CONDITION_OPER getOperator() {
        return operator;
    }

    @JsonProperty
    public Object getValue() {
        return value;
    }

    @JsonProperty
    public Type getType() {
        return type;
    }

    public byte[] valueToBytes() {
        if (type.getClass().equals(BigintType.BIGINT.getClass())) {
            if (value.getClass().equals(Integer.class)) {
                return Bytes.toBytes(Long.valueOf(String.valueOf(value)));
            } else {
                return Bytes.toBytes((Long) value);
            }
        } else if (type.getClass().equals(IntegerType.INTEGER.getClass())) {
            return Bytes.toBytes((Integer) value);
        } else if (type.getClass().equals(SmallintType.SMALLINT.getClass())) {
            return Bytes.toBytes((Short) value);
        } else if (type.getClass().equals(DoubleType.DOUBLE.getClass())) {
            return Bytes.toBytes((Double) value);
        } else if (type.getClass().equals(VarcharType.VARCHAR.getClass())) {
            return Bytes.toBytes(String.valueOf(value));
        }
        // Boolean is stored as int value like 0:false 1:true.
        // So we have to convert it to int here.
        else if (type.getClass().equals(BooleanType.BOOLEAN.getClass())) {
            return Bytes.toBytes((Integer) value);
        } else if (type.getClass().getSuperclass().equals(DecimalType.class)) {
            if (value.getClass().equals(Integer.class)) {
                return Bytes.toBytes(new BigDecimal(String.valueOf((Integer) value / 1000.0))
                        .setScale(3, BigDecimal.ROUND_HALF_UP));
            } else {
                return Bytes.toBytes(new BigDecimal(String.valueOf((Long) value / 1000.0))
                        .setScale(3, BigDecimal.ROUND_HALF_UP));
            }
        } else {
            return Bytes.toBytes(String.valueOf(value));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConditionInfo that = (ConditionInfo) o;

        if (!colName.equals(that.colName)) {
            return false;
        }
        if (!value.equals(that.value)) {
            return false;
        }
        if (operator != that.operator) {
            return false;
        }

        return type.equals(that.type);
    }

    /**
     * change value to String type
     * Or else when you use value to make a startKey,
     * it's value maybe like '00-Slice{base=[B@1ddec907, address=16, length=3}'
     * @return value toString
     */
    String valueToString() {
        /*Type type = null;
        if (type == null)
            return "";*/
        if (type.getClass().equals(INTEGER.getClass())) {
            return String.valueOf(value);
        } else if (type.getClass().equals(SmallintType.SMALLINT.getClass())) {
            return String.valueOf(value);
        } else if (type.getClass().equals(VarcharType.VARCHAR.getClass())) {
            return ((Slice) value).toStringUtf8();
        } else if (type.getClass().equals(BooleanType.BOOLEAN.getClass())) {
            return String.valueOf(value);
        } else { // if (type.getClass().equals(BIGINT.getClass())) {
            return String.valueOf(value);
        }
    }

    @Override
    public int hashCode() {
        int result = colName.hashCode();
        result = 31 * result + value.hashCode();
        result = 31 * result + operator.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ConditionInfo{" +
                "colName='" + colName + '\'' +
                ", value=" + value +
                ", operator=" + operator +
                ", type=" + type +
                '}';
    }
}
