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
package com.analysys.presto.connector.hbase.meta;

import com.analysys.presto.connector.hbase.utils.Constant;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;

import java.util.List;

import static com.analysys.presto.connector.hbase.utils.Utils.isEmpty;

/**
 * Table meta info meta
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class TableMetaInfo {
    private String tableName = null;
    private String schemaName = null;
    private String rowKeyColName = null;
    /**
     * Define which columns rowKey consist of, columns are separated by ','
     */
    private String rowKeyFormat = null;
    private String rowKeySaltUpperAndLower = null;
    private String describe = null;
    private List<ColumnMetaInfo> columns = null;
    private String rowKeySeparator = null;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getDescribe() {
        return describe;
    }

    public void setDescribe(String describe) {
        this.describe = describe;
    }

    public String getRowKeySaltUpperAndLower() {
        return rowKeySaltUpperAndLower;
    }

    public void setRowKeySaltUpperAndLower(String rowKeySaltUpperAndLower) {
        this.rowKeySaltUpperAndLower = rowKeySaltUpperAndLower;
    }

    @JsonIgnore
    public int getRowKeyPrefixLower() {
        return this.rowKeySaltUpperAndLower == null ? -1 : Integer.valueOf(this.rowKeySaltUpperAndLower.split(",")[0]);
    }

    @JsonIgnore
    public int getRowKeyPrefixUpper() {
        return this.rowKeySaltUpperAndLower == null ? -1 : Integer.valueOf(this.rowKeySaltUpperAndLower.split(",")[1]);
    }

    public List<ColumnMetaInfo> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnMetaInfo> columns) {
        this.columns = columns;
    }

    public String getRowKeyFormat() {
        return rowKeyFormat;
    }

    public void setRowKeyFormat(String rowKeyFormat) {
        this.rowKeyFormat = rowKeyFormat;
    }

    @JsonIgnore
    public String getRowKeyColName() {
        if (isEmpty(rowKeyColName)) {
            for (ColumnMetaInfo c : this.columns) {
                if (c.isIsRowKey()) {
                    this.rowKeyColName = c.getColumnName();
                    break;
                }
            }
        }
        Preconditions.checkState(this.rowKeyColName != null,
                "Table %s.%s doesn't specified ROW_KEY column. Specify ROW_KEY in your .json file.", schemaName, tableName);
        return this.rowKeyColName;
    }

    public String getRowKeySeparator() {
        return isEmpty(rowKeySeparator) ? Constant.ROWKEY_SPLITER : rowKeySeparator;
    }

    public void setRowKeySeparator(String rowKeySeparator) {
        this.rowKeySeparator = rowKeySeparator;
    }

    @Override
    public String toString() {
        return "TableMetaInfo{" +
                "tableName='" + tableName + '\'' +
                ", schemaName='" + schemaName + '\'' +
                ", rowKeyColName='" + rowKeyColName + '\'' +
                ", rowKeyFormat='" + rowKeyFormat + '\'' +
                ", rowKeySaltUpperAndLower='" + rowKeySaltUpperAndLower + '\'' +
                ", describe='" + describe + '\'' +
                ", columns=" + columns +
                ", rowKeySeparator='" + rowKeySeparator + '\'' +
                '}';
    }
}