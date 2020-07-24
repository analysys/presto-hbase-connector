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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * 写hbase
 * Created by wupeng on 2018/4/23.
 */
public class HBaseInsertTableHandle implements ConnectorInsertTableHandle {

    private final String connectorId;
    private final SchemaTableName schemaTableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;

    private final int rowKeyColumnChannel;
    private final Map<String, String> colNameAndFamilyNameMap;

    @JsonCreator
    public HBaseInsertTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("rowKeyColumnChannel") int rowKeyColumnChannel,
            @JsonProperty("colNameAndFamilyNameMap") Map<String, String> colNameAndFamilyNameMap) {
        // super(connectorId, schemaTableName, columnNames, columnTypes);
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(columnNames, "columnNames is null");
        requireNonNull(columnTypes, "columnTypes is null");
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes sizes don't match");
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.rowKeyColumnChannel = rowKeyColumnChannel;
        this.colNameAndFamilyNameMap = colNameAndFamilyNameMap;
    }

    @JsonProperty
    public int getRowKeyColumnChannel() {
        return rowKeyColumnChannel;
    }

    @JsonProperty
    public Map<String, String> getColNameAndFamilyNameMap() {
        return colNameAndFamilyNameMap;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
    }

    @JsonProperty
    public List<String> getColumnNames() {
        return columnNames;
    }

    @JsonProperty
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

}
