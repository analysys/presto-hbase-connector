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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import org.apache.hadoop.hbase.client.RegionInfo;
import java.util.List;
import java.util.Objects;

/**
 * HBase split
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseSplit implements ConnectorSplit {

    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final boolean randomScheduleRedundantSplit;
    private final List<HostAddress> addresses;
    private final String startRow;
    private final String endRow;
    private final List<ConditionInfo> constraint;
    private final String rowKeyName;
    private final Integer regionIndex;
    private final RegionInfo regionInfo;
    private final String snapshotName;

    @JsonCreator
    public HBaseSplit(@JsonProperty("connectorId") String connectorId,
                      @JsonProperty("schemaName") String schemaName,
                      @JsonProperty("tableName") String tableName,
                      @JsonProperty("rowKeyName") String rowKeyName,
                      @JsonProperty("addresses") List<HostAddress> addresses,
                      @JsonProperty("startRow") String startRow,
                      @JsonProperty("endRow") String endRow,
                      @JsonProperty("constraint") List<ConditionInfo> constraint,
                      @JsonProperty("randomScheduleRedundantSplit") boolean randomScheduleRedundantSplit,
                      @JsonProperty("regionIndex") Integer regionIndex,
                      @JsonProperty("regionInfo") RegionInfo regionInfo,
                      @JsonProperty("snapshotName") String snapshotName) {
        this.schemaName = Objects.requireNonNull(schemaName, "schema name is null");
        this.connectorId = Objects.requireNonNull(connectorId, "connector id is null");
        this.tableName = Objects.requireNonNull(tableName, "table name is null");
        // this.rowKeyName = Objects.requireNonNull(rowKeyName, "row key name is null");
        this.rowKeyName = rowKeyName;
        this.addresses = addresses;
        this.randomScheduleRedundantSplit = randomScheduleRedundantSplit;
        this.startRow = startRow;
        this.endRow = endRow;
        this.constraint = constraint;
        this.regionIndex = regionIndex;
        this.regionInfo = regionInfo;
        this.snapshotName = snapshotName;
    }

    @JsonProperty
    public String getConnectorId() {
        return this.connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return this.schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return this.tableName;
    }

    @JsonProperty
    public String getRowKeyName() {
        return rowKeyName;
    }

    @JsonProperty
    public String getStartRow() {
        return this.startRow;
    }

    @JsonProperty
    public String getEndRow() {
        return this.endRow;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return !this.randomScheduleRedundantSplit;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses() {
        return this.addresses;
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @JsonProperty
    public List<ConditionInfo> getConstraint() {
        return constraint;
    }

    @JsonProperty
    public Integer getRegionIndex() {
        return regionIndex;
    }

    @JsonProperty
    public RegionInfo getRegionInfo() {
        return regionInfo;
    }

    @JsonProperty
    public String getSnapshotName() {
        return snapshotName;
    }

    @Override
    public String toString() {
        return "HBaseSplit{" +
                "connectorId='" + connectorId + '\'' +
                ", schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", startRow='" + startRow + '\'' +
                ", endRow='" + endRow + '\'' +
                ", regionInfo=" + (regionInfo != null ? regionInfo.toString() : "null") +
                '}';
    }

    String toSimpleString() {
        return "HBaseSplit{" +
                "schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", startRow='" + startRow + '\'' +
                ", endRow='" + endRow + '\'' +
                '}';
    }
}
