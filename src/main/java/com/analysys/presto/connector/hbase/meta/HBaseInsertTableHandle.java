package com.analysys.presto.connector.hbase.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;

/**
 * 写hbase
 * Created by wupeng on 2018/4/23.
 */
public class HBaseInsertTableHandle extends HBaseExtendedTableHandle
        implements ConnectorInsertTableHandle {

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
        super(connectorId, schemaTableName, columnNames, columnTypes);
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

}
