package com.analysys.presto.connector.hbase.meta;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * HBase table handle
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseTableHandle implements ConnectorTableHandle {

    private final SchemaTableName schemaTableName;

    @JsonCreator
    public HBaseTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName) {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HBaseTableHandle that = (HBaseTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaTableName);
    }

    @Override
    public String toString() {
        return "HBaseTableHandle{" +
                "schemaTableName=" + schemaTableName +
                '}';
    }

}
