package com.analysys.presto.connector.hbase.meta;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * HBase table layout handle
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseTableLayoutHandle implements ConnectorTableLayoutHandle {

    private final HBaseTableHandle table;

    private final TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public HBaseTableLayoutHandle(
            @JsonProperty("table") HBaseTableHandle table,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint) {
        this.table = requireNonNull(table, "table is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public HBaseTableHandle getTable() {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint() {
        return constraint;
    }

    @Override
    public String toString() {
        return table.toString();
    }

}
