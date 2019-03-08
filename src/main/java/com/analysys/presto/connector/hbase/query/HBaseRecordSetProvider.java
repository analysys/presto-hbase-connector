package com.analysys.presto.connector.hbase.query;

import com.analysys.presto.connector.hbase.frame.HBaseConnectorId;
import com.analysys.presto.connector.hbase.connection.HBaseClientManager;
import com.analysys.presto.connector.hbase.meta.HBaseColumnHandle;
import com.analysys.presto.connector.hbase.schedule.HBaseSplit;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import javax.inject.Inject;
import java.util.List;
import java.util.Objects;

/**
 * HBase record set provider
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseRecordSetProvider implements ConnectorRecordSetProvider {

    private final String connectorId;
    private final HBaseClientManager clientManager;

    @Inject
    public HBaseRecordSetProvider(HBaseConnectorId connectorId, HBaseClientManager clientManager) {
        this.connectorId = (Objects.requireNonNull(connectorId, "connectorId is null")).toString();
        this.clientManager = Objects.requireNonNull(clientManager, "hBaseClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle,
                                  ConnectorSession session, ConnectorSplit split, List columns) {
        Objects.requireNonNull(split, "partitionChunk is null");
        HBaseSplit hBaseSplit = (HBaseSplit) split;
        Preconditions.checkArgument(
                hBaseSplit.getConnectorId().equals(this.connectorId), "split is not for this connector");
        Builder<ColumnHandle> handles = ImmutableList.builder();
        for (Object obj : columns) {
            HBaseColumnHandle hch = (HBaseColumnHandle) obj;
            handles.add(hch);
        }
        return new HBaseRecordSet(hBaseSplit, handles.build(), this.clientManager);
    }
}
