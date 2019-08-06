package com.analysys.presto.connector.hbase.query;

import com.analysys.presto.connector.hbase.connection.HBaseClientManager;
import com.analysys.presto.connector.hbase.meta.HBaseInsertTableHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.*;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * HBase page sink provider
 *
 * @author wupeng
 * @date 2018/4/25
 */
public class HBasePageSinkProvider implements ConnectorPageSinkProvider {
    private final HBaseClientManager clientManager;

    @Inject
    public HBasePageSinkProvider(HBaseClientManager clientManager) {
        this.clientManager = requireNonNull(clientManager, "clientManager is null");
    }

    /**
     * This interface is used for create table xxx as select * from table_a limit 11;
     * @param transactionHandle transactionHandle
     * @param session session
     * @param outputTableHandle outputTableHandle
     * @return ConnectorPageSink
     */
    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                            ConnectorOutputTableHandle outputTableHandle) {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating table.");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                            ConnectorInsertTableHandle insertTableHandle) {
        requireNonNull(insertTableHandle, "insertTableHandle is null.");
        checkArgument(insertTableHandle instanceof HBaseInsertTableHandle,
                "insertTableHandle is not an instance of HBaseInsertTableHandle.");
        HBaseInsertTableHandle handle = (HBaseInsertTableHandle) insertTableHandle;

        return new HBasePageSink(session, clientManager, handle);
    }
}

