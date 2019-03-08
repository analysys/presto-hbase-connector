package com.analysys.presto.connector.hbase.frame;

import com.analysys.presto.connector.hbase.meta.HBaseMetadata;
import com.analysys.presto.connector.hbase.query.HBaseRecordSetProvider;
import com.analysys.presto.connector.hbase.schedule.HBaseSplitManager;
import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;
import java.util.Objects;

/**
 * HBase connector
 * Created by wupeng on 2018/1/19
 */
class HBaseConnector implements Connector {

    private static final Logger log = Logger.get(HBaseConnector.class);
    private final LifeCycleManager lifeCycleManager;
    private final HBaseMetadata metadata;
    private final HBaseSplitManager splitManager;
    private final HBaseRecordSetProvider recordSetProvider;

    @Inject
    public HBaseConnector(LifeCycleManager lifeCycleManager,
                          HBaseMetadata metadata,
                          HBaseSplitManager splitManager,
                          HBaseRecordSetProvider recordSetProvider) {
        this.lifeCycleManager = Objects.requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = Objects.requireNonNull(metadata, "metadata is null");
        this.splitManager = Objects.requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = Objects.requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean b) {
        return HBaseTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle connectorTransactionHandle) {
        return this.metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return this.splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return this.recordSetProvider;
    }

    @Override
    public void shutdown() {
        if (this.lifeCycleManager != null) {
            try {
                lifeCycleManager.stop();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
