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
package com.analysys.presto.connector.hbase.frame;

import com.analysys.presto.connector.hbase.meta.HBaseMetadata;
import com.analysys.presto.connector.hbase.schedule.HBaseSplitManager;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.*;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * HBase connector
 * Created by wupeng on 2018/1/19
 */
class HBaseConnector implements Connector {

    private static final Logger log = Logger.get(HBaseConnector.class);
    private final LifeCycleManager lifeCycleManager;
    private final HBaseMetadata metadata;
    private final HBaseSplitManager splitManager;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final ConnectorPageSourceProvider pageSourceProvider;

    @Inject
    public HBaseConnector(LifeCycleManager lifeCycleManager,
                          HBaseMetadata metadata,
                          HBaseSplitManager splitManager,
                          ConnectorPageSinkProvider pageSinkProvider,
                          ConnectorPageSourceProvider pageSourceProvider) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
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
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return pageSinkProvider;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return pageSourceProvider;
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
