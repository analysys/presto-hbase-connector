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
package com.analysys.presto.connector.hbase.query;

import com.analysys.presto.connector.hbase.connection.HBaseClientManager;
import com.analysys.presto.connector.hbase.frame.HBaseConnectorId;
import com.analysys.presto.connector.hbase.meta.HBaseColumnHandle;
import com.analysys.presto.connector.hbase.schedule.HBaseSplit;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.prestosql.spi.connector.*;

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
