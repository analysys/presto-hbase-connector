/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.analysys.presto.connector.hbase.query;

import com.analysys.presto.connector.hbase.connection.HBaseClientManager;
import com.analysys.presto.connector.hbase.meta.HBaseColumnHandle;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * HBase delete function interface provider
 *
 * @author wupeng
 * @date 2019/09/04
 */
public class HBasePageSourceProvider implements ConnectorPageSourceProvider {

    private HBaseRecordSetProvider recordSetProvider;
    private HBaseClientManager hbaseClientManager;

    @Inject
    public HBasePageSourceProvider(HBaseRecordSetProvider recordSetProvider, HBaseClientManager hbaseClientManager) {
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.hbaseClientManager = requireNonNull(hbaseClientManager, "hbaseClientManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
                                                ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns) {
        HBaseRecordSet recordSet = (HBaseRecordSet) recordSetProvider.getRecordSet(transactionHandle, session, split, columns);
        if (columns.stream().anyMatch(ch -> ((HBaseColumnHandle) ch).isIsRowKey())) {
            return new HBaseUpdatablePageSource(recordSet, hbaseClientManager);
        } else {
            return new RecordPageSource(recordSet);
        }
    }
}
