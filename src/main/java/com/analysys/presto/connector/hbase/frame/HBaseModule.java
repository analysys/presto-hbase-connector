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

import com.analysys.presto.connector.hbase.connection.HBaseClientManager;
import com.analysys.presto.connector.hbase.meta.HBaseMetadata;
import com.analysys.presto.connector.hbase.meta.HBaseTables;
import com.analysys.presto.connector.hbase.meta.HBaseConfig;
import com.analysys.presto.connector.hbase.query.HBasePageSinkProvider;
import com.analysys.presto.connector.hbase.query.HBasePageSourceProvider;
import com.analysys.presto.connector.hbase.query.HBaseRecordSetProvider;
import com.analysys.presto.connector.hbase.schedule.HBaseSplitManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.ConfigBinder;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.type.TypeManager;
import java.util.Objects;

/**
 * HBase inject module
 * Created by wupeng on 2018/1/19
 */
class HBaseModule implements Module {

    private final String connectorId;
    private final TypeManager typeManager;

    HBaseModule(String connectorId, TypeManager typeManager) {
        this.connectorId = Objects.requireNonNull(connectorId, "connector id is null");
        this.typeManager = Objects.requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(TypeManager.class).toInstance(this.typeManager);
        binder.bind(HBaseConnector.class).in(Scopes.SINGLETON);
        binder.bind(HBaseConnectorId.class).toInstance(new HBaseConnectorId(this.connectorId));
        binder.bind(HBaseMetadata.class).in(Scopes.SINGLETON);
        binder.bind(HBaseClientManager.class).in(Scopes.SINGLETON);
        binder.bind(HBaseSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(HBaseRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(HBaseTables.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(HBasePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(HBasePageSinkProvider.class).in(Scopes.SINGLETON);
        ConfigBinder.configBinder(binder).bindConfig(HBaseConfig.class);
    }

}
