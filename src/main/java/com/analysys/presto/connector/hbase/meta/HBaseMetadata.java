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
package com.analysys.presto.connector.hbase.meta;

import com.analysys.presto.connector.hbase.connection.HBaseClientManager;
import com.analysys.presto.connector.hbase.frame.HBaseConnectorId;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.hadoop.hbase.client.Admin;

import java.util.*;
import java.util.stream.Collectors;

import static com.analysys.presto.connector.hbase.utils.Types.checkType;
import static java.util.Objects.requireNonNull;

/**
 * HBase metadata
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseMetadata implements ConnectorMetadata {

    private final HBaseConnectorId connectorId;
    private final HBaseTables hbaseTables;
    private final HBaseClientManager hbaseClientManager;

    @Inject
    public HBaseMetadata(HBaseConnectorId connectorId, HBaseTables hbaseTables, HBaseClientManager hbaseClientManager) {
        this.connectorId = connectorId;
        this.hbaseTables = requireNonNull(hbaseTables, "hbaseTables is null");
        this.hbaseClientManager = hbaseClientManager;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession) {
        return listSchemaNames();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName schemaTableName) {
        requireNonNull(schemaTableName, "schemaTableName is null");
        Admin admin = null;
        ConnectorTableHandle connectorTableHandle;
        try {
            admin = hbaseClientManager.getAdmin();
            connectorTableHandle = hbaseTables.getTables(
                    admin, schemaTableName.getSchemaName()).get(schemaTableName);
        } finally {
            if (admin != null) {
                hbaseClientManager.close(admin);
            }
        }
        return connectorTableHandle;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession connectorSession,
                                                            ConnectorTableHandle connectorTableHandle,
                                                            Constraint<ColumnHandle> constraint,
                                                            Optional<Set<ColumnHandle>> optional) {
        HBaseTableHandle tableHandle = checkType(connectorTableHandle, HBaseTableHandle.class, "tableHandle");
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new HBaseTableLayoutHandle(tableHandle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession connectorSession,
                                               ConnectorTableLayoutHandle connectorTableLayoutHandle) {
        HBaseTableLayoutHandle layout = checkType(connectorTableLayoutHandle, HBaseTableLayoutHandle.class, "layout");
        return new ConnectorTableLayout(layout);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession connectorSession,
                                                   ConnectorTableHandle connectorTableHandle) {
        HBaseTableHandle hBaseTableHandle = (HBaseTableHandle) connectorTableHandle;
        SchemaTableName tableName = new SchemaTableName(hBaseTableHandle.getSchemaTableName().getSchemaName(),
                hBaseTableHandle.getSchemaTableName().getTableName());
        return this.getTableMetadata(tableName);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName) {
        if (!this.listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        } else {
            HBaseTable table = hbaseClientManager.getTable(tableName.getSchemaName(), tableName.getTableName());
            return table == null ? null : new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
        }
    }

    private List<String> listSchemaNames() {
        return ImmutableList.copyOf(hbaseTables.getSchemaNames());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession connectorSession, String schema) {
        Admin admin = hbaseClientManager.getAdmin();
        List<SchemaTableName> schemaTableNames = null;
        try {
            schemaTableNames = hbaseTables.getTables(admin, schema)
                    .keySet()
                    .stream()
                    .collect(Collectors.toList());
        } finally {
            if (admin != null) {
                hbaseClientManager.close(admin);
            }
        }
        return schemaTableNames;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession connectorSession,
                                                      ConnectorTableHandle connectorTableHandle) {
        HBaseTableHandle hBaseTableHandle = (HBaseTableHandle) connectorTableHandle;
        HBaseTable table = hbaseClientManager.getTable(hBaseTableHandle.getSchemaTableName().getSchemaName(),
                hBaseTableHandle.getSchemaTableName().getTableName());
        if (table == null) {
            throw new TableNotFoundException(hBaseTableHandle.getSchemaTableName());
        } else {
            ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
            int index = 0;
            for (Iterator itr = table.getColumnsMetadata().iterator(); itr.hasNext(); ++index) {
                HBaseColumnMetadata column = (HBaseColumnMetadata) itr.next();
                columnHandles.put(column.getName(),
                        new HBaseColumnHandle(
                                connectorId.getId(), column.getFamily(), column.getName(),
                                column.getType(), index));
            }
            return columnHandles.build();
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession connectorSession,
                                            ConnectorTableHandle connectorTableHandle,
                                            ColumnHandle columnHandle) {
        checkType(connectorTableHandle, HBaseTableHandle.class, "tableHandle");
        return checkType(columnHandle, HBaseColumnHandle.class, "columnHandle").toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession connectorSession,
                                                                       SchemaTablePrefix schemaTablePrefix) {
        Objects.requireNonNull(schemaTablePrefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tables = this.listTables(connectorSession, schemaTablePrefix);
        for (SchemaTableName tableName : tables) {
            ConnectorTableMetadata tableMetadata = this.getTableMetadata(tableName);
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
        return (prefix.getSchemaName() == null ?
                this.listTables(session, prefix.getSchemaName()) :
                ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName())));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
        HBaseTableHandle handle = (HBaseTableHandle) tableHandle;
        String schema = handle.getSchemaTableName().getSchemaName();
        String tableName = handle.getSchemaTableName().getTableName();
        hbaseTables.dropTable(schema, tableName);
    }

}











