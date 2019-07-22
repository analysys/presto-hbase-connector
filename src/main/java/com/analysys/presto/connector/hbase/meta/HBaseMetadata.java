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
import com.analysys.presto.connector.hbase.utils.Constant;
import com.analysys.presto.connector.hbase.utils.Utils;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;

import static com.analysys.presto.connector.hbase.utils.Constant.CONNECTOR_NAME;
import static com.analysys.presto.connector.hbase.utils.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * HBase metadata
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseMetadata implements ConnectorMetadata {
    private static final Logger log = Logger.get(HBaseMetadata.class);

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
        List<SchemaTableName> schemaTableNames;
        try {
            schemaTableNames = new ArrayList<>(hbaseTables.getTables(admin, schema).keySet());
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
                                column.getType(), index, column.isIsRowKey()));
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

    /**
     * create snapshot
     *
     * @param snapshotName snapshot name
     * @param admin        admin
     * @param schemaName   schema name
     * @param tableName    table name
     * @throws IOException io exception
     */
    public static void createSnapshot(String snapshotName,
                                      Admin admin,
                                      String schemaName,
                                      String tableName) throws IOException {
        long start = System.currentTimeMillis();
        String fullTableName;
        if (Constant.HBASE_NAMESPACE_DEFAULT.equals(schemaName)
                || "".equals(schemaName)) {
            fullTableName = tableName;
        } else {
            fullTableName = schemaName + ":" + tableName;
        }
        HBaseProtos.SnapshotDescription snapshot = HBaseProtos.SnapshotDescription.newBuilder()
                .setName(snapshotName)
                .setTable(fullTableName)
                .setType(HBaseProtos.SnapshotDescription.Type.FLUSH)
                // .setType(HBaseProtos.SnapshotDescription.Type.DISABLED)
                .build();
        admin.snapshot(snapshot);
        log.info("createSnapshot: create snapshot " + snapshotName
                + " used " + (System.currentTimeMillis() - start) + " mill seconds.");
    }

    // ----------------------------------- start insert -----------------------------------
    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle connectorTableHandle) {
        HBaseTableHandle tableHandle = fromConnectorTableHandle(connectorTableHandle);
        String schemaName = tableHandle.getSchemaTableName().getSchemaName();
        String tableName = tableHandle.getSchemaTableName().getTableName();
        try {
            TableMetaInfo tableMetaInfo = Utils.getTableMetaInfoFromJson(schemaName, tableName,
                    this.hbaseClientManager.getConfig().getMetaDir());
            requireNonNull(tableMetaInfo,
                    String.format("The metadata of table %s.%s is null", schemaName, tableName));

            List<ColumnMetaInfo> cols = tableMetaInfo.getColumns();
            List<String> columnNames = new ArrayList<>(cols.size());
            List<Type> columnTypes = new ArrayList<>(cols.size());
            Map<String, String> colNameAndFamilyNameMap = new HashMap<>();
            for (ColumnMetaInfo col : cols) {
                columnNames.add(col.getColumnName());
                columnTypes.add(Utils.matchType(col.getType()));
                colNameAndFamilyNameMap.put(col.getColumnName(), col.getFamily());
            }
            int rowKeyColumnChannel = this.findRowKeyChannel(tableMetaInfo.getColumns());
            return new HBaseInsertTableHandle(
                    connectorId.getId(),
                    tableHandle.getSchemaTableName(),
                    columnNames,
                    columnTypes,
                    rowKeyColumnChannel,
                    colNameAndFamilyNameMap);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
        return null;
    }

    private int findRowKeyChannel(List<ColumnMetaInfo> columns) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).isIsRowKey()) {
                return i;
            }
        }
        // Table must specify rowKey column's name.
        // So it's impossible to be here.
        return -1;
    }

    /*@Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
                                                          ConnectorInsertTableHandle insertHandle,
                                                          Collection<Slice> fragments) {
        return Optional.empty();
    }*/

    private HBaseTableHandle fromConnectorTableHandle(ConnectorTableHandle tableHandle) {
        return checkType(tableHandle, HBaseTableHandle.class, "tableHandle");
    }
    // ----------------------------------- end insert -----------------------------------

    // --------------- support delete function start ---------------
    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle) {
        HBaseTableHandle hth = (HBaseTableHandle) tableHandle;
        String schemaName = hth.getSchemaTableName().getSchemaName();
        String tableName = hth.getSchemaTableName().getTableName();

        TableMetaInfo tableMetaInfo = Utils.getTableMetaInfoFromJson(schemaName, tableName,
                this.hbaseClientManager.getConfig().getMetaDir());
        requireNonNull(tableMetaInfo, String.format("Table %s.%s has no metadata, please check .json file under %s",
                schemaName, tableName, hbaseClientManager.getConfig().getMetaDir() + "/" + schemaName));

        Optional<ColumnMetaInfo> rowKeyOpt = tableMetaInfo.getColumns().stream().filter(ColumnMetaInfo::isIsRowKey).findFirst();
        checkArgument(rowKeyOpt.isPresent(),
                String.format("Table %s.%s has no rowKey! Please check .json file under %s",
                        schemaName, tableName, hbaseClientManager.getConfig().getMetaDir() + "/" + schemaName));

        ColumnMetaInfo rowKeyInfo = rowKeyOpt.get();
        // HBaseColumnHandle's attributes cannot be all the same with the REAL rowKey column,
        // Or there will be a java.lang.IllegalArgumentException: Multiple entries with same value Exception.
        return new HBaseColumnHandle(CONNECTOR_NAME, /*rowKeyInfo.getFamily(),*/ "",
                rowKeyInfo.getColumnName(), VarcharType.VARCHAR,
                tableMetaInfo.getColumns().indexOf(rowKeyOpt.get()),
                rowKeyInfo.isIsRowKey());
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return fromConnectorTableHandle(tableHandle);
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments) {
    }

    /*@Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle) {
        return false;
    }*/
    // --------------- support delete function end ---------------

}











