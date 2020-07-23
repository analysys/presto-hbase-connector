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
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import javax.inject.Inject;
import java.io.IOException;
import java.util.*;

/**
 * HBase tables
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseTables {

    public static final Logger logger = Logger.get(HBaseTables.class);

    private HBaseClientManager hbaseClientManager;

    @Inject
    public HBaseTables(HBaseClientManager hbaseClientManager) {
        this.hbaseClientManager = hbaseClientManager;
    }

    Map<SchemaTableName, HBaseTableHandle> getTables(Admin admin, String schema) {
        Map<SchemaTableName, HBaseTableHandle> tables;
        try {
            ImmutableMap.Builder<SchemaTableName, HBaseTableHandle> tablesBuilder = ImmutableMap.builder();
            List<TableDescriptor> descriptors = admin.listTableDescriptorsByNamespace(schema.getBytes());
            for (TableDescriptor table : descriptors) {
                // If the target table is in the other namespace, table.getNameAsString() will return
                // value like 'namespace1:tableName1', so we have to remove the unnecessary namespace.
                String tableName = table.getTableName().getNameAsString();
                if (tableName != null && tableName.contains(":")) {
                    tableName = tableName.split(":")[1];
                }

                Objects.requireNonNull(tableName, "tableName cannot be null!");
                SchemaTableName schemaTableName = new SchemaTableName(schema, tableName);

                tablesBuilder.put(schemaTableName, new HBaseTableHandle(schemaTableName, TupleDomain.all()));
            }
            tables = tablesBuilder.build();
            return tables;
        } catch (Exception ex) {
            logger.error(ex, ex.getMessage());
        }
        return null;
    }

    Set<String> getSchemaNames() {
        NamespaceDescriptor[] namespaceDescriptors = new NamespaceDescriptor[0];
        Admin admin = null;
        try {
            admin = hbaseClientManager.getAdmin();
            namespaceDescriptors = admin.listNamespaceDescriptors();
        } catch (IOException e) {
            logger.error(e, e.getMessage());
        } finally {
            if (admin != null) {
                hbaseClientManager.close(admin);
            }
        }

        HashSet<String> set = new HashSet<>();
        NamespaceDescriptor[] temp = namespaceDescriptors;
        int namespaceDescriptorLength = namespaceDescriptors.length;

        for (int i = 0; i < namespaceDescriptorLength; ++i) {
            NamespaceDescriptor namespaceDescriptor = temp[i];
            set.add(namespaceDescriptor.getName());
        }

        return set;
    }

    void dropTable(String schema, String tableName) {
        Admin admin = null;
        try {
            admin = hbaseClientManager.getAdmin();
            admin.disableTable(TableName.valueOf(schema + ":" + tableName));
            admin.deleteTable(TableName.valueOf(schema + ":" + tableName));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (admin != null) {
                hbaseClientManager.close(admin);
            }
        }
    }

}
