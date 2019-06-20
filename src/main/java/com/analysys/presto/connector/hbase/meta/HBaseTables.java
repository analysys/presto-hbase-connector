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
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
            HTableDescriptor[] descriptors = admin.listTableDescriptorsByNamespace(schema);
            for (HTableDescriptor table : descriptors) {
                // If the target table is in the other namespace, table.getNameAsString() will return
                // value like 'namespace1:tableName1', so we have to remove the unnecessary namespace.
                String tableName;
                if (table.getNameAsString() != null && table.getNameAsString().contains(":")) {
                    tableName = table.getNameAsString().split(":")[1];
                } else {
                    tableName = table.getNameAsString();
                }

                Objects.requireNonNull(tableName, "tableName cannot be null!");
                SchemaTableName schemaTableName = new SchemaTableName(schema, tableName);

                tablesBuilder.put(schemaTableName, new HBaseTableHandle(schemaTableName));
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

    public boolean dropTable(String schema, String tableName) {
        Admin admin = null;
        try {
            admin = hbaseClientManager.getAdmin();
            admin.disableTable(TableName.valueOf(schema + ":" + tableName));
            admin.deleteTable(TableName.valueOf(schema + ":" + tableName));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        } finally {
            if (admin != null) {
                hbaseClientManager.close(admin);
            }
        }
        return true;
    }

}
