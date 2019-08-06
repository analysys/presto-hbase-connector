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

import com.analysys.presto.connector.hbase.utils.Utils;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.ColumnMetadata;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.util.List;
import java.util.Objects;

/**
 * HBase table
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseTable {

    public static final Logger logger = Logger.get(HBaseTable.class);

    private final HTableDescriptor hTableDescriptor;
    private final List<ColumnMetadata> columnsMetadata;

    public HBaseTable(String schemaName, HTableDescriptor tabDesc, HBaseConfig config) {
        this.hTableDescriptor = Objects.requireNonNull(tabDesc, "tabDesc is null");
        Objects.requireNonNull(schemaName, "schemaName is null");
        ImmutableList<ColumnMetadata> tableMeta = null;
        try {
            String tableName = tabDesc.getNameAsString() != null && tabDesc.getNameAsString().contains(":") ?
                    tabDesc.getNameAsString().split(":")[1] : tabDesc.getNameAsString();
            tableMeta = Utils.getColumnMetaFromJson(schemaName, tableName, config.getMetaDir());
            if (tableMeta == null || tableMeta.size() <= 0) {
                logger.error("OOPS! Table meta info cannot be NULL, table name=" + tabDesc.getNameAsString());
                throw new Exception("Cannot find meta info of table " + tabDesc.getNameAsString() + ".");
            }
        } catch (Exception e) {
            logger.error(e, e.getMessage());
        }
        this.columnsMetadata = tableMeta;
    }

    public String getTableName() {
        return this.hTableDescriptor.getNameAsString();
    }

    List<ColumnMetadata> getColumnsMetadata() {
        return this.columnsMetadata;
    }

}
