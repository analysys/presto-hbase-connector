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
package com.analysys.presto.connector.hbase.connection;

import com.analysys.presto.connector.hbase.meta.HBaseConfig;
import com.analysys.presto.connector.hbase.meta.HBaseTable;
import javax.inject.Inject;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;

import java.io.IOException;
import java.util.Objects;

import static com.analysys.presto.connector.hbase.utils.Constant.SYSTEMOUT_INTERVAL;

/**
 * HBase client manager
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseClientManager {

    private static final Logger log = Logger.get(HBaseClientManager.class);

    private Connection connection;
    private HBaseConfig config;

    @Inject
    public HBaseClientManager(HBaseConfig config) {
        this.config = config;
        this.config.init();
        this.connection = createConnection();
    }

    public Connection createConnection() {
        Configuration conf;
        Connection connection;
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", config.getHbaseZookeeperQuorum());
            conf.set("hbase.zookeeper.property.clientPort", config.getZookeeperClientPort());
            conf.set("hbase.client.ipc.pool.size", "1");
            //  RPC fail retry times
            conf.set("hbase.client.retries.number", "3");

            conf.set("zookeeper.znode.parent", config.getZookeeperZnodeParent());

            // set this param a bigger value to avoid SocketTimeoutException when you invoke scanner.next()
            conf.set("hbase.client.scanner.timeout.period", "90000");

            if (config.getHbaseIsDistributed() != null) {
                conf.set("hbase.cluster.distributed", config.getHbaseIsDistributed());
            }
            long startTime = System.currentTimeMillis();
            connection = ConnectionFactory.createConnection(conf);

            if (System.currentTimeMillis() % SYSTEMOUT_INTERVAL == 0) {
                log.info("Create HBase connection " + (connection == null ? "succeed." : "failed.")
                        + ", used " + (System.currentTimeMillis() - startTime) + " mill sec");
            }

            return connection;
        } catch (Exception ex) {
            log.error(ex, ex.getMessage());
            return null;
        }
    }

    public Admin getAdmin() {
        try {
            if (connection == null) {
                connection = createConnection();
            }
            return connection.getAdmin();
        } catch (Exception ex) {
            log.error(ex, ex.getMessage());
        }
        return null;
    }

    public HBaseTable getTable(String schema, String tableName) {
        Objects.requireNonNull(schema, "schema is null");
        Objects.requireNonNull(tableName, "tableName is null");
        TableName hTableName = TableName.valueOf(schema.getBytes(), tableName.getBytes());
        TableDescriptor hTableDescriptor = null;

        Admin admin = null;
        try {
            admin = this.getAdmin();
            hTableDescriptor = admin.getDescriptor(hTableName);
        } catch (IOException ex) {
            log.error(ex, ex.getMessage());
        } finally {
            if (admin != null) {
                this.close(admin);
            }
        }

        if (hTableDescriptor == null) {
            return null;
        } else {
            return new HBaseTable(schema, hTableDescriptor, config);
        }
    }

    public void close(Admin admin) {
        try {
            admin.close();
        } catch (Exception ex) {
            log.error(ex, ex.getMessage());
        }
    }

    public HBaseConfig getConfig() {
        return config;
    }
}
