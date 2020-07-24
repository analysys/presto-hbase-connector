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

import io.airlift.configuration.Config;
import io.airlift.log.Logger;
import org.apache.commons.lang.StringUtils;
import javax.validation.constraints.NotNull;
import java.net.InetAddress;
import java.util.Arrays;

/**
 * HBase config
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseConfig {

    private static final Logger log = Logger.get(HBaseConfig.class);

    private String hbaseZookeeperQuorum;
    private String zookeeperClientPort;
    private String hbaseIsDistributed;
    private String prestoWorkersIp;
    private String prestoWorkersName;
    private int prestoServerPort;
    private String zookeeperZnodeParent;

    /**
     * Can we random schedule redundant split
     */
    private boolean randomScheduleRedundantSplit;

    /**
     * A path that contains json files which define HBase table meta info.
     * This path means the default namespace of HBase.
     * metaDir:
     *   --schemas(namespaces)
     *     --table_A.json
     *     --table_B.json
     *   --table_C.json
     */
    private String metaDir;

    private String hbaseRootDir;

    private boolean enableClientSideScan = false;

    /**
     * Table names that using ClientSideRegionScanner to do query operation.
     * namespace_1:tableName_1,namespace_2:tableName_2,namespace_2:tableName_3
     * Set * if all tables are using ClientSideRegionScanner
     */
    private String clientSideQueryModeTableNames;

    /**
     * create snapshot retry times
     */
    private int createSnapshotRetryTimes = 15;

    /**
     * is rowkey has a seperate salt part
     * salt part can only have one char
     * can be anyone of a~z,A~Z,0~9
     */
    private boolean seperateSaltPart = false;

    @NotNull
    public String getMetaDir() {
        return metaDir;
    }

    @Config("meta-dir")
    public void setMetaDir(String metaDir) {
        this.metaDir = metaDir;
    }

    public String getPrestoWorkersName() {
        return prestoWorkersName;
    }

    @Config("presto-workers-name")
    public void setPrestoWorkersName(String prestoWorkersName) {
        this.prestoWorkersName = prestoWorkersName;
    }

    public boolean isRandomScheduleRedundantSplit() {
        return randomScheduleRedundantSplit;
    }

    @Config("random-schedule-redundant-split")
    public void setRandomScheduleRedundantSplit(boolean randomScheduleRedundantSplit) {
        this.randomScheduleRedundantSplit = randomScheduleRedundantSplit;
    }

    public void init() {
        // If we don't support to schedule a split to a specify worker,
        // then prestoWorkersName and prestoWorkersIp can be null
        if (this.randomScheduleRedundantSplit) {
            String[] workersIp = Arrays.stream(this.prestoWorkersName.split(",")).map(hostname -> {
                InetAddress address;
                try {
                    address = InetAddress.getByName(hostname);
                    return address.getHostAddress();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    return "";
                }
            }).toArray(String[]::new);

            this.prestoWorkersIp = StringUtils.join(workersIp, ",");
        }
    }

    @Config("zookeeper-quorum")
    public HBaseConfig setHbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
        this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
        return this;
    }

    @Config("zookeeper-client-port")
    public HBaseConfig setZookeeperClientPort(String zookeeperClientPort) {
        this.zookeeperClientPort = zookeeperClientPort;
        return this;
    }

    @Config("hbase-cluster-distributed")
    public void setHbaseIsDistributed(String hbaseIsDistributed) {
        this.hbaseIsDistributed = hbaseIsDistributed;
    }

    @Config("presto-server-port")
    public void setPrestoServerPort(int prestoServerPort) {
        this.prestoServerPort = prestoServerPort;
    }

    @Config("presto-workers-ip")
    public void setPrestoWorkersIp(String prestoWorkersIp) {
        this.prestoWorkersIp = prestoWorkersIp;
    }

    @Config("zookeeper-znode-parent")
    public void setZookeeperZnodeParent(String zookeeperZnodeParent) {
        this.zookeeperZnodeParent = zookeeperZnodeParent;
    }

    @NotNull
    public String getHbaseZookeeperQuorum() {
        return this.hbaseZookeeperQuorum;
    }

    @NotNull
    public String getZookeeperClientPort() {
        return this.zookeeperClientPort;
    }

    @NotNull
    public String getZookeeperZnodeParent() {
        return this.zookeeperZnodeParent;
    }

    public String getHbaseIsDistributed() {
        return this.hbaseIsDistributed;
    }

    public String getPrestoWorkersIp() {
        return prestoWorkersIp;
    }

    @NotNull
    public int getPrestoServerPort() {
        return prestoServerPort;
    }

    public String getHbaseRootDir() {
        return hbaseRootDir;
    }

    @Config("hbase-rootdir")
    public void setHbaseRootDir(String hbaseRootDir) {
        this.hbaseRootDir = hbaseRootDir;
    }

    public boolean isEnableClientSideScan() {
        return enableClientSideScan;
    }

    @Config("enable-clientSide-scan")
    public void setEnableClientSideScan(boolean enableClientSideScan) {
        this.enableClientSideScan = enableClientSideScan;
    }

    public String getClientSideQueryModeTableNames() {
        return clientSideQueryModeTableNames;
    }

    @Config("clientside-querymode-tablenames")
    public void setClientSideQueryModeTableNames(String clientSideQueryModeTableNames) {
        this.clientSideQueryModeTableNames = clientSideQueryModeTableNames;
    }

    public int getCreateSnapshotRetryTimes() {
        return createSnapshotRetryTimes;
    }

    @Config("clientside-createsnapshot-retrytimes")
    public void setCreateSnapshotRetryTimes(int createSnapshotRetryTimes) {
        this.createSnapshotRetryTimes = createSnapshotRetryTimes;
    }

    public boolean isSeperateSaltPart() {
        return seperateSaltPart;
    }

    @Config("has-seperate-salt-part")
    public void setSeperateSaltPart(boolean seperateSaltPart) {
        this.seperateSaltPart = seperateSaltPart;
    }

    @Override
    public String toString() {
        return "HBaseConfig{" +
                "hbaseZookeeperQuorum='" + hbaseZookeeperQuorum + '\'' +
                ", zookeeperClientPort='" + zookeeperClientPort + '\'' +
                ", hbaseIsDistributed='" + hbaseIsDistributed + '\'' +
                ", prestoWorkersIp='" + prestoWorkersIp + '\'' +
                ", prestoWorkersName='" + prestoWorkersName + '\'' +
                ", prestoServerPort=" + prestoServerPort +
                ", randomScheduleRedundantSplit=" + randomScheduleRedundantSplit +
                ", metaDir='" + metaDir + '\'' +
                ", createSnapshotRetryTimes='" + createSnapshotRetryTimes + '\'' +
                '}';
    }

}
