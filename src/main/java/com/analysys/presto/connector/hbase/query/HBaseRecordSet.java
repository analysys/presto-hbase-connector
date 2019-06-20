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
import com.analysys.presto.connector.hbase.meta.HBaseColumnHandle;
import com.analysys.presto.connector.hbase.meta.HBaseConfig;
import com.analysys.presto.connector.hbase.schedule.ConditionInfo;
import com.analysys.presto.connector.hbase.schedule.HBaseSplit;
import com.analysys.presto.connector.hbase.utils.Utils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * HBase record set
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseRecordSet implements RecordSet {

    private static final Logger log = Logger.get(HBaseRecordSet.class);

    private final List<HBaseColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final HBaseSplit hBaseSplit;
    private ResultScanner resultScanner;
    private Connection connection;
    private Map<Integer, HBaseColumnHandle> fieldIndexMap = new HashMap<>();
    private HBaseConfig config;

    HBaseRecordSet(HBaseSplit split, List<ColumnHandle> columnHandles, HBaseClientManager clientManager) {
        this.hBaseSplit = Objects.requireNonNull(split, "split is null");
        Objects.requireNonNull(clientManager, "clientManager is null");
        this.config = clientManager.getConfig();

        Objects.requireNonNull(columnHandles, "column handles is null");
        this.columnHandles = columnHandles.stream().map(ch -> (HBaseColumnHandle) ch).collect(Collectors.toList());
        this.initFieldIndexMap(this.columnHandles);

        this.columnTypes = columnHandles.stream().map(ch -> ((HBaseColumnHandle) ch).getColumnType())
                .collect(Collectors.toList());

        this.connection = clientManager.createConnection();
    }

    @Override
    public List<Type> getColumnTypes() {
        return this.columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        try (Table table = connection
                .getTable(TableName.valueOf(
                        hBaseSplit.getSchemaName() + ":" + hBaseSplit.getTableName()))) {
            // Check out if this is batch get
            if (Utils.isBatchGet(this.hBaseSplit.getConstraint(), hBaseSplit.getRowKeyName())) {
                return new HBaseGetRecordCursor(this.columnHandles,
                        this.hBaseSplit, this.fieldIndexMap, this.connection);
            }
            // client side region scanner
            else if (this.hBaseSplit.getRegionInfo() != null) {
                Scan scan = getScanFromPrestoConstraint();
                long startTime = System.currentTimeMillis();
                Configuration conf = Utils.getHadoopConf(config.getHbaseZookeeperQuorum(), config.getZookeeperClientPort());
                Path root = new Path(config.getHbaseRootDir());
                FileSystem fs = FileSystem.get(conf);
                Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(hBaseSplit.getSnapshotName(), root);
                HBaseProtos.SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
                SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
                List<HRegionInfo> regionInfos = Utils.getRegionInfosFromManifest(manifest);
                HTableDescriptor htd = manifest.getTableDescriptor();
                ClientSideRegionScanner scanner;
                try {
                    scanner = new ClientSideRegionScanner(conf, fs, root, htd, regionInfos.get(hBaseSplit.getRegionIndex()), scan, null);
                } catch (AlreadyBeingCreatedException abce) {
                    log.error(abce, "E-3-1: " + abce.getMessage());
                    scanner = createClientSideRegionScannerWithExceptionHandle(conf, fs, root, htd, regionInfos.get(hBaseSplit.getRegionIndex()), scan);
                } catch (org.apache.hadoop.ipc.RemoteException re) {
                    log.error(re, "E-3-2: " + re.getMessage());
                    scanner = createClientSideRegionScannerWithExceptionHandle(conf, fs, root, htd, regionInfos.get(hBaseSplit.getRegionIndex()), scan);
                } catch (Exception e) {
                    log.error(e, "E-3-3: " + e.getMessage());
                    scanner = createClientSideRegionScannerWithExceptionHandle(conf, fs, root, htd, regionInfos.get(hBaseSplit.getRegionIndex()), scan);
                }
                if (scanner == null) {
                    log.error("ClientSideRegionScanner: Create scanner failed!");
                }
                // log.info("RegionIndex=" + hBaseSplit.getRegionIndex() + ", 获取regionInfo，耗时：" + (System.currentTimeMillis() - startTime) + " 毫秒。");
                log.info("Get regionInfo by regionIndex{ " + hBaseSplit.getRegionIndex()
                        + " }, used " + (System.currentTimeMillis() - startTime) + " mill seconds.");
                return new HBaseScanRecordCursorClientSide(this.columnHandles,
                        this.hBaseSplit, scanner, this.fieldIndexMap, connection);
            }
            // Normal scan
            else {
                Scan scan = getScanFromPrestoConstraint();
                if (table != null) {
                    this.resultScanner = table.getScanner(scan);
                }
                return new HBaseScanRecordCursor(this.columnHandles, this.hBaseSplit,
                        this.resultScanner, this.fieldIndexMap, this.connection);
            }
        } catch (Exception ex) {
            log.error(ex, ex.getMessage());
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
            return null;
        } /*if we close the connection here, table object will be unusable
        finally {
            if (connection != null)
                try {
                    connection.close();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
        }*/
    }

    private ClientSideRegionScanner createClientSideRegionScannerWithExceptionHandle(
            Configuration conf, FileSystem fs, Path root, HTableDescriptor htd,
            HRegionInfo regionInfo, Scan scan) {
        try {
            return new ClientSideRegionScanner(conf, fs, root, htd, regionInfo, scan, null);
        } catch (Exception e) {
            log.error(e, "E-3-4 Create ClientSideRegionScanner failed! Track is : " + e.getMessage());
            return null;
        }
    }

    private Filter getFilter(ConditionInfo condition) {
        CompareFilter.CompareOp operator;
        switch (condition.getOperator()) {
            case GT:
                operator = CompareFilter.CompareOp.GREATER;
                break;
            case GE:
                operator = CompareFilter.CompareOp.GREATER_OR_EQUAL;
                break;
            case LT:
                operator = CompareFilter.CompareOp.LESS;
                break;
            case LE:
                operator = CompareFilter.CompareOp.LESS_OR_EQUAL;
                break;
            default:
                operator = CompareFilter.CompareOp.EQUAL;
                break;
        }
        SingleColumnValueFilter f = new SingleColumnValueFilter(
                Bytes.toBytes(getFamilyByColumnName(condition.getColName(), columnHandles)),
                Bytes.toBytes(condition.getColName()), operator,
                condition.valueToBytes());
        f.setFilterIfMissing(true);
        return f;
    }

    private String getFamilyByColumnName(String columnName, List<HBaseColumnHandle> columns) {
        Objects.requireNonNull(columnName, "column name is null");
        HBaseColumnHandle column = columns.stream()
                .filter(col -> columnName.equals(col.getColumnName())).findAny().orElse(null);
        if (column != null) {
            return column.getFamily();
        } else {
            return "unknown_family";
        }
    }

    private Scan getScanFromPrestoConstraint() {
        Scan scan = new Scan().setCaching(10000);
        scan.setLoadColumnFamiliesOnDemand(true);
        scan.setCacheBlocks(true);

        // Filter the exactly columns we want
        // for (HBaseColumnHandle hch : this.columnHandles) {
        this.columnHandles.forEach(hch -> {
            if (this.hBaseSplit.getRowKeyName() == null) {
                scan.addColumn(
                        Bytes.toBytes(hch.getFamily()), Bytes.toBytes(hch.getColumnName()));
            } else {
                if (!this.hBaseSplit.getRowKeyName().equals(hch.getColumnName())) {
                    scan.addColumn(
                            Bytes.toBytes(hch.getFamily()), Bytes.toBytes(hch.getColumnName()));
                }
            }
        });

        FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        // ---------- Constraint push down ----------
        // This means user sql is like below:
        // select count(rowKey) / rowKey from table_xxx;
        // So we add FirstKeyOnlyFilter to return the first column to get the rowKey
        if (this.columnHandles.size() == 1
                && this.columnHandles.get(0).getColumnName().equals(this.hBaseSplit.getRowKeyName())) {
            allFilters.addFilter(new FirstKeyOnlyFilter());
            scan.setFilter(allFilters);
        } else {
            Map<String, List<ConditionInfo>> conditions = hBaseSplit.getConstraint().stream()
                    .collect(Collectors.groupingBy(ConditionInfo::getColName));
            // Here is what kind of condition presto can give to us:
            // 1.There can only be an 'and' relationship between different columns
            // 2.The same column can only be an 'or' relationship
            for (Map.Entry<String, List<ConditionInfo>> entry : conditions.entrySet()) {
                // Same column
                if (entry.getValue().size() > 1) {
                    List<Filter> columnFilterList = entry.getValue().stream().map(this::getFilter)
                            .collect(Collectors.toList());
                    FilterList columnFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE, columnFilterList);
                    allFilters.addFilter(columnFilter);
                }
                // different columns
                else {
                    allFilters.addFilter(getFilter(entry.getValue().get(0)));
                }
            }
            if (hBaseSplit.getConstraint().size() >= 1) {
                scan.setFilter(allFilters);
            }
        }
        // ---------- Constraint push down finished ----------

        if (hBaseSplit.getStartRow() != null && hBaseSplit.getEndRow() != null) {
            scan.setStopRow(Bytes.toBytes(hBaseSplit.getEndRow()));
            scan.setStartRow(Bytes.toBytes(hBaseSplit.getStartRow()));
        }
        return scan;
    }

    private void initFieldIndexMap(List<HBaseColumnHandle> columnHandles) {
        columnHandles.forEach(hch -> fieldIndexMap.put(hch.getColumnName().hashCode(), hch));
    }

}





















