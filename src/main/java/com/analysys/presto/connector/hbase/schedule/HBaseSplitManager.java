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
package com.analysys.presto.connector.hbase.schedule;

import com.analysys.presto.connector.hbase.connection.HBaseClientManager;
import com.analysys.presto.connector.hbase.frame.HBaseConnectorId;
import com.analysys.presto.connector.hbase.meta.*;
import com.analysys.presto.connector.hbase.utils.Constant;
import com.analysys.presto.connector.hbase.utils.TimeClicker;
import com.analysys.presto.connector.hbase.utils.Utils;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.base.Preconditions;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Admin;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import static com.analysys.presto.connector.hbase.utils.Constant.*;

/**
 * HBase split manager
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseSplitManager implements ConnectorSplitManager {

    public static final Logger log = Logger.get(HBaseSplitManager.class);

    private final String connectorId;
    private final HBaseClientManager clientManager;

    private final HBaseConfig config;

    @Inject
    public HBaseSplitManager(HBaseConnectorId connectorId, HBaseClientManager clientManager, HBaseConfig config) {
        this.connectorId = (Objects.requireNonNull(connectorId, "connectorId is null")).toString();
        this.clientManager = Objects.requireNonNull(clientManager, "client is null");
        this.config = Objects.requireNonNull(config, "config is null");
        log.info("\nPresto HBase Connector Config：" + this.config.toString());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transHd,
                                          ConnectorSession session,
                                          ConnectorTableLayoutHandle layout,
                                          SplitSchedulingStrategy strategy) {
        HBaseTableLayoutHandle layoutHandle = (HBaseTableLayoutHandle) layout;
        HBaseTableHandle tableHandle = layoutHandle.getTable();
        String schemaName = tableHandle.getSchemaTableName().getSchemaName();
        String tableName = tableHandle.getSchemaTableName().getTableName();
        HBaseTable table = this.clientManager.getTable(schemaName, tableName);
        Preconditions.checkState(table != null, "Table %s.%s no longer exists", schemaName, tableName);

        TupleDomain<ColumnHandle> constraint = layoutHandle.getConstraint();

        TableMetaInfo tableMetaInfo = Utils.getTableMetaInfoFromJson(schemaName, tableName, config.getMetaDir());
        Preconditions.checkState(tableMetaInfo != null,
                String.format("The meta info of table %s.%s doesn't exists! Table meta dir is %s.",
                        schemaName, tableName, config.getMetaDir()));

        List<HBaseSplit> splits;
        List<ConditionInfo> conditions = findConditionFromConstraint(constraint);
        if (Utils.isBatchGet(conditions, tableMetaInfo.getRowKeyColName())) {
            splits = getSplitsForBatchGet(conditions, tableMetaInfo, tableHandle);
            Collections.shuffle(splits);
            return new FixedSplitSource(splits);
        } // end of if isBatchGet

        List<String> clientSideTables = getClientSideTables(config.getClientSideQueryModeTableNames());
        if (isClientSideRegionScanTable(schemaName, tableName, clientSideTables)) {
            splits = getSplitsForClientSide(schemaName, tableName, conditions, tableMetaInfo.getRowKeyColName());
        } else {
            splits = getSplitsForScan(conditions, tableMetaInfo, schemaName, tableName);
        }

        log.info("The final split count is " + splits.size() + ".");
        splits.forEach(split -> log.info("print split info：" + split.toString()));

        Collections.shuffle(splits);
        return new FixedSplitSource(splits);
    }

    /**
     * get splits for client side query mode
     *
     * @param schemaName schema name
     * @param tableName  table name
     * @param conditions conditions
     * @param rowKeyName rowKey name
     * @return splits
     */
    private List<HBaseSplit> getSplitsForClientSide(String schemaName, String tableName,
                                                    List<ConditionInfo> conditions, String rowKeyName) {
        log.info("ClientSideScan:" + schemaName + ":" + tableName);
        int hostIndex = 0;
        long createSnapshotTime = 0;
        List<HBaseSplit> splits = new ArrayList<>();
        String snapshotName = null;
        try {
            long start = System.currentTimeMillis();
            // create snapshot with retry
            snapshotName = createSnapshotWithRetry(schemaName, tableName, clientManager.getAdmin());
            createSnapshotTime = TimeClicker.calculateTimeTo(start);

            // get regions from snapshot
            List<HRegionInfo> regions = Utils.getRegionInfos(config.getHbaseZookeeperQuorum(),
                    config.getZookeeperClientPort(), config.getHbaseRootDir(), snapshotName);
            // create splits
            for (HRegionInfo regionInfo : regions) {
                // Client side region scanner using no startKey and endKey.
                splits.add(new HBaseSplit(this.connectorId, schemaName, tableName,
                        rowKeyName, getHostAddresses(hostIndex), null, null, conditions,
                        config.isRandomScheduleRedundantSplit(),
                        hostIndex, regionInfo, snapshotName));
                hostIndex++;
            }
        } catch (Exception e) {
            log.error(e, "E-1-1: " + e.getMessage());
        }
        log.info("create snapshot " + snapshotName + ", using " + createSnapshotTime + " mill seconds.");
        return splits;
    }

    /**
     * create snapshot with retry
     *
     * @param admin HBase admin
     * @return snapshot name
     */
    private String createSnapshotWithRetry(String schemaName, String tableName, Admin admin) {
        long start = System.currentTimeMillis();
        String snapshotName = null;
        try {
            snapshotName = "ss-" + schemaName + "." + tableName + "-" + System.nanoTime();
            HBaseMetadata.createSnapshot(snapshotName, admin, schemaName, tableName);
        } catch (Exception e) {
            log.error(e, "E-2-1: Exception: create snapshot failed, snapshotName is " + snapshotName
                    + ", track:" + e.getMessage());
            for (int i = 0; i < 15; i++) {
                try {
                    Thread.sleep(100);
                    HBaseMetadata.createSnapshot(snapshotName, admin, schemaName, tableName);
                    log.info("Recreate snapshot success! snapshotName is " + snapshotName
                            + ", retried ：" + i + " times, using " + (System.currentTimeMillis() - start) + " mill seconds.");
                    return snapshotName;
                } catch (Exception ee) {
                    log.error(ee, "E-2-2: create snapshot failed, snapshotName is " + snapshotName
                            + ", track:" + ee.getMessage());
                }
            }
            log.error("E-2-3: after retry, create snapshot " + snapshotName + " still failed.");
        }
        log.info("createSnapshotWithRetry: create snapshot " + snapshotName
                + " finished, using " + (System.currentTimeMillis() - start) + " mill seconds.");
        return snapshotName;
    }

    /**
     * check if current table using ClientSideRegionScanner to query
     *
     * @param schemaName       schema name
     * @param tableName        table name
     * @param clientSideTables clientSideTables
     * @return true or false
     */
    private boolean isClientSideRegionScanTable(String schemaName, String tableName,
                                                List<String> clientSideTables) {
        // if we didn't open client side scan, return false
        if (!config.isEnableClientSideScan()) {
            return false;
        }
        Preconditions.checkState(clientSideTables != null && !clientSideTables.isEmpty(),
                "Parameter 'clientside-querymode-tablenames' cannot be NULL when 'enable-clientSide-scan' is true!" +
                        "\nSet this to * if all the table are using ClientSide query mode.");
        String schemaAndTableName = schemaName + ":" + tableName;
        return "*".equals(clientSideTables.get(0)) || clientSideTables.contains(schemaAndTableName);
    }

    private List<String> getClientSideTables(String clientSideQueryModeTableNames) {
        if (clientSideQueryModeTableNames == null) {
            return null;
        }
        return Arrays.asList(clientSideQueryModeTableNames.split(","));
    }

    /**
     * get splits for scan query mode
     *
     * @param conditions    conditions
     * @param tableMetaInfo tableMetaInfo
     * @param schemaName    schema name
     * @param tableName     table name
     * @return splits
     */
    private List<HBaseSplit> getSplitsForScan(List<ConditionInfo> conditions,
                                              TableMetaInfo tableMetaInfo,
                                              String schemaName, String tableName) {
        List<HBaseSplit> splits = new ArrayList<>();
        int hostIndex = 0;
        List<String> startKeyList;

        // make startKey by rowKey format.
        if (!conditions.isEmpty() && !StringUtils.isEmpty(tableMetaInfo.getRowKeyFormat())) {
            startKeyList = getScanStartKey(conditions, "", tableMetaInfo.getRowKeyFormat().split(","), 0);
        } else {
            startKeyList = new ArrayList<>();
        }

        // add salt
        if (tableMetaInfo.getRowKeyPrefixUpper() >= 0) {
            startKeyList = addSalt2StartKeys(startKeyList, tableMetaInfo.getRowKeyPrefixLower(),
                    tableMetaInfo.getRowKeyPrefixUpper(), tableMetaInfo.getRowKeySeparator());
        }

        if (!startKeyList.isEmpty()) {
            String endKey;
            for (String saltyStartKey : startKeyList) {
                endKey = saltyStartKey + ROWKEY_TAIL;
                splits.add(new HBaseSplit(this.connectorId, schemaName, tableName,
                        tableMetaInfo.getRowKeyColName(), getHostAddresses(hostIndex),
                        saltyStartKey, endKey, conditions, config.isRandomScheduleRedundantSplit(), null, null, null));
                hostIndex += 1;
            }
        }
        // single split
        else {
            splits.add(new HBaseSplit(this.connectorId, schemaName, tableName,
                    tableMetaInfo.getRowKeyColName(), getHostAddresses(hostIndex),
                    null, null, conditions, config.isRandomScheduleRedundantSplit(), null, null, null));
        }
        return splits;
    }

    private List<HBaseSplit> getSplitsForBatchGet(List<ConditionInfo> conditions,
                                                  TableMetaInfo tableMetaInfo,
                                                  HBaseTableHandle tableHandle) {
        List<HBaseSplit> splits = new ArrayList<>();
        // Find all conditions of rowKey(rowKey='xxx' or rowKey in('xxx','xxx'))
        List<ConditionInfo> rowKeys = conditions.stream().filter(cond ->
                        tableMetaInfo.getRowKeyColName().equals(cond.getColName())
                                && cond.getOperator() == CONDITION_OPER.EQ
        ).collect(Collectors.toList());

        int hostIndex = 0;
        int maxSplitSize;
        List<ConditionInfo> splitConditions = new ArrayList<>();
        // Each split has at least 20 pieces of data, and the maximum number of splits is 30.
        if (rowKeys.size() / BATCHGET_SPLIT_RECORD_COUNT > BATCHGET_SPLIT_MAX_COUNT) {
            maxSplitSize = rowKeys.size() / BATCHGET_SPLIT_MAX_COUNT;
        } else {
            maxSplitSize = BATCHGET_SPLIT_RECORD_COUNT;
        }
        for (ConditionInfo cond : rowKeys) {
            splitConditions.add(cond);
            // Rather than creating another split, spread the extra records into each split
            if (splitConditions.size() >=
                    maxSplitSize + (splits.size() < rowKeys.size() % BATCHGET_SPLIT_RECORD_COUNT ? 1 : 0)) {
                splits.add(new HBaseSplit(this.connectorId, tableHandle.getSchemaTableName().getSchemaName(),
                        tableHandle.getSchemaTableName().getTableName(), tableMetaInfo.getRowKeyColName(),
                        getHostAddresses(hostIndex), null, null, splitConditions,
                        config.isRandomScheduleRedundantSplit(), null, null, null));
                hostIndex++;
                splitConditions = new ArrayList<>();
            } // end of if
        } // end of for
        if (splitConditions.size() > 0) {
            splits.add(new HBaseSplit(this.connectorId, tableHandle.getSchemaTableName().getSchemaName(),
                    tableHandle.getSchemaTableName().getTableName(), tableMetaInfo.getRowKeyColName(),
                    getHostAddresses(hostIndex), null, null, splitConditions,
                    config.isRandomScheduleRedundantSplit(), null, null, null));
        }
        log.info("Batch get by RowKey. Split count: "
                + splits.size() + ", table=" + tableHandle.getSchemaTableName().toString());
        for (HBaseSplit split : splits) {
            log.info("Print Split: " + split.toSimpleString());
        }
        return splits;
    }


    private List<String> addSalt2StartKeys(List<String> startKeys,
                                           int saltLower, int saltUpper, String rowKeySplitter) {
        List<String> saltyStartKeys = new ArrayList<>();
        Iterator<String> skIt = startKeys.iterator();
        String startKey;
        String salt;
        do {
            if (!startKeys.isEmpty()) {
                startKey = skIt.next();
            } else {
                startKey = "";
            }
            for (int i = saltLower; i <= saltUpper; i++) {
                salt = Utils.addZeroPrefix(String.valueOf(i),
                        String.valueOf(saltUpper).length() - String.valueOf(i).length());
                String saltyStartKey;
                if (StringUtils.isEmpty(startKey)) {
                    saltyStartKey = salt;
                } else {
                    saltyStartKey = salt + rowKeySplitter + startKey;
                }

                saltyStartKeys.add(saltyStartKey);
            }
        } while (skIt.hasNext());
        return saltyStartKeys;
    }

    /**
     * get scan startKey
     *
     * @param conditions   all query conditions
     * @param tmpStartKey  tmp start key
     * @param rowKeyFormat format of rowKey
     * @param formatIndex  the index of rowKey format
     * @return A list of all startKey
     */
    private List<String> getScanStartKey(List<ConditionInfo> conditions,
                                         String tmpStartKey, String[] rowKeyFormat, int formatIndex) {
        List<String> tmpStartKeys = new ArrayList<>();
        if (formatIndex == rowKeyFormat.length) {
            tmpStartKeys.add(tmpStartKey);
            return tmpStartKeys;
        }
        List<ConditionInfo> formatCondition = conditions.stream()
                // Filter out query conditions that conform to rowKey composition and whose operator is ==
                .filter(condition -> condition.getColName().equals(rowKeyFormat[formatIndex])
                        && condition.getOperator() == CONDITION_OPER.EQ)
                .collect(Collectors.toList());
        for (ConditionInfo condition : formatCondition) {
            tmpStartKeys.addAll(
                    getScanStartKey(conditions,
                            tmpStartKey + ((StringUtils.isEmpty(tmpStartKey) ? "" : ",") + condition.valueToString()),
                            rowKeyFormat,
                            formatIndex + 1)
            );
        }
        return tmpStartKeys;
    }

    private List<HostAddress> getHostAddresses(int index) {
        List<HostAddress> addresses = new ArrayList<>();
        // If split-remotely-accessible is true, presto-workers-ip may be null
        if (config.isRandomScheduleRedundantSplit()) {
            String[] workers = config.getPrestoWorkersIp().split(Constant.COMMA);
            String workerIp = workers[index % workers.length];
            addresses.add(HostAddress.fromParts(workerIp, config.getPrestoServerPort()));
        }
        return addresses;
    }

    /**
     * Find all available constraint from Sql.
     * Beware, Query conditions on both sides of 'OR' cannot be obtained.
     *
     * @param constraint constraint
     * @return all available constraint from Sql
     */
    private List<ConditionInfo> findConditionFromConstraint(TupleDomain<ColumnHandle> constraint) {
        List<ConditionInfo> handles = new ArrayList<>();
        if (!constraint.getDomains().isPresent()) {
            return handles;
        }

        Map<ColumnHandle, Domain> domainMap = constraint.getDomains().orElse(null);
        Set<ColumnHandle> keySet = domainMap.keySet();

        for (ColumnHandle ch : keySet) {
            HBaseColumnHandle hch = (HBaseColumnHandle) ch;

            Domain domain = domainMap.get(hch);
            if (domain == null) {
                continue;
            }

            if (domain.isSingleValue()) {
                Object value = domain.getNullableSingleValue();
                // =
                if (value instanceof Slice) {
                    handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.EQ,
                            value, domain.getType()));
                } else {
                    handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.EQ,
                            value, domain.getType()));
                }
            } else {
                for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                    if (range.isSingleValue()) {
                        handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.EQ,
                                range.getSingleValue(), domain.getType()));
                    } else {
                        if (!range.getLow().isLowerUnbounded()) {
                            switch (range.getLow().getBound()) {
                                // >
                                // != 部分1
                                case ABOVE:
                                    handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.GT,
                                            range.getLow().getValue(), domain.getType()));
                                    break;
                                // >=
                                case EXACTLY:
                                    handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.GE,
                                            range.getLow().getValue(), domain.getType()));
                                    break;
                                case BELOW:
                                    throw new IllegalArgumentException("Low Marker should never use BELOW bound: " + range);
                                default:
                                    throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                            }
                        }
                        if (!range.getHigh().isUpperUnbounded()) {
                            switch (range.getHigh().getBound()) {
                                case ABOVE:
                                    throw new IllegalArgumentException("High Marker should never use ABOVE bound: " + range);
                                    // <=
                                case EXACTLY:
                                    handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.LE,
                                            range.getHigh().getValue(), domain.getType()));
                                    break;
                                // <
                                // !=
                                case BELOW:
                                    handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.LT,
                                            range.getHigh().getValue(), domain.getType()));
                                    break;
                                default:
                                    throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                            }
                        }
                        // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                    }
                } // end of for domain.getValues().getRanges().getOrderedRanges())
            } // else of if (domain.isSingleValue)

        } // end of for domainMap.keySet

        return handles;
    }

}
