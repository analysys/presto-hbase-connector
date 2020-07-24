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
package com.analysys.presto.connector.hbase.utils;

import com.analysys.presto.connector.hbase.meta.HBaseColumnMetadata;
import com.analysys.presto.connector.hbase.meta.TableMetaInfo;
import com.analysys.presto.connector.hbase.schedule.ConditionInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.analysys.presto.connector.hbase.utils.Constant.*;

/**
 * utils
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class Utils {

    public static final Logger logger = Logger.get(Utils.class);

    /**
     * Read table json from metaDir by schema name and table name
     *
     * @param schemaName schema name
     * @param tableName  table name
     * @param metaDir    meta dir
     * @return json file content
     */
    private static String readTableJson(String schemaName, String tableName, String metaDir) {
        try {
            String tableMetaPath = metaDir + File.separator
                    + (schemaName == null || "".equals(schemaName) ? DEFAULT_HBASE_NAMESPACE_NAME : schemaName)
                    + File.separator + tableName + TABLE_META_FILE_TAIL;
            return FileUtils.readFileToString(new File(tableMetaPath), JSON_ENCODING_UTF8);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return "";
    }

    /**
     * Read table json from metaDir by schema name and table name.
     * And convert it to an Object of TableMetaInfo.
     *
     * @param schemaName schema name
     * @param tableName  table name
     * @param metaDir    meta info dir
     * @return Object of TableMetaInfo
     */
    public static TableMetaInfo getTableMetaInfoFromJson(String schemaName, String tableName, String metaDir) {
        long startTime = System.currentTimeMillis();
        try {
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = readTableJson(schemaName, tableName, metaDir);
            return mapper.readValue(jsonString, TableMetaInfo.class);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            logger.info(String.format("Read meta info of TABLE %s.%s from json, totally used %d ms.",
                    schemaName, tableName, (System.currentTimeMillis() - startTime)));
        }
        return null;
    }

    /**
     * Fetch column meta info from json file
     *
     * @param schemaName schema name
     * @param tableName  table name
     * @param metaDir    meta dir
     * @return list of column meta info
     */
    public static ImmutableList<ColumnMetadata> getColumnMetaFromJson(String schemaName, String tableName, String metaDir) {
        long startTime = System.currentTimeMillis();
        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        try {
            String jsonStr = readTableJson(schemaName, tableName, metaDir);
            JSONObject obj = new JSONObject(jsonStr);
            JSONArray cols = obj.getJSONArray(JSON_TABLEMETA_COLUMNES);
            boolean hasRowKey = false;
            for (int i = 0; i < cols.length(); i++) {
                JSONObject temp = new JSONObject(cols.getString(i));
                String family = temp.getString(JSON_TABLEMETA_FAMILY);
                String columnName = temp.getString(JSON_TABLEMETA_COLUMNNAME);
                String type = temp.getString(JSON_TABLEMETA_TYPE);
                boolean isRowKey = temp.getBoolean(JSON_TABLEMETA_ISROWKEY);
                columnsMetadata.add(new HBaseColumnMetadata(family, columnName, matchType(type), isRowKey));
                if (isRowKey) {
                    hasRowKey = true;
                }
            }
            Preconditions.checkState(hasRowKey,
                    "Table %s.%s doesn't specified ROW_KEY column." +
                            " Specify ROW_KEY in your .json file.", schemaName, tableName);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            logger.info(String.format("Read COLUMN meta info of TABLE %s.%s from json, totally used %d ms.",
                    schemaName, tableName, (System.currentTimeMillis() - startTime)));
        }
        return columnsMetadata.build();
    }

    /**
     * Add zero prefix to a salt string
     *
     * @param key       key
     * @param missCount missCount
     * @return standard salt value
     */
    public static String addZeroPrefix(String key, int missCount) {
        for (int j = missCount; j > 0; j--) {
            key = "0" + key;
        }
        return key;
    }

    /**
     * Find the presto type of column you configured in json file by type flag.
     *
     * @param type The type value that configured in json file.
     * @return type in presto
     */
    public static Type matchType(String type) {
        if (type == null) {
            return VarcharType.VARCHAR;
        }

        switch (type.toLowerCase()) {
            /*case "string":
                return VarcharType.VARCHAR;*/
            case "int":
                return IntegerType.INTEGER;
            case "bigint":
                return BigintType.BIGINT;
            case "double":
                return DoubleType.DOUBLE;
            case "boolean":
                return BooleanType.BOOLEAN;
            case "array<string>":
                return new ArrayType(VarcharType.VARCHAR);
            case "timestamp":
                // return TimestampType.TIMESTAMP;
            case "datetime":
                return TimestampType.TIMESTAMP;
            case "number":
                return DecimalType.createDecimalType(DECIMAL_DEFAULT_PRECISION, DECIMAL_DEFAULT_SCALE);
            default:
                return VarcharType.VARCHAR;
        }
    }

    /**
     * Whether sql constraint contains conditions like "rowKey='xxx'" or "rowKey in ('xxx','xxx')"
     *
     * @return true if this sql is batch get.
     */
    public static boolean isBatchGet(List<ConditionInfo> conditions, String rowKeyColName) {
        if (conditions != null) {
            for (ConditionInfo cond : conditions) {
                if (rowKeyColName.equals(cond.getColName())
                        && cond.getOperator() == CONDITION_OPER.EQ) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Copy contents in ${srcAry} from position ${srcPos} for ${length} bytes.
     *
     * @param srcAry source array
     * @param srcPos start position
     * @param length length
     * @return copied byte array
     */
    public static byte[] arrayCopy(byte[] srcAry, int srcPos, int length) {
        byte[] destAry = new byte[length];
        System.arraycopy(srcAry, srcPos, destAry, 0, length);
        return destAry;
    }

    public static Configuration getHadoopConf(String zookeeperQuorum, String zookeeperClientPort) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        conf.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
        conf.set("hbase.cluster.distributed", "true");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("hbase.client.retries.number", "3");
        return conf;
    }

    public static List<RegionInfo> getRegionInfosFromManifest(SnapshotManifest manifest) {
        List<SnapshotProtos.SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
        if (regionManifests == null) {
            throw new IllegalArgumentException("Snapshot seems empty");
        }

        List<RegionInfo> regionInfos = Lists.newArrayListWithCapacity(regionManifests.size());

        for (SnapshotProtos.SnapshotRegionManifest regionManifest : regionManifests) {
            RegionInfo hri = ProtobufUtil.toRegionInfo(regionManifest.getRegionInfo());
            if (hri.isOffline() && (hri.isSplit() || hri.isSplitParent())) {
                continue;
            }
            regionInfos.add(hri);
        }
        return regionInfos;
    }

    /**
     * get region infos
     *
     * @param zookeeperQuorum     zookeeper quorum
     * @param zookeeperClientPort zookeeper client port
     * @param hBaseRootDir        HBase root dir
     * @param snapshotName        snapshot name
     * @return region info list
     * @throws IOException IOException
     */
    public static List<RegionInfo> getRegionInfos(String zookeeperQuorum, String zookeeperClientPort,
                                                   String hBaseRootDir, String snapshotName) throws IOException {
        try {
            Configuration conf = Utils.getHadoopConf(zookeeperQuorum, zookeeperClientPort);
            Path root = new Path(hBaseRootDir);
            FileSystem fs = FileSystem.get(conf);
            Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, root);
            SnapshotProtos.SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
            SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
            return Utils.getRegionInfosFromManifest(manifest);
        } catch (IOException ex) {
            logger.error("get region info error: " + ex.getMessage(), ex);
            throw ex;
        }
    }

    /**
     * 去掉array<string>中\001分隔的元素前的一个空格
     * presto查询出来的值是"aaa\001 bbb\001 ccc"，元素之间会带上一个空格
     *
     * @param value value
     * @return string
     */
    public static String removeExtraSpaceInArrayString(String value) {
        StringBuilder buff = new StringBuilder();
        String[] ss = value.split(ARRAY_STRING_SPLITTER);
        for (int j = 0; j < ss.length; j++) {
            String ele = ss[j];
            if (j > 0) {
                ele = ele.substring(1);
            } else {
                buff.append(ARRAY_STRING_SPLITTER);
            }
            buff.append(ele);
        }
        return buff.toString();
    }

    public static boolean isEmpty(String str) {
        return str == null || "".equals(str);
    }

}



