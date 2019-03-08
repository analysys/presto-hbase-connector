package com.analysys.presto.connector.hbase.frame;

import com.analysys.presto.connector.hbase.meta.HBaseTableHandle;
import com.analysys.presto.connector.hbase.meta.HBaseTableLayoutHandle;
import com.analysys.presto.connector.hbase.meta.HBaseColumnHandle;
import com.analysys.presto.connector.hbase.schedule.HBaseSplit;
import com.facebook.presto.spi.ConnectorHandleResolver;

/**
 * HBase handle resolver
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseHandleResolver implements ConnectorHandleResolver {

    @Override
    public Class getTableLayoutHandleClass() {
        return HBaseTableLayoutHandle.class;
    }

    @Override
    public Class getTableHandleClass() {
        return HBaseTableHandle.class;
    }

    @Override
    public Class getColumnHandleClass() {
        return HBaseColumnHandle.class;
    }

    @Override
    public Class getSplitClass() {
        return HBaseSplit.class;
    }

    @Override
    public Class getTransactionHandleClass() {
        return HBaseTransactionHandle.class;
    }

}
