package com.analysys.presto.connector.hbase.frame;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

/**
 * HBase connector plugin
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBasePlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(new HBaseConnectorFactory());
    }
}
