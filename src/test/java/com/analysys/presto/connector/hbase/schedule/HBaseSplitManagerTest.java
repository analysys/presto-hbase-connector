package com.analysys.presto.connector.hbase.schedule;

import org.junit.Test;


public class HBaseSplitManagerTest {

    @Test
    public void testGetStartEndKeysWhenRowKeyHaveSaltyPrefix() {
        System.out.println('9' - '0');
        System.out.println('F' - 'A');
        System.out.println('f' - 'a');
        System.out.println('z' - 'A');
        System.out.println((char)('z' + 1));
        /*List<HBaseSplitManager.StartAndEnd> startAndEnds = new HBaseSplitManager(new HBaseConnectorId("hbase"),
                null, null)
                .getStartEndKeysWhenRowKeyHasSaltyFirstChar("0~9,A~Z,a~z");
        System.out.println("startAndEnds.size=" + startAndEnds.size());
        System.out.println(Arrays.toString(startAndEnds.toArray()));*/
    }
}








