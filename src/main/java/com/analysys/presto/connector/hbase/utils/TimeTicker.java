package com.analysys.presto.connector.hbase.utils;

/**
 * Time ticker
 * Created by wupeng on 2018/1/19.
 */
public class TimeTicker {

    private long time;

    public TimeTicker() {
        this.time = System.currentTimeMillis();
    }

    public void updateTime() {
        this.time = System.currentTimeMillis();
    }

    public long checkDuration() {
        return System.currentTimeMillis() - time;
    }

    public String checkUpdateAndPrint() {
        long t = checkDurationAndUpdateTime();
        return t + " mill seconds";
    }

    public String checkUpdateAndPrint(String prefix) {
        return prefix + " " + checkUpdateAndPrint();
    }

    public long checkDurationAndUpdateTime() {
        long currentTime = System.currentTimeMillis();
        long duration = currentTime - time;
        this.time = currentTime;
        return duration;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public static long calculateTimeTo(long time) {
        return System.currentTimeMillis() - time;
    }
}
