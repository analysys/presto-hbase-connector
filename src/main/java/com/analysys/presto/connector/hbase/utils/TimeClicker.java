package com.analysys.presto.connector.hbase.utils;

/**
 * 计时器
 * Created by lenovo on 2018/7/11.
 */
public class TimeClicker {

    private long time;

    public TimeClicker() {
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
