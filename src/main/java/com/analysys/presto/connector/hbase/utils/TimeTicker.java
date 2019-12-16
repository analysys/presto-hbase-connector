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

    public static void main(String[] args) {
        System.out.println('9' - '0');
        System.out.println('F' - 'A');
        System.out.println('f' - 'a');
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
