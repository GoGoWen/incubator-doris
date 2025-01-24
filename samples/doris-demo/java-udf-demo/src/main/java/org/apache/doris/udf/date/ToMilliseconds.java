package org.apache.doris.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;


public class ToMilliseconds extends UDF {

    // 每单位时间的毫秒数
    private static final long MILLISECONDS_IN_A_SECOND = 1000;
    private static final long MILLISECONDS_IN_A_MINUTE = 60 * MILLISECONDS_IN_A_SECOND;
    private static final long MILLISECONDS_IN_AN_HOUR = 60 * MILLISECONDS_IN_A_MINUTE;
    private static final long MILLISECONDS_IN_A_DAY = 24 * MILLISECONDS_IN_AN_HOUR;
    private static final long MILLISECONDS_IN_A_MONTH = 30 * MILLISECONDS_IN_A_DAY; // 近似值
    private static final long MILLISECONDS_IN_A_YEAR = 365 * MILLISECONDS_IN_A_DAY; // 近似值

    public Long evaluate(String intervalStr) {
        if (intervalStr == null) {
            return null;
        }

        Interval interval = Interval.parseInterval(intervalStr);

        long totalMilliseconds = 0;

        totalMilliseconds += interval.getYears() * MILLISECONDS_IN_A_YEAR;
        totalMilliseconds += interval.getMonths() * MILLISECONDS_IN_A_MONTH;
        totalMilliseconds += interval.getDays() * MILLISECONDS_IN_A_DAY;
        totalMilliseconds += interval.getHours() * MILLISECONDS_IN_AN_HOUR;
        totalMilliseconds += interval.getMinutes() * MILLISECONDS_IN_A_MINUTE;
        totalMilliseconds += interval.getSeconds() * MILLISECONDS_IN_A_SECOND;

        return totalMilliseconds;
    }

    public static void main(String[] args) {
        ToMilliseconds udf = new ToMilliseconds();
        System.out.println(udf.evaluate("1-2-3 4:5:6")); // 示例输出
    }
}
