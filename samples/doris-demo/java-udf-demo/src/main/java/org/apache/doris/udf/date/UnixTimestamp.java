package org.apache.doris.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

/**
 * unix_timestamp
 */
public class UnixTimestamp extends UDF {

    /**
     * unix_timestamp()
     * select unix_timestamp();
     * @return
     */
    public double evaluate() {
        return System.currentTimeMillis() / 1000.0;
    }

    /**
     * unix_timestamp(varchar)
     * select unix_timestamp('2024-10-12');
     * select unix_timestamp('2024-10-12 12:11:11');
     * @param inputTimestamp
     * @return
     */
    public double evaluate(String inputTimestamp) {
        org.joda.time.format.DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime jodatime = formatter.parseDateTime(inputTimestamp);
        return jodatime.toDate().getTime() / 1000.0;
    }

    /**
     * unix_timestamp(varchar, varchar)
     * SELECT unix_timestamp('2009-03-20', 'yyyy-MM-dd')
     * @param inputTimestamp
     * @param pattern
     * @return
     */
    public double evaluate(String inputTimestamp, String pattern) {
        org.joda.time.format.DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern);
        DateTime jodatime = formatter.parseDateTime(inputTimestamp);
        return jodatime.toDate().getTime() / 1000.0;
    }
}
